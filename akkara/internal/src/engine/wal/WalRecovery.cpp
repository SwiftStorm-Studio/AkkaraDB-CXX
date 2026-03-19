/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/src/engine/wal/WalRecovery.cpp
#include "engine/wal/WalRecovery.hpp"

#include <algorithm>
#include <charconv>
#include <format>
#include <future>
#include <queue>
#include <stdexcept>
#include <vector>

namespace akkaradb::wal {
    // ============================================================================
    // Internal helpers
    // ============================================================================

    namespace {
        // ── Segment file enumeration ──────────────────────────────────────────

        struct SegmentFile {
            uint32_t shard_id;
            uint64_t segment_id;
            std::filesystem::path path;
        };

        /**
         * Parses "shard_{shard_id:04d}_seg{segment_id:04d}" from a stem string.
         * Returns false if the stem does not match the expected pattern.
         */
        [[nodiscard]] bool parse_segment_stem(const std::string& stem, uint32_t& out_shard, uint64_t& out_seg) {
            // Expected: "shard_NNNN_segNNNN"
            if (stem.size() < 15) return false; // "shard_0000_seg0000" = 18 chars minimum
            if (stem.substr(0, 6) != "shard_") return false;

            const auto seg_pos = stem.find("_seg", 6);
            if (seg_pos == std::string::npos) return false;

            const std::string shard_str = stem.substr(6, seg_pos - 6);
            const std::string seg_str = stem.substr(seg_pos + 4);

            uint32_t shard_id = 0;
            uint64_t segment_id = 0;

            auto [p1, ec1] = std::from_chars(shard_str.data(), shard_str.data() + shard_str.size(), shard_id);
            if (ec1 != std::errc{} || p1 != shard_str.data() + shard_str.size()) return false;

            auto [p2, ec2] = std::from_chars(seg_str.data(), seg_str.data() + seg_str.size(), segment_id);
            if (ec2 != std::errc{} || p2 != seg_str.data() + seg_str.size()) return false;

            out_shard = shard_id;
            out_seg = segment_id;
            return true;
        }

        /**
         * Scans wal_dir for shard_NNNN_segNNNN.akwal files.
         * Returns all found files sorted by (shard_id asc, segment_id asc).
         */
        [[nodiscard]] std::vector<SegmentFile>
        enumerate_segment_files(const std::filesystem::path& wal_dir) {
            std::vector<SegmentFile> files;
            if (!std::filesystem::exists(wal_dir)) return files;

            for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
                if (!entry.is_regular_file()) continue;
                const auto& p = entry.path();
                if (p.extension() != ".akwal") continue;

                uint32_t shard_id = 0;
                uint64_t segment_id = 0;
                if (!parse_segment_stem(p.stem().string(), shard_id, segment_id)) continue;

                files.push_back(SegmentFile{shard_id, segment_id, p});
            }

            std::ranges::sort(
                files,
                [](const SegmentFile& a, const SegmentFile& b) {
                    if (a.shard_id != b.shard_id) return a.shard_id < b.shard_id;
                    return a.segment_id < b.segment_id;
                }
            );
            return files;
        }

        /**
         * Groups a sorted SegmentFile list by shard_id.
         * Returns a vector-of-vectors, one inner vector per unique shard_id,
         * each already in segment_id ascending order.
         */
        [[nodiscard]] std::vector<std::vector<std::filesystem::path>>
        group_by_shard(const std::vector<SegmentFile>& files) {
            std::vector<std::vector<std::filesystem::path>> groups;
            for (const auto& sf : files) {
                if (groups.empty() || groups.back().empty() ||
                    // compare shard_id: derive from the last SegmentFile via index
                    sf.shard_id != static_cast<uint32_t>(groups.size() - 1)) {
                    // Start a new group for this shard_id.
                    // Note: shard_ids may not be contiguous; pad with empty groups.
                    while (groups.size() <= sf.shard_id) groups.emplace_back();
                }
                groups[sf.shard_id].push_back(sf.path);
            }
            return groups;
        }

        // ── ShardSegmentChain ────────────────────────────────────────────────

        /**
         * ShardSegmentChain - Presents all segment files of a single shard as a
         * single logical stream of batches, in segment_id ascending order.
         */
        class ShardSegmentChain {
            public:
                explicit ShardSegmentChain(std::vector<std::filesystem::path> segments)
                    : segments_{std::move(segments)}, next_seg_idx_{0} { open_next_segment(); }

                [[nodiscard]] std::optional<WalReader::BatchView> next_batch() {
                    while (current_reader_) {
                        if (auto batch = current_reader_->next_batch()) { return batch; }
                        if (current_reader_->has_error()) {
                            error_type_ = current_reader_->error_type();
                            error_position_ = current_reader_->error_position();
                            current_reader_.reset();
                            return std::nullopt;
                        }
                        current_reader_.reset();
                        open_next_segment();
                    }
                    return std::nullopt;
                }

                [[nodiscard]] bool has_error() const noexcept { return error_type_ != WalReader::ErrorType::NONE; }
                [[nodiscard]] WalReader::ErrorType error_type() const noexcept { return error_type_; }
                [[nodiscard]] uint64_t error_position() const noexcept { return error_position_; }
                [[nodiscard]] uint32_t shard_id() const noexcept { return shard_id_; }

            private:
                void open_next_segment() {
                    while (next_seg_idx_ < segments_.size()) {
                        const auto& path = segments_[next_seg_idx_++];
                        try { current_reader_ = WalReader::open(path); }
                        catch (const std::exception&) {
                            error_type_ = WalReader::ErrorType::INVALID_MAGIC;
                            error_position_ = 0;
                            return;
                        }

                        if (shard_id_ == UINT32_MAX) { shard_id_ = current_reader_->shard_id(); }

                        if (current_reader_->has_error()) {
                            error_type_ = current_reader_->error_type();
                            error_position_ = current_reader_->error_position();
                            current_reader_.reset();
                            return;
                        }
                        return;
                    }
                }

                std::vector<std::filesystem::path> segments_;
                size_t next_seg_idx_;
                std::unique_ptr<WalReader> current_reader_;

                uint32_t shard_id_ = UINT32_MAX;
                WalReader::ErrorType error_type_ = WalReader::ErrorType::NONE;
                uint64_t error_position_ = 0;
        };

        // ── Materialized WAL entry ────────────────────────────────────────────

        /**
         * MatRecord - Fully materialized WAL entry.
         *
         * Owns key/value bytes so the WalReader that produced them can be destroyed
         * after the shard thread returns.  Batch ordering is preserved via
         * (batch_seq, entry_order).
         */
        struct MatRecord {
            uint64_t batch_seq = 0;
            uint32_t entry_order = 0; ///< position within batch (0-based)
            WalEntryType type = WalEntryType::Record;

            // Record fields
            uint64_t seq = 0;
            uint8_t flags = 0;
            uint64_t fp64 = 0;
            uint64_t mini_key = 0;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;

            // Commit / Checkpoint fields (seq reused for Checkpoint)
            uint64_t commit_ts = 0;
        };

        struct ShardMatResult {
            std::vector<MatRecord> records;
            uint64_t batches_seen = 0;
            bool has_error = false;
            uint32_t shard_id = UINT32_MAX;
            WalReader::ErrorType error_type = WalReader::ErrorType::NONE;
            uint64_t error_position = 0;
        };

        /**
         * Reads all batches from one shard's segment chain and materialises every
         * entry into MatRecord structs (copies key/value bytes).
         *
         * Called from a background thread.  Each shard processes its segment files
         * entirely independently, so there is no shared state during I/O.
         */
        ShardMatResult materialize_shard(std::vector<std::filesystem::path> segments) {
            ShardSegmentChain chain(std::move(segments));
            ShardMatResult result{};
            result.shard_id = chain.shard_id();

            while (auto batch_opt = chain.next_batch()) {
                const auto& batch = *batch_opt;
                WalIterator it = batch.iterator();
                uint32_t entry_order = 0;
                ++result.batches_seen;

                while (it.next()) {
                    if (it.entry_type() == WalEntryType::Metadata) continue;

                    MatRecord mr;
                    mr.batch_seq = batch.batch_seq;
                    mr.entry_order = entry_order++;
                    mr.type = it.entry_type();

                    switch (it.entry_type()) {
                        case WalEntryType::Record: {
                            const WalRecordOpRef& ref = it.as_record();
                            mr.seq = ref.seq();
                            mr.flags = ref.header().flags;
                            mr.fp64 = ref.key_fp64();
                            mr.mini_key = ref.mini_key();
                            {
                                const auto k = ref.key();
                                mr.key.assign(k.begin(), k.end());
                            }
                            if (!ref.is_tombstone()) {
                                const auto v = ref.value();
                                mr.value.assign(v.begin(), v.end());
                            }
                            break;
                        }
                        case WalEntryType::Commit: {
                            const auto [s, ts] = it.as_commit();
                            mr.seq = s;
                            mr.commit_ts = ts;
                            break;
                        }
                        case WalEntryType::Checkpoint: {
                            const auto [s, _ts] = it.as_commit();
                            mr.seq = s;
                            break;
                        }
                        default: continue;
                    }

                    result.records.push_back(std::move(mr));
                }
            }

            if (chain.has_error()) {
                result.has_error = true;
                result.error_type = chain.error_type();
                result.error_position = chain.error_position();
            }

            return result;
        }
    } // anonymous namespace

    // ============================================================================
    // WalRecovery::replay  (parallel shard I/O)
    // ============================================================================

    WalRecovery::Result WalRecovery::replay(
        const std::filesystem::path& wal_dir,
        const RecordHandler& on_record,
        const CommitHandler& on_commit,
        const CheckpointHandler& on_checkpoint
    ) {
        if (!on_record) { throw std::invalid_argument("WalRecovery::replay: on_record is required"); }
        if (!on_commit) { throw std::invalid_argument("WalRecovery::replay: on_commit is required"); }

        Result result{};
        result.success = true;

        // ── 1. Enumerate and group segment files ──────────────────────────────
        const auto segment_files = enumerate_segment_files(wal_dir);
        if (segment_files.empty()) return result;

        const auto shard_groups = group_by_shard(segment_files);
        const size_t num_shards = shard_groups.size();

        // ── 2. Spawn one async task per non-empty shard ───────────────────────
        //
        // Each shard reads its segment chain and materialises all entries into
        // MatRecord structs (copying key/value bytes so the WalReader can be
        // freed inside the thread).  Shards are fully independent → true I/O
        // parallelism with no shared state during the read phase.
        std::vector<std::future<ShardMatResult>> futures;
        futures.reserve(num_shards);

        for (size_t i = 0; i < num_shards; ++i) {
            if (shard_groups[i].empty()) {
                // Dummy future for empty shard slot
                futures.push_back(std::async(std::launch::deferred, []() { return ShardMatResult{}; }));
            }
            else { futures.push_back(std::async(std::launch::async, materialize_shard, shard_groups[i])); }
        }

        // ── 3. Collect results ────────────────────────────────────────────────
        std::vector<MatRecord> all_records;
        for (size_t i = 0; i < num_shards; ++i) {
            ShardMatResult mat = futures[i].get();

            result.batches_replayed += mat.batches_seen;

            if (mat.has_error) {
                result.success = false;
                result.shard_errors.push_back(
                    ShardError{
                        .shard_id = mat.shard_id == UINT32_MAX ? static_cast<uint32_t>(i) : mat.shard_id,
                        .error_type = mat.error_type,
                        .error_position = mat.error_position,
                    }
                );
            }

            for (auto& mr : mat.records) { all_records.push_back(std::move(mr)); }
        }

        // ── 4. Restore global write order ─────────────────────────────────────
        //
        // Sort by (batch_seq, entry_order) to reproduce the exact same dispatch
        // order as the original sequential min-heap merge.
        std::sort(
            all_records.begin(),
            all_records.end(),
            [](const MatRecord& a, const MatRecord& b) noexcept {
                if (a.batch_seq != b.batch_seq) return a.batch_seq < b.batch_seq;
                return a.entry_order < b.entry_order;
            }
        );

        // ── 5. Dispatch in order ──────────────────────────────────────────────
        for (const auto& mr : all_records) {
            switch (mr.type) {
                case WalEntryType::Record: {
                    // Reconstruct an owned buffer that matches the serialised layout
                    // expected by WalRecordOpRef: [WalEntryHeader:8B][AKHdr32:32B][key][val]
                    const size_t buf_sz = WalEntryHeader::SIZE + sizeof(core::AKHdr32) + mr.key.size() + mr.value.size();
                    std::vector<std::byte> entry_buf(buf_sz);
                    core::BufferView bv(entry_buf.data(), buf_sz);

                    if (mr.flags & core::AKHdr32::FLAG_TOMBSTONE) {
                        (void)serialize_delete_direct(bv, std::span<const uint8_t>(mr.key.data(), mr.key.size()), mr.seq, mr.fp64, mr.mini_key);
                    }
                    else {
                        (void)serialize_add_direct(
                            bv,
                            std::span<const uint8_t>(mr.key.data(), mr.key.size()),
                            std::span<const uint8_t>(mr.value.data(), mr.value.size()),
                            mr.seq,
                            mr.fp64,
                            mr.mini_key,
                            mr.flags
                        );
                    }

                    // WalRecordOpRef is a zero-copy view into entry_buf (alive for
                    // the duration of on_record).
                    WalRecordOpRef ref(core::BufferView(entry_buf.data(), buf_sz));
                    on_record(ref);
                    ++result.records_replayed;
                    break;
                }

                case WalEntryType::Commit: {
                    on_commit(mr.seq, mr.commit_ts);
                    ++result.commits_replayed;
                    break;
                }

                case WalEntryType::Checkpoint: {
                    if (on_checkpoint) on_checkpoint(mr.seq);
                    ++result.checkpoints_replayed;
                    break;
                }

                default: break;
            }
        }

        return result;
    }

    // ============================================================================
    // WalRecovery public API
    // ============================================================================

    std::unique_ptr<WalRecovery> WalRecovery::create() { return std::unique_ptr<WalRecovery>(new WalRecovery()); }

    WalRecovery::~WalRecovery() = default;
} // namespace akkaradb::wal
