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
         *
         * When the current WalReader reaches clean EOF, it is closed and the
         * next segment file is opened automatically.
         *
         * Errors from any segment are recorded and stop the chain for that shard.
         */
        class ShardSegmentChain {
            public:
                explicit ShardSegmentChain(std::vector<std::filesystem::path> segments)
                    : segments_{std::move(segments)}, next_seg_idx_{0} { open_next_segment(); }

                /**
                 * Returns the next batch across all segments in order.
                 * Returns nullopt on chain EOF or error.
                 */
                [[nodiscard]] std::optional<WalReader::BatchView> next_batch() {
                    while (current_reader_) {
                        if (auto batch = current_reader_->next_batch()) { return batch; }
                        // Current segment ended - check for error
                        if (current_reader_->has_error()) {
                            error_type_ = current_reader_->error_type();
                            error_position_ = current_reader_->error_position();
                            current_reader_.reset();
                            return std::nullopt;
                        }
                        // Clean EOF on this segment; advance to next
                        current_reader_.reset();
                        open_next_segment();
                    }
                    return std::nullopt;
                }

                [[nodiscard]] bool has_error() const noexcept { return error_type_ != WalReader::ErrorType::NONE; }
                [[nodiscard]] WalReader::ErrorType error_type() const noexcept { return error_type_; }
                [[nodiscard]] uint64_t error_position() const noexcept { return error_position_; }

                // shard_id from the first successfully opened reader
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
                        return; // successfully opened
                    }
                    // No more segments - current_reader_ stays null (clean end)
                }

                std::vector<std::filesystem::path> segments_;
                size_t next_seg_idx_;
                std::unique_ptr<WalReader> current_reader_;

                uint32_t shard_id_ = UINT32_MAX;
                WalReader::ErrorType error_type_ = WalReader::ErrorType::NONE;
                uint64_t error_position_ = 0;
        };

        // ── HeapEntry ────────────────────────────────────────────────────────

        /**
         * HeapEntry - one live batch from a shard chain, held in the merge-heap.
         * Ordered by batch_seq ascending so the min-heap always yields
         * the globally earliest unprocessed batch.
         */
        struct HeapEntry {
            uint64_t batch_seq;
            uint32_t chain_idx; ///< Index into chains[] (tie-breaking)
            WalReader::BatchView batch;

            // min-heap: smallest batch_seq at top
            bool operator>(const HeapEntry& o) const noexcept {
                if (batch_seq != o.batch_seq) return batch_seq > o.batch_seq;
                return chain_idx > o.chain_idx; // deterministic tie-break
            }
        };

        using MinHeap = std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>>;

        // ── dispatch_batch ───────────────────────────────────────────────────

        struct DispatchCounts {
            uint64_t records;
            uint64_t commits;
            uint64_t checkpoints;
        };

        [[nodiscard]] DispatchCounts dispatch_batch(
            const WalReader::BatchView& batch,
            const WalRecovery::RecordHandler& on_record,
            const WalRecovery::CommitHandler& on_commit,
            const WalRecovery::CheckpointHandler& on_checkpoint
        ) {
            DispatchCounts counts{0, 0, 0};
            WalIterator it = batch.iterator();

            while (it.next()) {
                switch (it.entry_type()) {
                    case WalEntryType::Record: {
                        on_record(it.as_record());
                        ++counts.records;
                        break;
                    }
                    case WalEntryType::Commit: {
                        const auto [seq, ts] = it.as_commit();
                        on_commit(seq, ts);
                        ++counts.commits;
                        break;
                    }
                    case WalEntryType::Checkpoint: {
                        if (on_checkpoint) {
                            // Checkpoint entry reuses Commit layout: [seq:u64][ts:u64]
                            const auto [seq, _ts] = it.as_commit();
                            on_checkpoint(seq);
                        }
                        ++counts.checkpoints;
                        break;
                    }
                    case WalEntryType::Metadata:
                        // Reserved for future use; skip silently.
                        break;
                }
            }
            return counts;
        }
    } // anonymous namespace

    // ============================================================================
    // WalRecovery::replay
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

        // ── 1. Enumerate and group segment files ─────────────────────────────
        const auto segment_files = enumerate_segment_files(wal_dir);
        if (segment_files.empty()) return result;

        const auto shard_groups = group_by_shard(segment_files);

        // ── 2. Build ShardSegmentChains and seed the heap ────────────────────
        const uint32_t chain_count = static_cast<uint32_t>(shard_groups.size());

        std::vector<std::unique_ptr<ShardSegmentChain>> chains;
        chains.reserve(chain_count);

        MinHeap heap;

        for (uint32_t i = 0; i < chain_count; ++i) {
            if (shard_groups[i].empty()) {
                chains.push_back(nullptr);
                continue;
            }

            auto chain = std::make_unique<ShardSegmentChain>(shard_groups[i]);

            if (chain->has_error()) {
                result.success = false;
                result.shard_errors.push_back(
                    ShardError{
                        .shard_id = chain->shard_id() == UINT32_MAX
                                        ? i
                                        : chain->shard_id(),
                        .error_type = chain->error_type(),
                        .error_position = chain->error_position(),
                    }
                );
                chains.push_back(std::move(chain));
                continue;
            }

            // Prime the heap with the first batch from this chain
            if (auto batch_opt = chain->next_batch()) { heap.push(HeapEntry{.batch_seq = batch_opt->batch_seq, .chain_idx = i, .batch = *batch_opt,}); }
            else if (chain->has_error()) {
                result.success = false;
                result.shard_errors.push_back(
                    ShardError{.shard_id = chain->shard_id(), .error_type = chain->error_type(), .error_position = chain->error_position(),}
                );
            }
            // Empty chain (all segments empty): fine, just don't push.

            chains.push_back(std::move(chain));
        }

        // ── 3. Merge-drain the heap in batch_seq order ────────────────────────
        while (!heap.empty()) {
            HeapEntry top = std::move(const_cast<HeapEntry&>(heap.top()));
            heap.pop();

            const DispatchCounts counts = dispatch_batch(top.batch, on_record, on_commit, on_checkpoint);
            result.batches_replayed += 1;
            result.records_replayed += counts.records;
            result.commits_replayed += counts.commits;
            result.checkpoints_replayed += counts.checkpoints;

            // Advance this chain and re-push if more batches exist
            ShardSegmentChain* chain = chains[top.chain_idx].get();
            if (chain == nullptr) continue;

            if (auto next_opt = chain->next_batch()) {
                heap.push(HeapEntry{.batch_seq = next_opt->batch_seq, .chain_idx = top.chain_idx, .batch = *next_opt,});
            }
            else if (chain->has_error()) {
                result.success = false;
                result.shard_errors.push_back(
                    ShardError{.shard_id = chain->shard_id(), .error_type = chain->error_type(), .error_position = chain->error_position(),}
                );
            }
            // Clean EOF from this chain: just don't re-push.
        }

        return result;
    }

    // ============================================================================
    // WalRecovery public API
    // ============================================================================

    std::unique_ptr<WalRecovery> WalRecovery::create() { return std::unique_ptr<WalRecovery>(new WalRecovery()); }

    WalRecovery::~WalRecovery() = default;
} // namespace akkaradb::wal