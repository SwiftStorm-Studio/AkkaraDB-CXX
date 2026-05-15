/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// internal/src/engine/AkkEngine.cpp
#include "engine/AkkEngine.hpp"

#include "core/record/KeyFingerprint.hpp"
#include "core/record/MemHdr16.hpp"
#include "engine/blob/BlobFraming.hpp"
#include "engine/cluster/ClusterRuntime.hpp"
#include "engine/manifest/Manifest.hpp"
#include "engine/wal/WalRecovery.hpp"

#include <algorithm>
#include <atomic>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>

namespace akkaradb::engine {
    namespace fs = std::filesystem;

    namespace {
        [[nodiscard]] uint64_t now_ns() noexcept {
            timespec ts{};
            timespec_get(&ts, TIME_UTC);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
        }

        [[nodiscard]] uint32_t next_pow2(uint32_t value) noexcept {
            uint32_t out = 1;
            while (out < value) { out <<= 1; }
            return out;
        }

        [[nodiscard]] uint32_t shards_for_threads(uint32_t writers, uint32_t cap) noexcept {
            if (writers <= 1) { return 1; }
            const uint32_t raw = (writers * (writers - 1u) * 9u + 3u) / 4u;
            return std::min(next_pow2(std::max(raw, 2u)), cap);
        }

        void ensure_dir(const fs::path& path) {
            if (!path.empty()) { fs::create_directories(path); }
        }

        [[nodiscard]] uint64_t load_or_create_node_id(const fs::path& path) {
            if (path.empty()) { return 0; }
            ensure_dir(path.parent_path());

            {
                std::ifstream in(path, std::ios::binary);
                uint64_t id = 0;
                if (in.read(reinterpret_cast<char*>(&id), sizeof(id)) && id != 0) { return id; }
            }

            std::random_device rd;
            std::mt19937_64 rng(rd());
            uint64_t id = rng();
            if (id == 0) { id = 1; }

            std::ofstream out(path, std::ios::binary | std::ios::trunc);
            if (!out.write(reinterpret_cast<const char*>(&id), sizeof(id))) {
                throw std::runtime_error("AkkEngine: failed to persist node id: " + path.string());
            }
            return id;
        }

        [[nodiscard]] int compare_key(std::span<const uint8_t> a, std::span<const uint8_t> b) {
            const int cmp = std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()) ? -1
                : std::lexicographical_compare(b.begin(), b.end(), a.begin(), a.end()) ? 1
                : 0;
            return cmp;
        }

        void copy_span(std::span<const uint8_t> in, std::vector<uint8_t>& out) {
            out.assign(in.begin(), in.end());
        }
    } // namespace

    class AkkEngine::ScanIterator::Impl {
        public:
            Impl(
                memtable::MemTable::RangeIterator memtable_iter,
                sst::SSTManager::Iterator sst_iter,
                blob::BlobManager* blob_manager
            ) : memtable_iter_{std::move(memtable_iter)}, sst_iter_{std::move(sst_iter)}, blob_manager_{blob_manager} {
                advance_memtable();
                advance_sst();
                advance_pending();
            }

            [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

            [[nodiscard]] std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> next() {
                auto out = std::move(pending_);
                advance_pending();
                return out;
            }

        private:
            memtable::MemTable::RangeIterator memtable_iter_;
            sst::SSTManager::Iterator sst_iter_;
            std::optional<core::RecordView> memtable_cur_;
            std::optional<sst::SSTRecord> sst_cur_;
            blob::BlobManager* blob_manager_ = nullptr;
            std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> pending_;

            void advance_memtable() { memtable_cur_ = memtable_iter_.has_next() ? memtable_iter_.next() : std::optional<core::RecordView>{}; }
            void advance_sst() { sst_cur_ = sst_iter_.has_next() ? sst_iter_.next() : std::optional<sst::SSTRecord>{}; }

            [[nodiscard]] std::optional<std::vector<uint8_t>> resolve(uint8_t flags, std::span<const uint8_t> value) const {
                if ((flags & core::MemHdr16::FLAG_BLOB) == 0) { return std::vector<uint8_t>{value.begin(), value.end()}; }
                if (!blob_manager_ || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
                const blob::BlobRef ref = blob::decode_blob_ref(value.data());
                auto out = blob_manager_->read(ref.blob_id, ref.content_crc32c);
                if (out.empty() && ref.total_size != 0) { return std::nullopt; }
                return out;
            }

            void advance_pending() {
                pending_.reset();
                while (memtable_cur_ || sst_cur_) {
                    const bool has_mt = memtable_cur_.has_value();
                    const bool has_sst = sst_cur_.has_value();
                    int cmp = 0;
                    if (has_mt && has_sst) { cmp = compare_key(memtable_cur_->key(), sst_cur_->key); }
                    else { cmp = has_mt ? -1 : 1; }

                    std::vector<uint8_t> key;
                    std::optional<std::vector<uint8_t>> value;
                    bool tombstone = false;

                    if (cmp <= 0) {
                        const auto record = *memtable_cur_;
                        copy_span(record.key(), key);
                        tombstone = record.is_tombstone();
                        if (!tombstone) { value = resolve(record.flags(), record.value()); }
                        advance_memtable();
                        if (cmp == 0) { advance_sst(); }
                    }
                    else {
                        const auto record = std::move(*sst_cur_);
                        key = record.key;
                        tombstone = record.is_tombstone();
                        if (!tombstone) { value = resolve(record.flags, record.value); }
                        advance_sst();
                    }

                    if (!tombstone && value) {
                        pending_ = std::make_pair(std::move(key), std::move(*value));
                        return;
                    }
                }
            }
    };

    AkkEngine::ScanIterator::ScanIterator(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    AkkEngine::ScanIterator::~ScanIterator() = default;
    AkkEngine::ScanIterator::ScanIterator(ScanIterator&&) noexcept = default;
    AkkEngine::ScanIterator& AkkEngine::ScanIterator::operator=(ScanIterator&&) noexcept = default;
    bool AkkEngine::ScanIterator::has_next() const noexcept { return impl_ && impl_->has_next(); }
    std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> AkkEngine::ScanIterator::next() {
        return impl_ ? impl_->next() : std::nullopt;
    }

    class AkkEngine::Impl {
        public:
            AkkEngineOptions opts;
            std::atomic<bool> closed{false};
            uint64_t node_id = 0;

            std::unique_ptr<manifest::Manifest> manifest;
            std::unique_ptr<sst::SSTManager> sst_manager;
            std::unique_ptr<memtable::MemTable> memtable;
            std::unique_ptr<wal::WalWriter> wal_writer;
            std::unique_ptr<blob::BlobManager> blob_manager;
            std::unique_ptr<vlog::VersionLog> version_log;
            std::unique_ptr<cluster::ClusterRuntime> cluster_runtime;

            mutable std::mutex write_mu;

            [[nodiscard]] uint64_t snapshot_seq() const noexcept { return memtable ? memtable->last_seq() : 0; }

            [[nodiscard]] bool persistent() const noexcept { return opts.components.wal_enabled || opts.components.sst_enabled || opts.components.manifest_enabled; }

            [[nodiscard]] std::vector<uint8_t> maybe_externalize(uint64_t seq, std::span<const uint8_t> value, uint8_t& flags) {
                if (!blob_manager || value.size() < blob_manager->threshold()) { return {value.begin(), value.end()}; }

                blob_manager->write(seq, value);
                std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
                blob::encode_blob_ref(ref.data(), blob::BlobRef{seq, static_cast<uint64_t>(value.size()), blob::crc32c(value)});
                flags |= core::MemHdr16::FLAG_BLOB;
                if (cluster_runtime) { cluster_runtime->ship_blob(seq, seq, value); }
                return ref;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> resolve_value(uint8_t flags, std::span<const uint8_t> value) const {
                if ((flags & core::MemHdr16::FLAG_BLOB) == 0) { return std::vector<uint8_t>{value.begin(), value.end()}; }
                if (!blob_manager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
                const blob::BlobRef ref = blob::decode_blob_ref(value.data());
                auto out = blob_manager->read(ref.blob_id, ref.content_crc32c);
                if (out.empty() && ref.total_size != 0) { return std::nullopt; }
                return out;
            }

            void append_all(uint64_t seq, std::span<const uint8_t> key, std::span<const uint8_t> stored_value, uint8_t flags, uint64_t source_node_id) {
                const uint64_t fp64 = core::compute_key_fp64(key);
                const uint64_t mini = core::build_mini_key(key);
                if (wal_writer) { wal_writer->append(key, stored_value, seq, flags, fp64); }
                if (version_log) { version_log->append(key, seq, source_node_id, now_ns(), flags, stored_value); }

                if ((flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, stored_value, seq, flags, fp64, mini); }
            }

            void apply_replica_record(
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            ) {
                std::lock_guard lock(write_mu);
                uint8_t flags = record_flags;
                if (op == cluster::ReplOpType::Remove) { flags |= core::MemHdr16::FLAG_TOMBSTONE; }
                append_all(seq, key, value, flags, source_node_id);
                memtable->advance_seq(seq);
            }
    };

    AkkEngine::AkkEngine() = default;
    AkkEngine::~AkkEngine() { close(); }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
        auto fill_path = [&](fs::path& target, const char* fallback) {
            if (target.empty() && !options.paths.data_dir.empty()) { target = options.paths.data_dir / fallback; }
        };

        fill_path(options.paths.wal_dir, "wal");
        fill_path(options.paths.blob_dir, "blobs");
        fill_path(options.paths.sst_dir, "sstable");
        fill_path(options.paths.manifest_path, "manifest.akmf");
        fill_path(options.paths.version_log_path, "history.akvlog");
        fill_path(options.paths.cluster_config_path, "cluster.akcc");
        fill_path(options.paths.node_id_path, "node.id");

        if (options.wal.wal_dir.empty()) { options.wal.wal_dir = options.paths.wal_dir; }
        if (options.blob.blob_dir.empty()) { options.blob.blob_dir = options.paths.blob_dir; }
        if (options.sst.sst_dir.empty()) { options.sst.sst_dir = options.paths.sst_dir; }
        if (options.vlog.log_path.empty()) { options.vlog.log_path = options.paths.version_log_path; }

        if (options.runtime.writer_threads > 0) {
            if (options.memtable.expected_concurrent_writers == 0) { options.memtable.expected_concurrent_writers = options.runtime.writer_threads; }
            if (options.memtable.shard_count == 0) { options.memtable.shard_count = shards_for_threads(options.runtime.writer_threads, 256); }
            if (options.wal.shard_count == 0) { options.wal.shard_count = static_cast<uint16_t>(shards_for_threads(options.runtime.writer_threads, 64)); }
        }

        if (!options.paths.data_dir.empty()) { ensure_dir(options.paths.data_dir); }
        if (options.components.wal_enabled) { ensure_dir(options.wal.wal_dir); }
        if (options.components.blob_enabled) { ensure_dir(options.blob.blob_dir); }
        if (options.components.sst_enabled) { ensure_dir(options.sst.sst_dir); }
        if (options.components.manifest_enabled) { ensure_dir(options.paths.manifest_path.parent_path()); }
        if (options.components.version_log_enabled) { ensure_dir(options.vlog.log_path.parent_path()); }

        auto engine = std::unique_ptr<AkkEngine>{new AkkEngine()};
        engine->impl_ = std::make_unique<Impl>();
        Impl& impl = *engine->impl_;
        impl.opts = std::move(options);
        impl.node_id = load_or_create_node_id(impl.opts.paths.node_id_path);

        if (impl.opts.components.manifest_enabled && !impl.opts.paths.manifest_path.empty()) {
            impl.manifest = manifest::Manifest::create(impl.opts.paths.manifest_path, impl.opts.manifest.fast_mode);
            impl.manifest->start();
        }

        if (impl.opts.components.sst_enabled && !impl.opts.sst.sst_dir.empty()) {
            impl.sst_manager = sst::SSTManager::create(impl.opts.sst, impl.manifest.get());
            if (impl.opts.runtime.recover_sst) { impl.sst_manager->recover(); }
        }

        impl.opts.memtable.on_flush = [&impl](std::span<const core::RecordView> records) {
            if (!impl.sst_manager || records.empty()) { return; }
            const uint64_t checkpoint_seq = impl.sst_manager->flush(records);
            if (impl.wal_writer && impl.opts.runtime.prune_wal_on_flush) { impl.wal_writer->prune_until(checkpoint_seq); }
            if (impl.manifest) { impl.manifest->checkpoint(std::optional<std::string>{"flush"}, std::nullopt, checkpoint_seq); }
        };
        impl.memtable = memtable::MemTable::create(impl.opts.memtable);

        if (impl.opts.components.wal_enabled && impl.opts.runtime.recover_wal) {
            const auto recovery = wal::WalRecovery::recover_into(wal::WalRecoveryOptions{.wal_dir = impl.opts.wal.wal_dir}, *impl.memtable);
            (void)recovery;
        }

        if (impl.opts.components.wal_enabled) { impl.wal_writer = wal::WalWriter::create(impl.opts.wal); }

        if (impl.opts.components.blob_enabled) {
            impl.blob_manager = blob::BlobManager::create(impl.opts.blob);
            impl.blob_manager->start();
        }

        if (impl.opts.components.version_log_enabled) { impl.version_log = vlog::VersionLog::create(impl.opts.vlog); }

        if (impl.opts.components.cluster_enabled) {
            cluster::ClusterConfig cfg = impl.opts.cluster.config.has_value()
                ? *impl.opts.cluster.config
                : cluster::ClusterConfig::load(impl.opts.paths.cluster_config_path);
            cluster::ClusterEngineCallbacks callbacks;
            callbacks.get_current_seq = [&impl] { return impl.snapshot_seq(); };
            callbacks.get_last_seq = [&impl] { return impl.snapshot_seq(); };
            callbacks.apply = [&impl](
                                  uint64_t seq,
                                  cluster::ReplOpType op,
                                  std::span<const uint8_t> key,
                                  std::span<const uint8_t> value,
                                  uint8_t record_flags,
                                  uint64_t source_node_id
                              ) { impl.apply_replica_record(seq, op, key, value, record_flags, source_node_id); };
            callbacks.apply_blob = [&impl](uint64_t /*seq*/, uint64_t blob_id, std::span<const uint8_t> content) {
                if (impl.blob_manager) { impl.blob_manager->write(blob_id, content); }
            };

            impl.cluster_runtime = cluster::ClusterRuntime::create(
                impl.opts.paths.data_dir,
                std::move(cfg),
                impl.node_id,
                std::move(callbacks),
                impl.opts.cluster.runtime
            );
            impl.cluster_runtime->start();
        }

        return engine;
    }

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        std::lock_guard lock(impl_->write_mu);

        const uint64_t seq = impl_->memtable->reserve_seq(1);
        uint8_t flags = core::MemHdr16::FLAG_NORMAL;
        std::vector<uint8_t> stored = impl_->maybe_externalize(seq, value, flags);
        impl_->append_all(seq, key, stored, flags, impl_->node_id);
        if (impl_->cluster_runtime) { impl_->cluster_runtime->ship_entry(seq, cluster::ReplOpType::Put, key, stored, flags, impl_->node_id); }
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        std::lock_guard lock(impl_->write_mu);

        const uint64_t seq = impl_->memtable->reserve_seq(1);
        constexpr uint8_t flags = core::MemHdr16::FLAG_TOMBSTONE;
        impl_->append_all(seq, key, {}, flags, impl_->node_id);
        if (impl_->cluster_runtime) { impl_->cluster_runtime->ship_entry(seq, cluster::ReplOpType::Remove, key, {}, flags, impl_->node_id); }
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        const uint64_t seq = impl_->snapshot_seq();

        core::RecordView view;
        if (impl_->memtable->get(key, seq, &view)) {
            if (view.is_tombstone()) { return std::nullopt; }
            return impl_->resolve_value(view.flags(), view.value());
        }

        if (impl_->sst_manager) {
            auto record = impl_->sst_manager->get(key);
            if (record) {
                if (record->is_tombstone()) { return std::nullopt; }
                return impl_->resolve_value(record->flags, record->value);
            }
        }
        return std::nullopt;
    }

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        const uint64_t seq = impl_->snapshot_seq();
        if (const auto mt = impl_->memtable->contains(key, seq); mt.has_value()) { return *mt; }
        if (impl_->sst_manager) {
            if (const auto sst = impl_->sst_manager->contains(key); sst.has_value()) { return *sst; }
        }
        return false;
    }

    bool AkkEngine::get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (impl_->blob_manager) {
            auto value = get(key);
            if (!value) { return false; }
            out = std::move(*value);
            return true;
        }

        const uint64_t seq = impl_->snapshot_seq();
        if (const auto mt = impl_->memtable->get_into(key, seq, out); mt.has_value()) { return *mt; }
        if (impl_->sst_manager) {
            if (const auto sst = impl_->sst_manager->get_into(key, out); sst.has_value()) { return *sst; }
        }
        return false;
    }

    AkkEngine::ScanIterator AkkEngine::scan(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        memtable::MemTable::KeyRange range;
        range.start.assign(start_key.begin(), start_key.end());
        range.end.assign(end_key.begin(), end_key.end());
        auto mt = impl_->memtable->iterator(range, impl_->snapshot_seq());
        sst::SSTManager::Iterator st;
        if (impl_->sst_manager) { st = impl_->sst_manager->scan_iter(start_key, end_key); }
        return ScanIterator{std::make_unique<ScanIterator::Impl>(std::move(mt), std::move(st), impl_->blob_manager.get())};
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get_at(std::span<const uint8_t> key, uint64_t at_seq) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->version_log) { return std::nullopt; }
        auto entry = impl_->version_log->get_at(key, at_seq);
        if (!entry || (entry->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { return std::nullopt; }
        return impl_->resolve_value(entry->flags, entry->value);
    }

    std::vector<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->version_log ? impl_->version_log->history(key) : std::vector<VersionEntry>{};
    }

    void AkkEngine::rollback_to(uint64_t target_seq) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->version_log) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        for (const auto& [key, prev] : impl_->version_log->collect_rollback_targets(target_seq)) {
            if (!prev || (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { remove(key); }
            else {
                auto value = impl_->resolve_value(prev->flags, prev->value);
                if (value) { put(key, *value); }
                else { remove(key); }
            }
        }
    }

    void AkkEngine::rollback_key(std::span<const uint8_t> key, uint64_t target_seq) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->version_log) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        const auto prev = impl_->version_log->get_at(key, target_seq);
        if (!prev || (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { remove(key); }
        else {
            auto value = impl_->resolve_value(prev->flags, prev->value);
            if (value) { put(key, *value); }
            else { remove(key); }
        }
    }

    void AkkEngine::force_sync() {
        if (impl_ && impl_->wal_writer) { impl_->wal_writer->force_sync(); }
    }

    void AkkEngine::force_flush() {
        if (impl_ && impl_->memtable) { impl_->memtable->force_flush(); }
    }

    void AkkEngine::close() {
        if (!impl_) { return; }
        bool expected = false;
        if (!impl_->closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) { return; }

        if (impl_->cluster_runtime) {
            impl_->cluster_runtime->close();
            impl_->cluster_runtime.reset();
        }
        if (impl_->memtable && impl_->opts.runtime.force_flush_on_close) { impl_->memtable->force_flush(); }
        if (impl_->sst_manager) {
            impl_->sst_manager->shutdown();
            impl_->sst_manager.reset();
        }
        if (impl_->wal_writer) {
            if (impl_->opts.runtime.force_sync_on_close) { impl_->wal_writer->force_sync(); }
            impl_->wal_writer->close();
            impl_->wal_writer.reset();
        }
        if (impl_->manifest) {
            impl_->manifest->close();
            impl_->manifest.reset();
        }
        if (impl_->blob_manager) {
            impl_->blob_manager->close();
            impl_->blob_manager.reset();
        }
        if (impl_->version_log) {
            impl_->version_log->close();
            impl_->version_log.reset();
        }
        impl_->memtable.reset();
    }
} // namespace akkaradb::engine
