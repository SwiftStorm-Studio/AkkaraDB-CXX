/*
 * AkkaraDB - SST v2 smoke tests
 */

#include "core/record/KeyFingerprint.hpp"
#include "core/record/SSTHdr32.hpp"
#include "engine/sstable/SSTManager.hpp"
#include "engine/sstable/SSTReader.hpp"
#include "engine/sstable/SSTWriter.hpp"
#include "engine/memtable/MemTable.hpp"
#include "engine/manifest/Manifest.hpp"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <format>
#include <iostream>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace akkaradb;
namespace sst = akkaradb::engine::sst;

namespace {
    std::vector<uint8_t> bytes(std::string_view s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), reinterpret_cast<const uint8_t*>(s.data() + s.size())};
    }

    std::string str(std::span<const uint8_t> s) {
        return {reinterpret_cast<const char*>(s.data()), s.size()};
    }

    fs::path temp_dir(std::string_view name) {
        auto p = fs::temp_directory_path() / std::format("akkara_{}_{}", name, std::chrono::steady_clock::now().time_since_epoch().count());
        fs::remove_all(p);
        fs::create_directories(p);
        return p;
    }

    struct Records {
        std::vector<std::vector<uint8_t>> keys;
        std::vector<std::vector<uint8_t>> vals;
        std::vector<core::RecordView> views;
    };

    Records make_records(int begin, int count, uint64_t seq_base = 1) {
        Records r;
        r.keys.reserve(static_cast<size_t>(count));
        r.vals.reserve(static_cast<size_t>(count));
        r.views.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
            r.keys.push_back(bytes(std::format("key_{:04}", begin + i)));
            r.vals.push_back(bytes(std::format("value_{:04}", begin + i)));
        }
        for (int i = 0; i < count; ++i) {
            const auto& k = r.keys[static_cast<size_t>(i)];
            const auto& v = r.vals[static_cast<size_t>(i)];
            const uint64_t fp = core::compute_key_fp64(k.data(), k.size());
            const uint64_t mk = core::build_mini_key(k.data(), k.size());
            r.views.emplace_back(k.data(), static_cast<uint16_t>(k.size()), v.data(), static_cast<uint16_t>(v.size()), seq_base + static_cast<uint64_t>(i), core::SSTHdr32::FLAG_NORMAL, fp, mk);
        }
        return r;
    }

    void test_writer_reader_roundtrip() {
        auto dir = temp_dir("sst_roundtrip");
        auto recs = make_records(0, 256);
        const auto path = dir / "one.aksst";

        sst::SSTWriter::Options opts;
        opts.block_size = 4096;
        opts.codec = sst::SSTWriter::Codec::Zstd;
        const auto result = sst::SSTWriter::write(path, recs.views, opts);
        assert(result.entry_count == 256);

        auto reader = sst::SSTReader::open(path);
        assert(reader);
        assert(reader->header().magic == sst::SST_MAGIC_V2);
        assert(reader->header().version == sst::SST_VERSION_V2);

        auto found = reader->get(bytes("key_0042"));
        assert(found);
        assert(str(found->value) == "value_0042");
        assert(!reader->contains(bytes("missing")).has_value());

        std::vector<uint8_t> out;
        auto got = reader->get_into(bytes("key_0100"), out);
        assert(got.has_value() && *got);
        assert(str(out) == "value_0100");

        size_t scanned = 0;
        auto scan = reader->scan(bytes("key_0010"), bytes("key_0020"));
        for (auto&& rec : scan) {
            assert(str(rec.key) >= "key_0010");
            assert(str(rec.key) < "key_0020");
            ++scanned;
        }
        assert(scanned == 10);
        reader.reset();
        fs::remove_all(dir);
    }

    void test_memtable_flush_manager_recover() {
        auto dir = temp_dir("sst_manager");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sst_dir = dir / "sst";
        sopts.max_l0_files = 8;
        sopts.block_size = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        engine::memtable::MemTable::Options mopts;
        mopts.shard_count = 1;
        mopts.threshold_bytes_per_shard = 1ULL << 30;
        mopts.on_flush = [&](std::span<const core::RecordView> batch) { manager->flush(batch); };
        auto mem = engine::memtable::MemTable::create(mopts);
        for (int i = 0; i < 64; ++i) {
            auto k = bytes(std::format("key_{:04}", i));
            auto v = bytes(std::format("value_{:04}", i));
            mem->put(k, v, mem->next_seq());
        }
        mem->force_flush();
        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();

        auto manifest2 = engine::manifest::Manifest::create(dir / "manifest.akmf", false);
        auto manager2 = sst::SSTManager::create(sopts, manifest2.get());
        manager2->recover();
        auto rec = manager2->get(bytes("key_0020"));
        assert(rec);
        assert(str(rec->value) == "value_0020");
        manager2->shutdown();
        manager2.reset();
        manifest2->close();
        manifest2.reset();
        fs::remove_all(dir);
    }

    void test_compaction_overwrite_and_tombstone() {
        auto dir = temp_dir("sst_compact");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sst_dir = dir / "sst";
        sopts.max_levels = 2;
        sopts.max_l0_files = 2;
        sopts.block_size = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        auto v1_key = bytes("dup");
        auto v1_val = bytes("v1");
        const uint64_t fp = core::compute_key_fp64(v1_key.data(), v1_key.size());
        const uint64_t mk = core::build_mini_key(v1_key.data(), v1_key.size());
        std::vector<core::RecordView> batch1;
        batch1.emplace_back(v1_key.data(), static_cast<uint16_t>(v1_key.size()), v1_val.data(), static_cast<uint16_t>(v1_val.size()), 1, core::SSTHdr32::FLAG_NORMAL, fp, mk);
        manager->flush(batch1);

        std::vector<uint8_t> empty;
        std::vector<core::RecordView> batch2;
        batch2.emplace_back(v1_key.data(), static_cast<uint16_t>(v1_key.size()), empty.data(), static_cast<uint16_t>(0), 2, core::SSTHdr32::FLAG_TOMBSTONE, fp, mk);
        manager->flush(batch2);

        for (int i = 0; i < 100; ++i) {
            if (manager->compaction_snapshot().compactions_completed > 0) { break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        auto c = manager->contains(v1_key);
        assert(!c.has_value());
        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();
        fs::remove_all(dir);
    }
}

int main() {
    test_writer_reader_roundtrip();
    test_memtable_flush_manager_recover();
    test_compaction_overwrite_and_tombstone();
    std::cout << "SST v2 smoke tests passed\n";
    return 0;
}
