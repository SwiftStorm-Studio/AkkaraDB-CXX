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

// internal/src/engine/cluster/ClusterConfig.cpp
#include "engine/cluster/ClusterConfig.hpp"
#include "core/CRC32C.hpp"
#include <chrono>
#include <cstring>
#include <fstream>
#include <stdexcept>

namespace akkaradb::engine::cluster {

    namespace {

        // --- Little-endian helpers ---
        inline void write_u16(uint8_t* b, size_t off, uint16_t v) noexcept {
            b[off]   = static_cast<uint8_t>(v);
            b[off+1] = static_cast<uint8_t>(v >> 8);
        }
        inline void write_u32(uint8_t* b, size_t off, uint32_t v) noexcept {
            b[off]   = static_cast<uint8_t>(v);
            b[off+1] = static_cast<uint8_t>(v >> 8);
            b[off+2] = static_cast<uint8_t>(v >> 16);
            b[off+3] = static_cast<uint8_t>(v >> 24);
        }
        inline void write_u64(uint8_t* b, size_t off, uint64_t v) noexcept {
            for (int i = 0; i < 8; ++i) { b[off+i] = static_cast<uint8_t>(v >> (8*i)); }
        }
        inline uint16_t read_u16(const uint8_t* b, size_t off) noexcept {
            return static_cast<uint16_t>(b[off]) | (static_cast<uint16_t>(b[off+1]) << 8);
        }
        inline uint32_t read_u32(const uint8_t* b, size_t off) noexcept {
            return static_cast<uint32_t>(b[off])        |
                   (static_cast<uint32_t>(b[off+1])<<8) |
                   (static_cast<uint32_t>(b[off+2])<<16)|
                   (static_cast<uint32_t>(b[off+3])<<24);
        }
        inline uint64_t read_u64(const uint8_t* b, size_t off) noexcept {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(b[off+i]) << (8*i); }
            return v;
        }

        static constexpr size_t HDR_SIZE = 32;

        uint64_t now_us() noexcept {
            return static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count()
            );
        }

    } // anonymous namespace

    // ============================================================================
    // ClusterConfig::load
    // ============================================================================

    ClusterConfig ClusterConfig::load(const std::filesystem::path& path) {
        std::ifstream f(path, std::ios::binary);
        if (!f) {
            throw std::runtime_error("ClusterConfig: cannot open " + path.string());
        }

        // --- File header (32 bytes) ---
        uint8_t hdr[HDR_SIZE];
        f.read(reinterpret_cast<char*>(hdr), HDR_SIZE);
        if (!f || f.gcount() < static_cast<std::streamsize>(HDR_SIZE)) {
            throw std::runtime_error("ClusterConfig: file too short");
        }

        const uint32_t magic   = read_u32(hdr, 0);
        const uint16_t version = read_u16(hdr, 4);
        if (magic != MAGIC) {
            throw std::runtime_error("ClusterConfig: bad magic");
        }
        if (version != VERSION) {
            throw std::runtime_error("ClusterConfig: unsupported version");
        }

        // Verify CRC32C (crc field at offset 20, zero it for computation)
        const uint32_t stored_crc = read_u32(hdr, 20);
        uint8_t hdr_for_crc[HDR_SIZE];
        std::memcpy(hdr_for_crc, hdr, HDR_SIZE);
        write_u32(hdr_for_crc, 20, 0);
        const uint32_t computed_crc = core::CRC32C::compute(hdr_for_crc, HDR_SIZE);
        if (stored_crc != computed_crc) {
            throw std::runtime_error("ClusterConfig: header CRC mismatch");
        }

        const uint16_t flags = read_u16(hdr, 6);
        const uint16_t node_count = read_u16(hdr, 8);
        const auto     mode        = static_cast<ReplicationMode>(hdr[10]);
        const uint8_t  repl_factor = hdr[11];

        // --- Per-node entries ---
        std::vector<NodeInfo> nodes;
        nodes.reserve(node_count);
        for (uint16_t i = 0; i < node_count; ++i) {
            uint8_t entry_hdr[14]; // node_id(8) + data_port(2) + repl_port(2) + host_len(2)
            f.read(reinterpret_cast<char*>(entry_hdr), 14);
            if (!f || f.gcount() < 14) {
                throw std::runtime_error("ClusterConfig: truncated node entry");
            }
            NodeInfo ni;
            ni.node_id   = read_u64(entry_hdr, 0);
            ni.data_port = read_u16(entry_hdr, 8);
            ni.repl_port = read_u16(entry_hdr, 10);
            const uint16_t host_len = read_u16(entry_hdr, 12);
            ni.host.resize(host_len);
            f.read(ni.host.data(), host_len);
            if (!f || f.gcount() < host_len) {
                throw std::runtime_error("ClusterConfig: truncated host string");
            }
            nodes.push_back(std::move(ni));
        }

        ClusterConfig cfg{std::move(nodes), mode, repl_factor};
        if (flags & ClusterConfig::FLAG_WEB_CONFIG_ENABLED) cfg.set_web_config_enabled(true);
        return cfg;
    }

    // ============================================================================
    // ClusterConfig::save
    // ============================================================================

    void ClusterConfig::save(const std::filesystem::path& path, const ClusterConfig& cfg) {
        // Build the entire file in memory, then write atomically via temp file.
        std::vector<uint8_t> buf;
        buf.reserve(HDR_SIZE + cfg.nodes_.size() * 32);

        // --- File header ---
        buf.resize(HDR_SIZE);
        uint8_t* h = buf.data();
        write_u32(h,  0, MAGIC);
        write_u16(h,  4, VERSION);
        write_u16(h, 6, cfg.flags());
        write_u16(h,  8, static_cast<uint16_t>(cfg.nodes_.size()));
        h[10] = static_cast<uint8_t>(cfg.mode_);
        h[11] = cfg.repl_factor_;
        write_u64(h, 12, now_us());
        write_u32(h, 20, 0); // crc placeholder
        std::memset(h + 24, 0, 8); // reserved... wait, crc is at 24?

        // Layout: [magic:u32][version:u16][flags:u16][node_count:u16][mode:u8][repl_factor:u8]
        //         [created_at_us:u64][crc32c:u32][reserved:u8×8]
        // Offsets: 0       4       6       8            10      11
        //          12               20       24
        // Total: 4+2+2+2+1+1+8+4+8 = 32 ✓

        // Per-node entries
        for (const auto& ni : cfg.nodes_) {
            const auto host_len = static_cast<uint16_t>(ni.host.size());
            const size_t off = buf.size();
            buf.resize(off + 14 + host_len);
            uint8_t* e = buf.data() + off;
            write_u64(e, 0, ni.node_id);
            write_u16(e, 8, ni.data_port);
            write_u16(e, 10, ni.repl_port);
            write_u16(e, 12, host_len);
            std::memcpy(e + 14, ni.host.data(), host_len);
        }

        // Compute and write CRC over header (with crc field = 0)
        const uint32_t crc = core::CRC32C::compute(buf.data(), HDR_SIZE);
        write_u32(buf.data(), 20, crc);

        // Atomic write: write to temp file, then rename
        const auto tmp_path = path.parent_path() / (path.filename().string() + ".tmp");
        {
            std::ofstream f(tmp_path, std::ios::binary | std::ios::trunc);
            if (!f) {
                throw std::runtime_error("ClusterConfig: cannot create " + tmp_path.string());
            }
            f.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
            if (!f) {
                throw std::runtime_error("ClusterConfig: write failed");
            }
        }
        std::filesystem::rename(tmp_path, path);
    }

    // ============================================================================
    // ClusterConfig::find_by_id
    // ============================================================================

    const NodeInfo* ClusterConfig::find_by_id(uint64_t node_id) const noexcept {
        for (const auto& ni : nodes_) {
            if (ni.node_id == node_id) { return &ni; }
        }
        return nullptr;
    }

} // namespace akkaradb::engine::cluster
