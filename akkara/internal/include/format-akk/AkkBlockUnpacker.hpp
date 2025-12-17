// internal/include/format-akk/AkkBlockUnpacker.hpp
#pragma once

#include "format-api/BlockUnpacker.hpp"
#include "format-api/RecordCursor.hpp"

namespace akkaradb::format::akk {
    /**
     * AkkBlockUnpacker - Concrete implementation of BlockUnpacker for AkkaraDB format.
     *
     * Unpacks records from 32 KiB blocks with the following layout:
     * [0..3]       payloadLen (u32 LE)
     * [4..N)       payload = repeated { AKHdr32(32B) + key + value }
     * [N..32764)   zero padding
     * [32764..32768) CRC32C (u32 LE)
     *
     * Design principles:
     * - Zero-copy: Returns RecordView (non-owning pointers)
     * - CRC validation: Always validates checksum before iteration
     * - Bounds checking: Validates all record boundaries
     * - Cursor-based: Forward-only iteration
     *
     * Thread-safety: Stateless, so thread-safe if called with different buffers.
     * Cursors are NOT thread-safe.
     */
    class AkkBlockUnpacker : public BlockUnpacker {
    public:
        /**
         * Block size constant (32 KiB).
         */
        static constexpr size_t BLOCK_SIZE = 32 * 1024;

        /**
         * Creates an AkkBlockUnpacker.
         *
         * @return Unique pointer to unpacker
         */
        [[nodiscard]] static std::unique_ptr<AkkBlockUnpacker> create();

        ~AkkBlockUnpacker() override;

        [[nodiscard]] std::unique_ptr<RecordCursor> cursor(core::BufferView block) const override;

        void unpack_into(core::BufferView block, std::vector<core::RecordView>& out) const override;

        [[nodiscard]] bool validate(core::BufferView block) const noexcept override;

        [[nodiscard]] size_t block_size() const noexcept override { return BLOCK_SIZE; }

    private:
        AkkBlockUnpacker() = default;
    };

    /**
     * AkkRecordCursor - Forward-only cursor over records in an AkkaraDB block.
     *
     * This class provides zero-copy iteration through records.
     *
     * Thread-safety: NOT thread-safe.
     */
    class AkkRecordCursor : public RecordCursor {
    public:
        /**
         * Creates a cursor from a validated block.
         *
         * @param block Block buffer (must remain valid during cursor lifetime)
         * @param payload_len Length of payload section
         * @throws std::runtime_error if CRC validation fails
         * @throws std::out_of_range if block is malformed
         */
        [[nodiscard]] static std::unique_ptr<AkkRecordCursor> create(
            core::BufferView block,
            uint32_t payload_len
        );

        [[nodiscard]] bool has_next() const noexcept override;

        [[nodiscard]] std::optional<core::RecordView> try_next() override;

    private:
        AkkRecordCursor(core::BufferView block, uint32_t payload_len);

        core::BufferView block_;
        uint32_t payload_len_;
    size_t current_offset_;  // Current position in payload (relative to offset 4)
};

} // namespace akkaradb::format::akk