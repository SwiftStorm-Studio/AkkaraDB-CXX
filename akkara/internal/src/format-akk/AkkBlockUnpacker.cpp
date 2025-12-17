// internal/src/format-akk/AkkBlockUnpacker.cpp
#include "format-akk/AkkBlockUnpacker.hpp"
#include "core/buffer/BufferView.hpp"
#include "core/record/AKHdr32.hpp"
#include "core/record/RecordView.hpp"
#include <stdexcept>

namespace akkaradb::format::akk {
    namespace {
        /**
         * Validates block CRC32C.
         *
         * @return true if CRC matches
         */
        bool validate_crc(core::BufferView block) noexcept {
            if (block.size() != AkkBlockUnpacker::BLOCK_SIZE) { return false; }

            // Read stored CRC at offset 32764
            const uint32_t stored_crc = block.read_u32_le(AkkBlockUnpacker::BLOCK_SIZE - sizeof(uint32_t));

            // Compute CRC over [0..32764)
            const uint32_t computed_crc = block.crc32c(0, AkkBlockUnpacker::BLOCK_SIZE - sizeof(uint32_t));

            return stored_crc == computed_crc;
        }
    } // anonymous namespace

    // ==================== AkkBlockUnpacker Implementation ====================

    std::unique_ptr<AkkBlockUnpacker> AkkBlockUnpacker::create() { return std::unique_ptr<AkkBlockUnpacker>(new AkkBlockUnpacker()); }

    AkkBlockUnpacker::~AkkBlockUnpacker() = default;

    std::unique_ptr<RecordCursor> AkkBlockUnpacker::cursor(core::BufferView block) const {
        if (block.size() != BLOCK_SIZE) { throw std::out_of_range("AkkBlockUnpacker::cursor: block size must be 32 KiB"); }

        // Validate CRC
        if (!validate_crc(block)) { throw std::runtime_error("AkkBlockUnpacker::cursor: CRC validation failed"); }

        // Read payloadLen
        const uint32_t payload_len = block.read_u32_le(0);

        // Validate payload length
        if (constexpr size_t max_payload = BLOCK_SIZE - sizeof(uint32_t) * 2; payload_len > max_payload) {
            throw std::out_of_range("AkkBlockUnpacker::cursor: invalid payload length");
        }

        return AkkRecordCursor::create(block, payload_len);
    }

    void AkkBlockUnpacker::unpack_into(core::BufferView block, std::vector<core::RecordView>& out) const {
        auto cur = cursor(block);

        while (cur->has_next()) {
            auto record_opt = cur->try_next();
            if (!record_opt) { throw std::runtime_error("AkkBlockUnpacker::unpack_into: malformed record"); }
            out.push_back(*record_opt);
        }
    }

    bool AkkBlockUnpacker::validate(core::BufferView block) const noexcept {
        if (block.size() != BLOCK_SIZE) { return false; }

        // Validate CRC
        if (!validate_crc(block)) { return false; }

        // Validate payload length
        try {
            const uint32_t payload_len = block.read_u32_le(0);
            constexpr size_t max_payload = BLOCK_SIZE - sizeof(uint32_t) * 2;
            if (payload_len > max_payload) { return false; }
        }
        catch (...) { return false; }

        return true;
    }

    // ==================== AkkRecordCursor Implementation ====================

    std::unique_ptr<AkkRecordCursor> AkkRecordCursor::create(
        core::BufferView block,
        uint32_t payload_len
    ) { return std::unique_ptr<AkkRecordCursor>(new AkkRecordCursor(block, payload_len)); }

    AkkRecordCursor::AkkRecordCursor(core::BufferView block, uint32_t payload_len) : block_{block}, payload_len_{payload_len}, current_offset_{0} {}

    bool AkkRecordCursor::has_next() const noexcept { return current_offset_ < payload_len_; }

    std::optional<core::RecordView> AkkRecordCursor::try_next() {
        if (!has_next()) { return std::nullopt; }

        // Calculate absolute offset (payload starts at offset 4)
        const size_t absolute_offset = sizeof(uint32_t) + current_offset_;

        // Check if we have space for header
        if (absolute_offset + sizeof(core::AKHdr32) > block_.size()) {
            return std::nullopt; // Malformed: not enough space for header
        }

        // Read header
        const auto* header_ptr = reinterpret_cast<const core::AKHdr32*>(
            block_.data() + absolute_offset
        );

        // Calculate total record size
        const size_t record_size = sizeof(core::AKHdr32) + header_ptr->k_len + header_ptr->v_len;

        // Check bounds
        if (current_offset_ + record_size > payload_len_) {
            return std::nullopt; // Malformed: record extends beyond payload
        }

        // Construct RecordView
        const auto* key_ptr = reinterpret_cast<const uint8_t*>(header_ptr + 1);
        const auto* value_ptr = key_ptr + header_ptr->k_len;

        core::RecordView view{header_ptr, key_ptr, value_ptr};

        // Advance offset
    current_offset_ += record_size;

    return view;
}

} // namespace akkaradb::format::akk