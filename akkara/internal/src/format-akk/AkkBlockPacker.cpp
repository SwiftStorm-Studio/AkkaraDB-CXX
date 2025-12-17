// internal/src/format-akk/AkkBlockPacker.cpp
#include "format-akk/AkkBlockPacker.hpp"
#include "core/buffer/BufferView.hpp"
#include "core/record/AKHdr32.hpp"
#include <cstring>

namespace akkaradb::format::akk {
    /**
 * AkkBlockPacker::Impl - Private implementation (Pimpl idiom).
 */
    class AkkBlockPacker::Impl {
    public:
        Impl(BlockReadyCallback callback, std::shared_ptr<core::BufferPool> pool) : callback_{std::move(callback)}, pool_{std::move(pool)} {}

        void begin_block() {
            if (current_buffer_.empty()) {
                // Acquire new buffer from pool
                current_buffer_ = pool_->acquire();
                current_buffer_.zero_fill();

                // Reserve space for payloadLen at offset 0
                payload_offset_ = sizeof(uint32_t);
                record_count_ = 0;
            }
            else if (record_count_ > 0) {
                // Non-empty block already open, seal it first
                end_block();
                begin_block();
            }
            // else: empty block already open, reuse it
        }

        bool try_append(
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint64_t seq,
            uint8_t flags,
            uint64_t key_fp64,
            uint64_t mini_key
        ) {
            if (current_buffer_.empty()) {
                return false; // No block open
            }

            // Calculate required space
            const size_t record_size = sizeof(core::AKHdr32) + key.size() + value.size();

            // Check if block has enough space (留 CRC space)
            if (const size_t required = payload_offset_ + record_size; required > MAX_PAYLOAD) {
                return false; // Block full
            }

            // Write AKHdr32
            const core::AKHdr32 header{
                .k_len = static_cast<uint16_t>(key.size()),
                .v_len = static_cast<uint32_t>(value.size()),
                .seq = seq,
                .flags = flags,
                .pad0 = 0,
                .key_fp64 = key_fp64,
                .mini_key = mini_key
            };

            auto view = current_buffer_.view();
            std::memcpy(view.data() + payload_offset_, &header, sizeof(header));
            payload_offset_ += sizeof(header);

            // Write key
            if (!key.empty()) {
                std::memcpy(view.data() + payload_offset_, key.data(), key.size());
                payload_offset_ += key.size();
            }

            // Write value
            if (!value.empty()) {
                std::memcpy(view.data() + payload_offset_, value.data(), value.size());
                payload_offset_ += value.size();
            }

            ++record_count_;
            return true;
        }

        void end_block() {
            if (current_buffer_.empty() || record_count_ == 0) {
                return; // No block to seal
            }

            auto view = current_buffer_.view();

            // Write payloadLen at offset 0
            const uint32_t payload_len = payload_offset_ - sizeof(uint32_t);
            view.write_u32_le(0, payload_len);

            // Zero-fill padding (already zero-filled on acquire, but be explicit)
            const size_t padding_start = payload_offset_;
            constexpr size_t padding_end = BLOCK_SIZE - sizeof(uint32_t);
            if (padding_end > padding_start) { view.fill(padding_start, padding_end - padding_start, std::byte{0}); }

            // Compute CRC32C over [0..32764)
            const uint32_t crc = view.crc32c(0, BLOCK_SIZE - sizeof(uint32_t));

            // Write CRC32C at offset 32764
            view.write_u32_le(BLOCK_SIZE - sizeof(uint32_t), crc);

            // Emit block via callback
            callback_(std::move(current_buffer_));

            // Reset state
            current_buffer_ = core::OwnedBuffer{};
            payload_offset_ = 0;
            record_count_ = 0;
        }

        void flush() { end_block(); }

        [[nodiscard]] size_t remaining() const noexcept {
            if (current_buffer_.empty()) { return 0; }
            return MAX_PAYLOAD - payload_offset_;
        }

        [[nodiscard]] size_t record_count() const noexcept { return record_count_; }

    private:
        BlockReadyCallback callback_;
        std::shared_ptr<core::BufferPool> pool_;

        core::OwnedBuffer current_buffer_;
        size_t payload_offset_{0};
        size_t record_count_{0};
    };

    // ==================== AkkBlockPacker Public API ====================

    std::unique_ptr<AkkBlockPacker> AkkBlockPacker::create(
        BlockReadyCallback callback,
        std::shared_ptr<core::BufferPool> pool
    ) { return std::unique_ptr<AkkBlockPacker>(new AkkBlockPacker(std::move(callback), std::move(pool))); }

    AkkBlockPacker::AkkBlockPacker(
        BlockReadyCallback callback,
        std::shared_ptr<core::BufferPool> pool
    ) : impl_{std::make_unique<Impl>(std::move(callback), std::move(pool))} {}

    AkkBlockPacker::~AkkBlockPacker() = default;

    void AkkBlockPacker::begin_block() { impl_->begin_block(); }

    bool AkkBlockPacker::try_append(
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint64_t seq,
        uint8_t flags,
        uint64_t key_fp64,
        uint64_t mini_key
    ) { return impl_->try_append(key, value, seq, flags, key_fp64, mini_key); }

    void AkkBlockPacker::end_block() { impl_->end_block(); }

    void AkkBlockPacker::flush() { impl_->flush(); }

    size_t AkkBlockPacker::remaining() const noexcept { return impl_->remaining(); }

    size_t AkkBlockPacker::record_count() const noexcept { return impl_->record_count(); }
} // namespace akkaradb::format::akk