#pragma once
#include <cstdint>
#include <chrono>

namespace akkaradb::format::api {

/**
 * @brief Flush mode enumeration.
 * 
 * Determines when buffered data should be flushed to persistent storage.
 */
enum class FlushMode {
    /**
     * Flush immediately after every write operation.
     * Provides maximum durability at the cost of performance.
     */
    IMMEDIATE,
    
    /**
     * Flush when buffer reaches a size threshold or timeout occurs.
     * Balances durability and performance through group commits.
     */
    BUFFERED,
    
    /**
     * Never flush automatically; caller must explicitly request flush.
     * Maximizes throughput but requires manual durability management.
     */
    MANUAL
};

/**
 * @brief Policy for controlling when buffered writes are flushed.
 * 
 * A flush policy defines thresholds (count-based and time-based) that
 * trigger a flush operation in buffered mode. This allows applications
 * to tune the trade-off between write latency and durability.
 * 
 * Example:
 * @code
 * FlushPolicy policy{
 *     .mode = FlushMode::BUFFERED,
 *     .max_buffered_blocks = 32,
 *     .max_buffer_duration = std::chrono::microseconds(500)
 * };
 * @endcode
 */
struct FlushPolicy {
    /** Flush mode. */
    FlushMode mode = FlushMode::BUFFERED;
    
    /**
     * Maximum number of blocks to buffer before forcing a flush.
     * Only applies in BUFFERED mode.
     * Default: 32 blocks.
     */
    uint32_t max_buffered_blocks = 32;
    
    /**
     * Maximum duration to buffer writes before forcing a flush.
     * Only applies in BUFFERED mode.
     * Default: 500 microseconds.
     */
    std::chrono::microseconds max_buffer_duration{500};
    
    /**
     * @brief Check if a flush should be triggered based on count.
     * @param current_count Number of currently buffered blocks.
     * @return true if flush is required, false otherwise.
     */
    [[nodiscard]]
    bool should_flush_by_count(uint32_t current_count) const noexcept {
        return mode == FlushMode::BUFFERED && current_count >= max_buffered_blocks;
    }
    
    /**
     * @brief Check if a flush should be triggered based on elapsed time.
     * @param elapsed Time since last flush.
     * @return true if flush is required, false otherwise.
     */
    [[nodiscard]]
    bool should_flush_by_time(std::chrono::microseconds elapsed) const noexcept {
        return mode == FlushMode::BUFFERED && elapsed >= max_buffer_duration;
    }
};

} // namespace akkaradb::format::api