// internal/src/engine/sstable/SSTableReader.cpp
#include "engine/sstable/SSTableReader.hpp"
#include <algorithm>
#include <stdexcept>
#include <utility>

namespace akkaradb::engine::sstable {
    // ==================== BlockCache ====================

    BlockCache::BlockCache(size_t max_blocks) : max_blocks_{max_blocks} {}

    std::shared_ptr<core::OwnedBuffer> BlockCache::get(uint64_t offset) {
        std::lock_guard lock{mutex_};

        auto it = cache_.find(offset);
        if (it == cache_.end()) { return nullptr; }

        ++it->second.access_count;
        return it->second.buffer; // shared_ptr copy (fast)
    }

    void BlockCache::put(uint64_t offset, std::shared_ptr<core::OwnedBuffer> buffer) {
        std::lock_guard lock{mutex_};

        // Evict LRU if at capacity
        if (cache_.size() >= max_blocks_) {
            auto lru_it = std::min_element(
                cache_.begin(),
                cache_.end(),
                [](const auto& a, const auto& b) { return a.second.access_count < b.second.access_count; }
            );
            cache_.erase(lru_it);
        }

        cache_[offset] = CachedBlock{std::move(buffer), 1};
    }

    void BlockCache::clear() {
        std::lock_guard lock{mutex_};
        cache_.clear();
    }

    // ==================== SSTableReader ====================

    std::unique_ptr<SSTableReader> SSTableReader::open(
        const std::filesystem::path& file_path,
        bool verify_footer_crc
    ) {
        // Open file
        std::ifstream file(file_path, std::ios::binary);
        if (!file) { throw std::runtime_error("SSTableReader: failed to open file"); }

        // Get file size
        file.seekg(0, std::ios::end);
        const uint64_t file_size = file.tellg();

        if (file_size < AKSSFooter::SIZE) { throw std::runtime_error("SSTableReader: file too small for footer"); }

        // Read footer
        file.seekg(file_size - AKSSFooter::SIZE);
        auto footer_buf = core::OwnedBuffer::allocate(AKSSFooter::SIZE);
        file.read(reinterpret_cast<char*>(footer_buf.view().data()), AKSSFooter::SIZE);

        auto footer = verify_footer_crc
                          ? AKSSFooter::read_from_file(footer_buf.view(), file_path, true)
                          : AKSSFooter::read_from(footer_buf.view());

        // Read IndexBlock
        const uint64_t index_end = footer.bloom_off > 0 ? footer.bloom_off : (file_size - AKSSFooter::SIZE);
        const uint64_t index_len = index_end - footer.index_off;

        if (index_len < 16 || index_len > (1ULL << 31)) { throw std::runtime_error("SSTableReader: invalid index length"); }

        file.seekg(footer.index_off);
        auto index_buf = core::OwnedBuffer::allocate(index_len);
        file.read(reinterpret_cast<char*>(index_buf.view().data()), index_len);

        auto index = IndexBlock::read_from(index_buf.view());

        // Read BloomFilter (optional)
        std::optional<BloomFilter> bloom;
        if (footer.bloom_off > 0) {
            const uint64_t bloom_len = (file_size - AKSSFooter::SIZE) - footer.bloom_off;

            if (bloom_len >= BloomFilter::HEADER_SIZE) {
                file.seekg(footer.bloom_off);
                auto bloom_buf = core::OwnedBuffer::allocate(bloom_len);
                file.read(reinterpret_cast<char*>(bloom_buf.view().data()), bloom_len);

                bloom = BloomFilter::read_from(bloom_buf.view());
            }
        }

        file.close();

        return std::unique_ptr<SSTableReader>(new SSTableReader(
            file_path, footer, std::move(index), std::move(bloom)
        ));
    }

    SSTableReader::SSTableReader(
        std::filesystem::path file_path,
        const AKSSFooter::Footer& footer,
        IndexBlock index,
        std::optional<BloomFilter> bloom
    ) : file_path_{std::move(file_path)}
        , footer_{footer}
        , index_{std::move(index)}
        , bloom_{std::move(bloom)}
        , unpacker_{format::akk::AkkBlockUnpacker::create()}
        , cache_{512} {
        file_.open(file_path_, std::ios::binary);
        if (!file_) { throw std::runtime_error("SSTableReader: failed to reopen file"); }

        file_.seekg(0, std::ios::end);
        file_size_ = static_cast<uint64_t>(file_.tellg());
    }

    SSTableReader::~SSTableReader() { close(); }

    std::optional<std::vector<uint8_t>> SSTableReader::get(std::span<const uint8_t> key) {
        // Fast Bloom reject
        if (bloom_.has_value() && !bloom_->might_contain(key)) { return std::nullopt; }

        // Locate candidate block via index
        const int64_t block_off = index_.lookup(key);
        if (block_off < 0) { return std::nullopt; }

        // Load block (zero-copy via shared_ptr)
        auto block_ptr = load_block(static_cast<uint64_t>(block_off));

        // Search in block
        auto record_opt = find_in_block(block_ptr->view(), key);
        if (!record_opt) { return std::nullopt; }

        // Copy value
        auto value_span = record_opt->value();
        return std::vector(value_span.begin(), value_span.end());
    }

    std::vector<SSTableReader::RangeRecord> SSTableReader::range(
        std::span<const uint8_t> start_key,
        const std::optional<std::span<const uint8_t>>& end_key
    ) {
        std::vector<RangeRecord> results;

        // Find starting block
        const int64_t start_block_off = index_.lookup(start_key);
        if (start_block_off < 0) { return results; }

        // Iterate through blocks
        const auto& entries = index_.entries();
        auto entry_it = std::ranges::find_if(entries, [start_block_off](const IndexBlock::Entry& e) {
                                                 return static_cast<int64_t>(e.block_offset) == start_block_off;
                                             }
        );

        while (entry_it != entries.end()) {
            auto block_ptr = load_block(entry_it->block_offset);
            auto cursor = unpacker_->cursor(block_ptr->view());

            while (cursor->has_next()) {
                auto record_opt = cursor->try_next();
                if (!record_opt) break;

                auto& record = *record_opt;
                auto rec_key = record.key();

                // Check start boundary
                if (std::ranges::lexicographical_compare(rec_key, start_key
                )) { continue; }

                // Check end boundary
                if (end_key.has_value()) {
                    auto end_span = *end_key;
                    if (!std::ranges::lexicographical_compare(rec_key, end_span
                    )) { return results; }
                }

                // Add to results
                auto rec_value = record.value();
                results.push_back(RangeRecord{
                    std::vector(rec_key.begin(), rec_key.end()),
                    std::vector(rec_value.begin(), rec_value.end()),
                    record.header()
                });
            }

            ++entry_it;
        }

        return results;
    }

    void SSTableReader::close() {
        if (file_.is_open()) { file_.close(); }
        cache_.clear();
    }

    std::shared_ptr<core::OwnedBuffer> SSTableReader::load_block(uint64_t offset) {
        // Check cache (zero-copy via shared_ptr)
        if (auto cached = cache_.get(offset)) { return cached; }

        // Read from file
        constexpr size_t BLOCK_SIZE = 32 * 1024;
        auto block_buf = core::OwnedBuffer::allocate(BLOCK_SIZE);

        file_.seekg(offset);
        file_.read(
            reinterpret_cast<char*>(const_cast<std::byte*>(block_buf.view().data())),
            BLOCK_SIZE
        );

        if (!file_) { throw std::runtime_error("SSTableReader: failed to read block"); }

        // Validate
        if (!unpacker_->validate(block_buf.view())) { throw std::runtime_error("SSTableReader: block validation failed"); }

        // Wrap in shared_ptr and cache
        auto block_ptr = std::make_shared<core::OwnedBuffer>(std::move(block_buf));
        cache_.put(offset, block_ptr);

        return block_ptr;
    }

    std::optional<core::RecordView> SSTableReader::find_in_block(
        core::BufferView block,
        std::span<const uint8_t> key
    ) {
        auto cursor = unpacker_->cursor(block);

        while (cursor->has_next()) {
            auto record_opt = cursor->try_next();
            if (!record_opt) break;

            auto& record = *record_opt;
            auto rec_key = record.key();

            if (std::equal(rec_key.begin(), rec_key.end(), key.begin(), key.end())) { return record; }
        }

        return std::nullopt;
    }
} // namespace akkaradb::engine::sstable
