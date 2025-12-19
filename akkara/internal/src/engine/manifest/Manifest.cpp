// internal/src/engine/manifest/Manifest.cpp
#include "engine/manifest/Manifest.hpp"
#include <fstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>
#include <stdexcept>
#include <unordered_set>
#include <nlohmann/json.hpp>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace akkaradb::engine::manifest {
    using json = nlohmann::json;

    namespace {
        /**
 * Platform-specific file handle for fsync.
 */
        class FileHandle {
        public:
#ifdef _WIN32
            using NativeHandle = HANDLE;
            inline static const NativeHandle INVALID = INVALID_HANDLE_VALUE;
#else
            using NativeHandle = int;
            static constexpr NativeHandle INVALID = -1;
#endif

            FileHandle() : handle_{INVALID} {}

            ~FileHandle() { close(); }

            FileHandle(const FileHandle&) = delete;
            FileHandle& operator=(const FileHandle&) = delete;

            FileHandle(FileHandle&& other) noexcept : handle_{other.handle_} { other.handle_ = INVALID; }

            FileHandle& operator=(FileHandle&& other) noexcept {
                if (this != &other) {
                    close();
                    handle_ = other.handle_;
                    other.handle_ = INVALID;
                }
                return *this;
            }

            [[nodiscard]] static FileHandle open(const std::filesystem::path& path) {
                FileHandle fh;

#ifdef _WIN32
                fh.handle_ = ::CreateFileW(
                    path.c_str(),
                    GENERIC_WRITE,
                    0,
                    nullptr,
                    OPEN_ALWAYS,
                    FILE_ATTRIBUTE_NORMAL,
                    nullptr
                );

                if (fh.handle_ == INVALID) { throw std::runtime_error("Failed to open manifest: " + path.string()); }

                // Seek to end
                ::SetFilePointer(fh.handle_, 0, nullptr, FILE_END);
#else
                fh.handle_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);

                if (fh.handle_ < 0) { throw std::runtime_error("Failed to open manifest: " + path.string()); }
#endif

                return fh;
            }

            void write(const uint8_t* data, size_t size) {
#ifdef _WIN32
                DWORD written = 0;
                if (!::WriteFile(handle_, data, static_cast<DWORD>(size), &written, nullptr)) { throw std::runtime_error("Manifest write failed"); }
#else
                ssize_t result = ::write(handle_, data, size);
                if (result < 0 || static_cast<size_t>(result) != size) { throw std::runtime_error("Manifest write failed"); }
#endif
            }

            void fsync_data() {
#ifdef _WIN32
                if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("Manifest fsync failed"); }
#elif defined(__APPLE__)
                if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("Manifest fsync failed"); }
#else
                if (::fdatasync(handle_) < 0) { throw std::runtime_error("Manifest fsync failed"); }
#endif
            }

            void fsync_full() {
#ifdef _WIN32
                if (!::FlushFileBuffers(handle_)) { throw std::runtime_error("Manifest fsync failed"); }
#elif defined(__APPLE__)
                if (::fcntl(handle_, F_FULLFSYNC) < 0) { throw std::runtime_error("Manifest fsync failed"); }
#else
                if (::fsync(handle_) < 0) { throw std::runtime_error("Manifest fsync failed"); }
#endif
            }

            void close() noexcept {
                if (handle_ != INVALID) {
#ifdef _WIN32
                    ::CloseHandle(handle_);
#else
                    ::close(handle_);
#endif
                    handle_ = INVALID;
                }
            }

        private:
            NativeHandle handle_;
        };

        /**
 * CRC32C computation.
 */
        uint32_t compute_crc32c(const uint8_t* data, size_t size) {
            uint32_t crc = 0xFFFFFFFF;
            for (size_t i = 0; i < size; ++i) {
                crc ^= data[i];
                for (int j = 0; j < 8; ++j) { crc = (crc >> 1) ^ (0x82F63B78 & -(crc & 1)); }
            }
            return ~crc;
        }

        /**
 * Current timestamp in milliseconds.
 */
        uint64_t now_millis() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
        }

        /**
 * Encode manifest record: [len:u32][json][crc32c:u32]
 */
        std::vector<uint8_t> encode_record(const std::string& json_str) {
            const auto len = static_cast<uint32_t>(json_str.size());
            std::vector<uint8_t> buffer(4 + len + 4);

            // Write length (LE)
            buffer[0] = len & 0xFF;
            buffer[1] = (len >> 8) & 0xFF;
            buffer[2] = (len >> 16) & 0xFF;
            buffer[3] = (len >> 24) & 0xFF;

            // Write JSON
            std::memcpy(buffer.data() + 4, json_str.data(), len);

            // Compute CRC over JSON only
            const uint32_t crc = compute_crc32c(reinterpret_cast<const uint8_t*>(json_str.data()), len);

            // Write CRC (LE)
            buffer[4 + len] = crc & 0xFF;
            buffer[4 + len + 1] = (crc >> 8) & 0xFF;
            buffer[4 + len + 2] = (crc >> 16) & 0xFF;
            buffer[4 + len + 3] = (crc >> 24) & 0xFF;

            return buffer;
        }
    } // anonymous namespace

    /**
 * Manifest::Impl - Private implementation.
 */
    class Manifest::Impl {
    public:
        Impl(std::filesystem::path path, bool fast_mode) : path_{std::move(path)}
                                                           , fast_mode_{fast_mode}
                                                           , running_{false}
                                                           , stripes_written_{0} {
            // Create parent directory
            if (path_.has_parent_path()) { std::filesystem::create_directories(path_.parent_path()); }

            // Replay existing manifest
            if (std::filesystem::exists(path_)) { replay_internal(); }

            // Open for append
            file_handle_ = FileHandle::open(path_);
        }

        ~Impl() { close(); }

        void start() {
            if (!fast_mode_ || running_) { return; }

            running_ = true;
            last_strong_sync_ = std::chrono::steady_clock::now();

            flusher_thread_ = std::thread([this]() { this->run_flusher(); });
        }

        void advance(uint64_t new_count) {
            if (new_count < stripes_written_) { throw std::invalid_argument("Manifest: stripe counter must be monotonic"); }

            stripes_written_ = new_count;

            json j = {
                {"type", "StripeCommit"},
                {"after", new_count},
                {"ts", now_millis()}
            };

            append(j.dump());
        }

        void sst_seal(
            int level,
            const std::string& file,
            uint64_t entries,
            const std::optional<std::string>& first_key_hex,
            const std::optional<std::string>& last_key_hex
        ) {
            json j = {
                {"type", "SSTSeal"},
                {"level", level},
                {"file", file},
                {"entries", entries},
                {"ts", now_millis()}
            };

            if (first_key_hex) { j["firstKeyHex"] = *first_key_hex; }
            if (last_key_hex) { j["lastKeyHex"] = *last_key_hex; }

            append(j.dump());

            // Update state
            std::lock_guard lock{mutex_};
            sst_seals_.push_back(SSTSealEvent{
                level, file, entries, first_key_hex, last_key_hex, j["ts"]
            });
            live_sst_.insert(file);
            deleted_sst_.erase(file);
        }

        void checkpoint(
            const std::optional<std::string>& name,
            const std::optional<uint64_t>& stripe,
            const std::optional<uint64_t>& last_seq
        ) {
            json j = {
                {"type", "Checkpoint"},
                {"ts", now_millis()}
            };

            if (name) j["name"] = *name;
            if (stripe) j["stripe"] = *stripe;
            if (last_seq) j["lastSeq"] = *last_seq;

            append(j.dump());

            // Update state
            std::lock_guard lock{mutex_};
            last_checkpoint_ = CheckpointEvent{name, stripe, last_seq, j["ts"]};
        }

        void compaction_start(int level, const std::vector<std::string>& inputs) {
            json j = {
                {"type", "CompactionStart"},
                {"level", level},
                {"inputs", inputs},
                {"ts", now_millis()}
            };

            append(j.dump());
        }

        void compaction_end(
            int level,
            const std::string& output,
            const std::vector<std::string>& inputs,
            uint64_t entries,
            const std::optional<std::string>& first_key_hex,
            const std::optional<std::string>& last_key_hex
        ) {
            json j = {
                {"type", "CompactionEnd"},
                {"level", level},
                {"output", output},
                {"inputs", inputs},
                {"entries", entries},
                {"ts", now_millis()}
            };

            if (first_key_hex) j["firstKeyHex"] = *first_key_hex;
            if (last_key_hex) j["lastKeyHex"] = *last_key_hex;

            append(j.dump());

            // Update state
            std::lock_guard lock{mutex_};
            live_sst_.insert(output);
            for (const auto& input : inputs) {
                live_sst_.erase(input);
                deleted_sst_.insert(input);
            }
        }

        void sst_delete(const std::string& file) {
            json j = {
                {"type", "SSTDelete"},
                {"file", file},
                {"ts", now_millis()}
            };

            append(j.dump());

            // Update state
            std::lock_guard lock{mutex_};
            live_sst_.erase(file);
            deleted_sst_.insert(file);
        }

        void truncate(const std::optional<std::string>& reason) {
            json j = {
                {"type", "Truncate"},
                {"ts", now_millis()}
            };

            if (reason) j["reason"] = *reason;

            append(j.dump());
        }

        void replay() { replay_internal(); }

        uint64_t stripes_written() const noexcept { return stripes_written_; }

        std::optional<CheckpointEvent> last_checkpoint() const noexcept {
            std::lock_guard lock{mutex_};
            return last_checkpoint_;
        }

        std::vector<std::string> live_sst() const {
            std::lock_guard lock{mutex_};
            return {live_sst_.begin(), live_sst_.end()};
        }

        std::vector<std::string> deleted_sst() const {
            std::lock_guard lock{mutex_};
            return {deleted_sst_.begin(), deleted_sst_.end()};
        }

        std::vector<SSTSealEvent> sst_seals() const {
            std::lock_guard lock{mutex_};
            return sst_seals_;
        }

        void close() {
            if (fast_mode_ && running_) {
                running_ = false;
                queue_cv_.notify_one();

                if (flusher_thread_.joinable()) { flusher_thread_.join(); }
            }

            file_handle_.close();
        }

    private:
        void append(const std::string& json_str) {
            if (fast_mode_) {
                // Enqueue for background flusher
                std::lock_guard lock{queue_mutex_};
                queue_.emplace(json_str); // push → emplace
                queue_cv_.notify_one();
            }
            else {
                // Synchronous write
                auto record = encode_record(json_str);
                file_handle_.write(record.data(), record.size());
                file_handle_.fsync_data();
            }
        }

        void run_flusher() {
            constexpr auto MAX_WAIT = std::chrono::microseconds(500);

            while (true) {
                constexpr size_t MAX_BATCH = 32;
                std::unique_lock lock{queue_mutex_};

                // Wait for data or shutdown
                queue_cv_.wait_for(lock, MAX_WAIT, [this]() { return !queue_.empty() || !running_; });

                if (!running_ && queue_.empty()) { break; }

                // Collect batch
                std::vector<std::string> batch;
                while (!queue_.empty() && batch.size() < MAX_BATCH) {
                    batch.push_back(std::move(queue_.front()));
                    queue_.pop();
                }

                lock.unlock();

                // Write batch
                for (const auto& json_str : batch) {
                    auto record = encode_record(json_str);
                    file_handle_.write(record.data(), record.size());
                }

                file_handle_.fsync_data();

                // Periodic full sync (metadata)
                auto now = std::chrono::steady_clock::now();
                if (now - last_strong_sync_ > std::chrono::seconds(5)) {
                    file_handle_.fsync_full();
                    last_strong_sync_ = now;
                }
            }
        }

        void replay_internal() {
            if (!std::filesystem::exists(path_)) { return; }

            std::ifstream file(path_, std::ios::binary);
            if (!file) { throw std::runtime_error("Failed to open manifest for replay"); }

            while (file) {
                // Read length
                uint32_t len;
                file.read(reinterpret_cast<char*>(&len), 4);
                if (!file || file.gcount() < 4) {
                    break; // EOF or truncated
                }

                // Read JSON
                std::string json_str(len, '\0');
                file.read(json_str.data(), len);
                if (!file || static_cast<size_t>(file.gcount()) < len) {
                    break; // Truncated
                }

                // Read CRC
                uint32_t stored_crc;
                file.read(reinterpret_cast<char*>(&stored_crc), 4);
                if (!file || file.gcount() < 4) {
                    break; // Truncated
                }

                // Verify CRC
                const uint32_t computed_crc = compute_crc32c(
                    reinterpret_cast<const uint8_t*>(json_str.data()),
                    json_str.size()
                );

                if (stored_crc != computed_crc) {
                    break; // Corrupted, stop replay
                }

                // Parse and apply event
                try { apply_event(json::parse(json_str)); }
                catch (...) {
                    // Malformed JSON, skip
                }
            }
        }

        void apply_event(const json& j) {
            const std::string type = j.value("type", "");

            if (type == "StripeCommit") {
                uint64_t after = j.value("after", 0ULL);
                if (after >= stripes_written_) { stripes_written_ = after; }
            }
            else if (type == "SSTSeal") {
                std::lock_guard lock{mutex_};
                SSTSealEvent event{
                    j.value("level", 0),
                    j.value("file", ""),
                    j.value("entries", 0ULL),
                    j.contains("firstKeyHex") ? std::optional(j["firstKeyHex"].get<std::string>()) : std::nullopt,
                    j.contains("lastKeyHex") ? std::optional(j["lastKeyHex"].get<std::string>()) : std::nullopt,
                    j.value("ts", 0ULL)
                };
                sst_seals_.push_back(event);
                live_sst_.insert(event.file);
                deleted_sst_.erase(event.file);
            }
            else if (type == "SSTDelete") {
                std::lock_guard lock{mutex_};
                std::string file = j.value("file", "");
                live_sst_.erase(file);
                deleted_sst_.insert(file);
            }
            else if (type == "CompactionEnd") {
                std::lock_guard lock{mutex_};
                std::string output = j.value("output", "");
                live_sst_.insert(output);

                if (j.contains("inputs")) {
                    for (const auto& input : j["inputs"]) {
                        std::string f = input.get<std::string>();
                        live_sst_.erase(f);
                        deleted_sst_.insert(f);
                    }
                }
            }
            else if (type == "Checkpoint") {
                std::lock_guard lock{mutex_};
                last_checkpoint_ = CheckpointEvent{
                    j.contains("name") ? std::optional(j["name"].get<std::string>()) : std::nullopt,
                    j.contains("stripe") ? std::optional(j["stripe"].get<uint64_t>()) : std::nullopt,
                    j.contains("lastSeq") ? std::optional(j["lastSeq"].get<uint64_t>()) : std::nullopt,
                    j.value("ts", 0ULL)
                };
            }
            // Other events (Truncate, FormatBump, etc.) are informational only
        }

        std::filesystem::path path_;
        bool fast_mode_;
        std::atomic<bool> running_;
        FileHandle file_handle_;

        // State
        std::atomic<uint64_t> stripes_written_;
        mutable std::mutex mutex_;
        std::vector<SSTSealEvent> sst_seals_;
        std::unordered_set<std::string> live_sst_;
        std::unordered_set<std::string> deleted_sst_;
        std::optional<CheckpointEvent> last_checkpoint_;

        // Background flusher
        std::thread flusher_thread_;
        std::mutex queue_mutex_;
        std::condition_variable queue_cv_;
        std::queue<std::string> queue_;
        std::chrono::steady_clock::time_point last_strong_sync_;
    };

    // ==================== Manifest Public API ====================

    std::unique_ptr<Manifest> Manifest::create(
        const std::filesystem::path& path,
        bool fast_mode
    ) { return std::unique_ptr<Manifest>(new Manifest(path, fast_mode)); }

    Manifest::Manifest(const std::filesystem::path& path, bool fast_mode) : impl_{std::make_unique<Impl>(path, fast_mode)} {}

    Manifest::~Manifest() = default;

    void Manifest::start() { impl_->start(); }

    void Manifest::advance(uint64_t new_count) { impl_->advance(new_count); }

    void Manifest::sst_seal(
        int level,
        const std::string& file,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    ) { impl_->sst_seal(level, file, entries, first_key_hex, last_key_hex); }

    void Manifest::checkpoint(
        const std::optional<std::string>& name,
        const std::optional<uint64_t>& stripe,
        const std::optional<uint64_t>& last_seq
    ) { impl_->checkpoint(name, stripe, last_seq); }

    void Manifest::compaction_start(int level, const std::vector<std::string>& inputs) { impl_->compaction_start(level, inputs); }

    void Manifest::compaction_end(
        int level,
        const std::string& output,
        const std::vector<std::string>& inputs,
        uint64_t entries,
        const std::optional<std::string>& first_key_hex,
        const std::optional<std::string>& last_key_hex
    ) { impl_->compaction_end(level, output, inputs, entries, first_key_hex, last_key_hex); }

    void Manifest::sst_delete(const std::string& file) { impl_->sst_delete(file); }

    void Manifest::truncate(const std::optional<std::string>& reason) { impl_->truncate(reason); }

    void Manifest::replay() { impl_->replay(); }

    uint64_t Manifest::stripes_written() const noexcept { return impl_->stripes_written(); }

    std::optional<Manifest::CheckpointEvent> Manifest::last_checkpoint() const noexcept { return impl_->last_checkpoint(); }

    std::vector<std::string> Manifest::live_sst() const { return impl_->live_sst(); }

    std::vector<std::string> Manifest::deleted_sst() const { return impl_->deleted_sst(); }

    std::vector<Manifest::SSTSealEvent> Manifest::sst_seals() const { return impl_->sst_seals(); }

    void Manifest::close() { impl_->close(); }
} // namespace akkaradb::engine::manifest