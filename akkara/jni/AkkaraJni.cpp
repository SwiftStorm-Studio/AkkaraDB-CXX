/**
 * AkkaraDB JNI Bridge
 *
 * Exposes AkkEngine put/get/remove/scan to the JVM layer.
 * Built only when AKKARADB_BUILD_JNI=ON.
 *
 * Handle encoding
 * ───────────────
 *  DB handle (jlong)     : raw AkkEngine* obtained via unique_ptr::release()
 *  Table handle (jlong)  : heap-allocated TableHandle* (engine ptr + 8-byte prefix + schema)
 *  Cursor handle (jlong) : heap-allocated ScanCursor* (streaming iterator + optional query)
 *
 * Key layout (matches C++ PackedTable)
 * ─────────────────────────────────────
 *  [8 B] FNV-1a-64(table_name) big-endian  ← namespace prefix
 *  [N B] BinPack(pk)                        ← as serialized by the JVM side
 */

#include <jni.h>
#include "engine/AkkEngine.hpp"
#include "engine/AstEvaluator.hpp"
#include "akkaradb/detail/Hash.hpp"

#include <array>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

using namespace akkaradb::engine;
using namespace akkaradb::detail;
using namespace akkaradb::binpack::detail; // read_u8 / read_u16 from WireHelpers.hpp

// ── Handle types ─────────────────────────────────────────────────────────────

struct TableHandle {
    AkkEngine*              engine; // borrowed; lifetime managed by DB handle
    std::array<uint8_t, 8>  prefix; // FNV-1a-64(name) as big-endian 8 bytes
    AkStructSchema          schema; // recursive field schema (parsed from JVM SchemaSerializer)
};

// ── Value-returning scan cursor (entity queries) ──────────────────────────────
//
// Abstract base so that the concrete iterator type (returned by AkkEngine::scan) can
// be stored on the heap across multiple JNI calls without exposing the template
// parameter to C-linkage JNI functions.

struct ScanCursor {
    virtual ~ScanCursor() = default;
    // Returns a span into an internal buffer valid until the next call.
    // Empty span signals exhaustion.
    virtual std::span<const uint8_t> next_match() = 0;
};

template<typename Iter>
struct ScanCursorImpl final : ScanCursor {
    // Key vectors are kept alive so the iterator never holds dangling references.
    std::vector<uint8_t>       start_key_;
    std::vector<uint8_t>       end_key_;
    Iter                       iter_;
    std::optional<ParsedQuery> query_;   // nullopt → full scan (no predicate filter)
    AkStructSchema             schema_;
    std::vector<uint8_t>       out_buf_; // reused across next_match calls; no per-entity alloc

    ScanCursorImpl(std::vector<uint8_t> sk, std::vector<uint8_t> ek,
                   Iter it, std::optional<ParsedQuery> q, AkStructSchema s)
        : start_key_(std::move(sk)), end_key_(std::move(ek)),
          iter_(std::move(it)), query_(std::move(q)), schema_(std::move(s)) {}

    std::span<const uint8_t> next_match() override {
        while (iter_.has_next()) {
            auto pair = iter_.next();
            if (!pair) continue;
            if (!query_ || query_->matches(pair->second, schema_)) {
                // assign reuses capacity when entity size is stable (fixed schema)
                out_buf_.assign(pair->second.begin(), pair->second.end());
                return std::span<const uint8_t>(out_buf_);
            }
        }
        return {};
    }
};

template<typename Iter>
static std::unique_ptr<ScanCursor> make_scan_cursor(
        std::vector<uint8_t> start_key, std::vector<uint8_t> end_key,
        Iter iter, std::optional<ParsedQuery> query, AkStructSchema schema)
{
    return std::make_unique<ScanCursorImpl<Iter>>(
            std::move(start_key), std::move(end_key),
            std::move(iter), std::move(query), std::move(schema));
}

// ── Key-returning scan cursor (secondary index scans) ─────────────────────────
//
// Used by nativeRawOpenCursor / nativeRawCursorNext: returns the raw key bytes
// of each entry in the scanned range, so the JVM can extract embedded pk bytes.

struct KeyScanCursor {
    virtual ~KeyScanCursor() = default;
    // Returns a span into an internal buffer valid until the next call.
    // Empty span signals exhaustion.
    virtual std::span<const uint8_t> next_key() = 0;
};

template<typename Iter>
struct KeyScanCursorImpl final : KeyScanCursor {
    std::vector<uint8_t> start_key_;
    std::vector<uint8_t> end_key_;
    Iter                 iter_;
    std::vector<uint8_t> out_buf_; // reused across next_key calls

    KeyScanCursorImpl(std::vector<uint8_t> sk, std::vector<uint8_t> ek, Iter it)
        : start_key_(std::move(sk)), end_key_(std::move(ek)), iter_(std::move(it)) {}

    std::span<const uint8_t> next_key() override {
        while (iter_.has_next()) {
            auto pair = iter_.next();
            if (!pair) continue;
            out_buf_.assign(pair->first.begin(), pair->first.end());
            return std::span<const uint8_t>(out_buf_);
        }
        return {};
    }
};

template<typename Iter>
static std::unique_ptr<KeyScanCursor> make_key_scan_cursor(
        std::vector<uint8_t> start_key, std::vector<uint8_t> end_key, Iter iter)
{
    return std::make_unique<KeyScanCursorImpl<Iter>>(
            std::move(start_key), std::move(end_key), std::move(iter));
}

// ── JNI helpers ──────────────────────────────────────────────────────────────

// Thread-local scratch buffers — reused across calls to avoid per-operation
// heap allocation on the hot CRUD and cursor paths.
thread_local std::vector<uint8_t> tls_key_buf;   // composite [prefix|pk] key
thread_local std::vector<uint8_t> tls_val_buf;   // entity / raw value bytes
thread_local std::vector<uint8_t> tls_query_buf; // serialized query AST bytes

// Read a jbyteArray into a caller-supplied buffer; returns a span into it.
// Use for transient data that does not outlive the current JNI call.
static std::span<const uint8_t> jbytes_to_span(JNIEnv* env, jbyteArray arr,
                                                std::vector<uint8_t>& buf) {
    if (!arr) { buf.clear(); return {}; }
    const jsize len = env->GetArrayLength(arr);
    buf.resize(static_cast<size_t>(len));
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(buf.data()));
    return std::span<const uint8_t>(buf);
}

// Read a jbyteArray into a newly-allocated vector.
// Use only when the data must outlive the current JNI call (e.g., cursor key ranges).
static std::vector<uint8_t> jbytes_to_vec(JNIEnv* env, jbyteArray arr) {
    if (!arr) return {};
    const jsize len = env->GetArrayLength(arr);
    std::vector<uint8_t> vec(static_cast<size_t>(len));
    env->GetByteArrayRegion(arr, 0, len, reinterpret_cast<jbyte*>(vec.data()));
    return vec;
}

// Write a span to a new jbyteArray (one allocation + one copy).
static jbyteArray span_to_jbytes(JNIEnv* env, std::span<const uint8_t> data) {
    jbyteArray arr = env->NewByteArray(static_cast<jsize>(data.size()));
    if (!arr) return nullptr;
    env->SetByteArrayRegion(arr, 0, static_cast<jsize>(data.size()),
                            reinterpret_cast<const jbyte*>(data.data()));
    return arr;
}

static jbyteArray vec_to_jbytes(JNIEnv* env, const std::vector<uint8_t>& vec) {
    return span_to_jbytes(env, std::span<const uint8_t>(vec));
}

static std::string jstring_to_str(JNIEnv* env, jstring js) {
    if (!js) return {};
    const char* chars = env->GetStringUTFChars(js, nullptr);
    std::string s(chars);
    env->ReleaseStringUTFChars(js, chars);
    return s;
}

static void throw_jni(JNIEnv* env, const char* cls, const char* msg) {
    jclass ex = env->FindClass(cls);
    if (ex) env->ThrowNew(ex, msg);
}

// Build prefixed key [8 B prefix | pk] into tls_key_buf.
// Returns a span valid until the next build_key call on this thread.
static std::span<const uint8_t> build_key(JNIEnv* env,
                                           const std::array<uint8_t, 8>& prefix,
                                           jbyteArray jpk) {
    const jsize pk_len = env->GetArrayLength(jpk);
    tls_key_buf.resize(8 + static_cast<size_t>(pk_len));
    std::memcpy(tls_key_buf.data(), prefix.data(), 8);
    env->GetByteArrayRegion(jpk, 0, pk_len,
                            reinterpret_cast<jbyte*>(tls_key_buf.data() + 8));
    return std::span<const uint8_t>(tls_key_buf);
}

// ── ParsedQuery cache ─────────────────────────────────────────────────────────
//
// Queries are compile-time constants (IR plugin generates fixed AkkQuery objects),
// so the serialized bytes are identical on every call site invocation.
// A direct-mapped thread-local cache with CAP=16 slots gives ~100% hit rate
// in steady state with zero contention and zero heap allocation on a cache hit.

static uint64_t fnv1a_bytes(std::span<const uint8_t> data) noexcept {
    uint64_t h = 14695981039346656037ULL;
    for (uint8_t b : data) { h ^= b; h *= 1099511628211ULL; }
    return h;
}

struct QueryCache {
    static constexpr size_t CAP = 16;
    struct Slot { uint64_t hash = 0; ParsedQuery query; bool valid = false; };
    std::array<Slot, CAP> slots{};

    [[nodiscard]] const ParsedQuery* get(uint64_t h) const noexcept {
        const auto& s = slots[h % CAP];
        return (s.valid && s.hash == h) ? &s.query : nullptr;
    }
    void put(uint64_t h, ParsedQuery q) {
        auto& s = slots[h % CAP];
        s.hash = h; s.query = std::move(q); s.valid = true;
    }
};
thread_local QueryCache tls_query_cache;

// Compute namespace prefix from table name
static std::array<uint8_t, 8> make_prefix(const std::string& name) {
    const uint64_t h = fnv1a_64(name);
    std::array<uint8_t, 8> prefix{};
    write_be64(h, prefix.data());
    return prefix;
}

// Scan helper: [prefix, prefix+1) range as start/end key vectors
static std::pair<std::vector<uint8_t>, std::vector<uint8_t>>
prefix_range(const std::array<uint8_t, 8>& prefix) {
    std::vector<uint8_t> start(prefix.begin(), prefix.end());
    std::vector<uint8_t> end  (prefix.begin(), prefix.end());
    increment_be64(end.data()); // prefix + 1 (exclusive upper bound)
    return { std::move(start), std::move(end) };
}

// ── AkkaraDB ─────────────────────────────────────────────────────────────────

extern "C" {

JNIEXPORT jlong JNICALL
Java_dev_swiftstorm_akkaradb_engine_AkkaraDB_nativeOpen(
        JNIEnv* env, jclass /*cls*/, jstring jpath)
{
    try {
        AkkEngineOptions opts;
        opts.data_dir = jstring_to_str(env, jpath);
        auto engine = AkkEngine::open(std::move(opts));
        return reinterpret_cast<jlong>(engine.release());
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return 0;
    }
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_AkkaraDB_jniClose(
        JNIEnv* env, jobject /*self*/, jlong handle)
{
    try {
        delete reinterpret_cast<AkkEngine*>(handle);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

JNIEXPORT jlong JNICALL
Java_dev_swiftstorm_akkaradb_engine_AkkaraDB_jniOpenTable(
        JNIEnv* env, jobject /*self*/, jlong handle, jstring jname, jbyteArray jschema)
{
    try {
        auto* engine      = reinterpret_cast<AkkEngine*>(handle);
        auto  name        = jstring_to_str(env, jname);
        auto  schema_vec  = jbytes_to_vec(env, jschema);
        auto  schema_span = std::span<const uint8_t>(schema_vec);

        // Parse recursive schema (produced by SchemaSerializer.kt)
        auto schema = parse_struct_schema(schema_span);
        auto* th = new TableHandle{ engine, make_prefix(name), std::move(schema) };
        return reinterpret_cast<jlong>(th);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return 0;
    }
}

// ── JniPackedTable ───────────────────────────────────────────────────────────

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeCloseTable(
        JNIEnv* /*env*/, jobject /*self*/, jlong handle)
{
    delete reinterpret_cast<TableHandle*>(handle);
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativePut(
        JNIEnv* env, jobject /*self*/, jlong handle,
        jbyteArray jPkBytes, jbyteArray jEntityBytes)
{
    try {
        auto* th    = reinterpret_cast<TableHandle*>(handle);
        auto  key   = build_key(env, th->prefix, jPkBytes);
        auto  value = jbytes_to_span(env, jEntityBytes, tls_val_buf);
        th->engine->put(key, value);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

JNIEXPORT jbyteArray JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeGet(
        JNIEnv* env, jobject /*self*/, jlong handle, jbyteArray jPkBytes)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = build_key(env, th->prefix, jPkBytes);
        auto  opt = th->engine->get(key);
        return opt ? vec_to_jbytes(env, *opt) : nullptr;
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRemove(
        JNIEnv* env, jobject /*self*/, jlong handle, jbyteArray jPkBytes)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = build_key(env, th->prefix, jPkBytes);
        th->engine->remove(key);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

JNIEXPORT jboolean JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeExistsPk(
        JNIEnv* env, jobject /*self*/, jlong handle, jbyteArray jPkBytes)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = build_key(env, th->prefix, jPkBytes);
        return th->engine->get(key).has_value() ? JNI_TRUE : JNI_FALSE;
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return JNI_FALSE;
    }
}

// ── Cursor-based streaming (nativeQuery / nativeScanAll replacement) ──────────
//
// nativeOpenCursor opens a scan over the table's prefix range.
//   jqueryBytes == null  → full scan, every entity is yielded
//   jqueryBytes != null  → filtered scan; ParsedQuery applied per entity in C++
//
// nativeCursorNext returns the next matching entity's raw value bytes, or null
//   when the scan is exhausted.
//
// nativeCloseCursor frees the cursor; always called from Kotlin's finally block.

JNIEXPORT jlong JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeOpenCursor(
        JNIEnv* env, jobject /*self*/, jlong handle, jbyteArray jqueryBytes)
{
    try {
        auto* th = reinterpret_cast<TableHandle*>(handle);
        auto [start_key, end_key] = prefix_range(th->prefix);

        std::optional<ParsedQuery> query;
        if (jqueryBytes) {
            auto qspan = jbytes_to_span(env, jqueryBytes, tls_query_buf);
            const uint64_t qhash = fnv1a_bytes(qspan);
            if (const auto* cached = tls_query_cache.get(qhash)) {
                query = *cached;
            } else {
                query = ParsedQuery::from_bytes(qspan);
                tls_query_cache.put(qhash, *query);
            }
        }

        auto iter   = th->engine->scan(start_key, end_key);
        auto cursor = make_scan_cursor(
                std::move(start_key), std::move(end_key),
                std::move(iter), std::move(query), th->schema);
        return reinterpret_cast<jlong>(cursor.release());
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return 0;
    }
}

JNIEXPORT jbyteArray JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeCursorNext(
        JNIEnv* env, jobject /*self*/, jlong cursorHandle)
{
    try {
        auto* cursor = reinterpret_cast<ScanCursor*>(cursorHandle);
        auto  result = cursor->next_match();
        return result.empty() ? nullptr : span_to_jbytes(env, result);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeCloseCursor(
        JNIEnv* /*env*/, jobject /*self*/, jlong cursorHandle)
{
    delete reinterpret_cast<ScanCursor*>(cursorHandle);
}

// ── nativeCount ───────────────────────────────────────────────────────────────

JNIEXPORT jlong JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeCount(
        JNIEnv* env, jobject /*self*/, jlong handle)
{
    try {
        auto* th                  = reinterpret_cast<TableHandle*>(handle);
        auto [start_key, end_key] = prefix_range(th->prefix);
        jlong count = 0;
        auto  it    = th->engine->scan(start_key, end_key);
        while (it.has_next()) { it.next(); ++count; }
        return count;
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return -1;
    }
}

// ── Raw-key operations (JVM-managed secondary indexes) ────────────────────────
//
// These bypass the table prefix entirely — the caller (Kotlin AkkaraIndex) supplies
// fully-formed keys.  Used to maintain secondary index entries in the same engine.

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRawPut(
        JNIEnv* env, jobject /*self*/, jlong handle,
        jbyteArray jkey, jbyteArray jval)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = jbytes_to_span(env, jkey, tls_key_buf);
        auto  val = jbytes_to_span(env, jval, tls_val_buf);
        th->engine->put(key, val);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRawRemove(
        JNIEnv* env, jobject /*self*/, jlong handle, jbyteArray jkey)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = jbytes_to_span(env, jkey, tls_key_buf);
        th->engine->remove(key);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

JNIEXPORT jlong JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRawOpenCursor(
        JNIEnv* env, jobject /*self*/, jlong handle,
        jbyteArray jstart, jbyteArray jend)
{
    try {
        auto* th        = reinterpret_cast<TableHandle*>(handle);
        auto  start_key = jbytes_to_vec(env, jstart);
        auto  end_key   = jbytes_to_vec(env, jend);
        auto  iter      = th->engine->scan(start_key, end_key);
        auto  cursor    = make_key_scan_cursor(
                std::move(start_key), std::move(end_key), std::move(iter));
        return reinterpret_cast<jlong>(cursor.release());
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return 0;
    }
}

JNIEXPORT jbyteArray JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRawCursorNext(
        JNIEnv* env, jobject /*self*/, jlong cursorHandle)
{
    try {
        auto* cursor = reinterpret_cast<KeyScanCursor*>(cursorHandle);
        auto  result = cursor->next_key();
        return result.empty() ? nullptr : span_to_jbytes(env, result);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRawCloseCursor(
        JNIEnv* /*env*/, jobject /*self*/, jlong cursorHandle)
{
    delete reinterpret_cast<KeyScanCursor*>(cursorHandle);
}

// ── VersionLog: table-scoped operations ──────────────────────────────────────

JNIEXPORT jbyteArray JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeGetAt(
        JNIEnv* env, jobject /*self*/, jlong handle,
        jbyteArray jPkBytes, jlong seq)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = build_key(env, th->prefix, jPkBytes);
        auto  opt = th->engine->get_at(key, static_cast<uint64_t>(seq));
        return opt ? vec_to_jbytes(env, *opt) : nullptr;
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return nullptr;
    }
}

JNIEXPORT jbyteArray JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeHistory(
        JNIEnv* env, jobject /*self*/, jlong handle, jbyteArray jPkBytes)
{
    try {
        auto* th      = reinterpret_cast<TableHandle*>(handle);
        auto  key     = build_key(env, th->prefix, jPkBytes);
        auto  entries = th->engine->history(key);

        // Wire format:
        //   [4B] count
        //   for each entry:
        //     [8B] seq  [8B] source_node_id  [8B] timestamp_ns  [1B] flags
        //     [4B] value_len  [value_len B] value
        std::vector<uint8_t> buf;
        buf.reserve(4 + entries.size() * 29); // avg estimate
        write_u32(static_cast<uint32_t>(entries.size()), buf);
        for (auto& e : entries) {
            write_u64(e.seq,            buf);
            write_u64(e.source_node_id, buf);
            write_u64(e.timestamp_ns,   buf);
            write_u8 (e.flags,          buf);
            write_u32(static_cast<uint32_t>(e.value.size()), buf);
            buf.insert(buf.end(), e.value.begin(), e.value.end());
        }
        return vec_to_jbytes(env, buf);
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_JniPackedTable_nativeRollbackKey(
        JNIEnv* env, jobject /*self*/, jlong handle,
        jbyteArray jPkBytes, jlong targetSeq)
{
    try {
        auto* th  = reinterpret_cast<TableHandle*>(handle);
        auto  key = build_key(env, th->prefix, jPkBytes);
        th->engine->rollback_key(key, static_cast<uint64_t>(targetSeq));
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

// ── VersionLog: DB-scoped rollback ────────────────────────────────────────────

JNIEXPORT void JNICALL
Java_dev_swiftstorm_akkaradb_engine_AkkaraDB_jniRollbackTo(
        JNIEnv* env, jobject /*self*/, jlong handle, jlong targetSeq)
{
    try {
        reinterpret_cast<AkkEngine*>(handle)
            ->rollback_to(static_cast<uint64_t>(targetSeq));
    } catch (const std::exception& e) {
        throw_jni(env, "java/lang/RuntimeException", e.what());
    }
}

} // extern "C"
