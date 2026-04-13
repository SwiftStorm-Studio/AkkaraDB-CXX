// akkara/internal/include/engine/AstEvaluator.hpp
//
// Header-only AstEvaluator: parses the AstSerializer wire format produced by
// the JVM and evaluates it against a BinPack-encoded entity.
//
// Supports:
//   • Primitive fields (bool / int8-64 / float / double / string) + nullable
//   • List<T> fields  — IN / NOT_IN queries on elements
//   • Map<K,V> fields — MAP_GET (index access), IN (key membership)
//   • Nested data class fields — transparently flattened to dotted paths
//     e.g.  Address { city: String }  stored as  "address.city"
#pragma once

#include "akkaradb/binpack/detail/WireHelpers.hpp"

#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace akkaradb::engine {

using namespace akkaradb::binpack::detail;

// ── Schema kind constants (match SchemaSerializer.kt SK_* / AkSchema::Kind) ──
static constexpr uint8_t SK_BOOL     = 0x01;
static constexpr uint8_t SK_INT8     = 0x02;
static constexpr uint8_t SK_INT16    = 0x03;
static constexpr uint8_t SK_INT32    = 0x04;
static constexpr uint8_t SK_INT64    = 0x05;
static constexpr uint8_t SK_FLOAT    = 0x06;
static constexpr uint8_t SK_DOUBLE   = 0x07;
static constexpr uint8_t SK_STRING   = 0x08;
static constexpr uint8_t SK_LIST     = 0x0A;
static constexpr uint8_t SK_MAP      = 0x0B;
static constexpr uint8_t SK_STRUCT   = 0x0C;
static constexpr uint8_t SK_NULLABLE = 0x0D;

// ── Literal type tags (AstSerializer wire format) ─────────────────────────────
static constexpr uint8_t LIT_BOOL   = 0x01;
static constexpr uint8_t LIT_INT8   = 0x02;
static constexpr uint8_t LIT_INT16  = 0x03;
static constexpr uint8_t LIT_INT32  = 0x04;
static constexpr uint8_t LIT_INT64  = 0x05;
static constexpr uint8_t LIT_FLOAT  = 0x06;
static constexpr uint8_t LIT_DOUBLE = 0x07;
static constexpr uint8_t LIT_STRING = 0x08;
static constexpr uint8_t LIT_NULL   = 0x09;

// ── AST node tags ─────────────────────────────────────────────────────────────
static constexpr uint8_t NODE_BIN     = 0x01;
static constexpr uint8_t NODE_UN      = 0x02;
static constexpr uint8_t NODE_LIT     = 0x03;
static constexpr uint8_t NODE_COL     = 0x04;
static constexpr uint8_t NODE_CAPTURE = 0x05;

// ── Op wire values: AkkOp.ordinal + 1 ────────────────────────────────────────
enum class AstOp : uint8_t {
    GT=1, GE, LT, LE, EQ, NEQ,
    AND, OR, NOT,
    IN, NOT_IN,
    IS_NULL, IS_NOT_NULL,
    MAP_GET = 14    // AkkOp.MAP_GET.ordinal(13) + 1
};

// ── Recursive schema types ────────────────────────────────────────────────────
//
// AkSchema and AkStructSchema are mutually recursive through unique_ptr.
// Both are fully defined in this header so every translation unit sees the
// complete types before any destructor is instantiated.

struct AkStructSchema;   // forward declaration

struct AkSchema {
    enum class Kind : uint8_t {
        BOOL     = SK_BOOL,
        INT8     = SK_INT8,
        INT16    = SK_INT16,
        INT32    = SK_INT32,
        INT64    = SK_INT64,
        FLOAT    = SK_FLOAT,
        DOUBLE   = SK_DOUBLE,
        STRING   = SK_STRING,
        LIST     = SK_LIST,
        MAP      = SK_MAP,
        STRUCT   = SK_STRUCT,
        NULLABLE = SK_NULLABLE
    };

    Kind kind{ Kind::BOOL };
    // NULLABLE → inner  = wrapped non-nullable type
    // LIST     → inner  = element type
    // MAP      → inner  = key type,  inner2 = value type
    // STRUCT   → struct_schema = nested field list
    std::unique_ptr<AkSchema>       inner;
    std::unique_ptr<AkSchema>       inner2;
    std::unique_ptr<AkStructSchema> struct_schema;
};

struct AkStructSchema {
    struct Field {
        std::string               name;
        std::unique_ptr<AkSchema> type;
    };
    std::vector<Field> fields;
};

// ── Schema wire-format parser ─────────────────────────────────────────────────

[[nodiscard]] inline std::unique_ptr<AkSchema>
parse_schema_type(std::span<const uint8_t>& data);

[[nodiscard]] inline AkStructSchema
parse_struct_schema(std::span<const uint8_t>& data)
{
    AkStructSchema schema;
    const uint8_t num = read_u8(data);
    schema.fields.reserve(num);
    for (uint8_t i = 0; i < num; ++i) {
        AkStructSchema::Field field;
        const uint16_t name_len = read_u16(data);
        if (data.size() < name_len)
            throw std::runtime_error("AstEvaluator: field name underflow in schema");
        field.name = std::string(reinterpret_cast<const char*>(data.data()), name_len);
        data = data.subspan(name_len);
        field.type = parse_schema_type(data);
        schema.fields.push_back(std::move(field));
    }
    return schema;
}

[[nodiscard]] inline std::unique_ptr<AkSchema>
parse_schema_type(std::span<const uint8_t>& data)
{
    auto s  = std::make_unique<AkSchema>();
    s->kind = static_cast<AkSchema::Kind>(read_u8(data));
    switch (s->kind) {
        case AkSchema::Kind::NULLABLE:
            s->inner = parse_schema_type(data);
            break;
        case AkSchema::Kind::LIST:
            s->inner = parse_schema_type(data);
            break;
        case AkSchema::Kind::MAP:
            s->inner  = parse_schema_type(data);   // key type
            s->inner2 = parse_schema_type(data);   // value type
            break;
        case AkSchema::Kind::STRUCT:
            s->struct_schema = std::make_unique<AkStructSchema>(parse_struct_schema(data));
            break;
        default:
            break;  // primitives carry no extra schema bytes
    }
    return s;
}

// ── Dynamically-typed field value ─────────────────────────────────────────────

struct FieldValue {
    // MapType uses a vector-of-pairs so keys may be any FieldValue type.
    using ListType = std::vector<FieldValue>;
    using MapType  = std::vector<std::pair<FieldValue, FieldValue>>;
    using V = std::variant<
        std::nullptr_t, bool, int64_t, double, std::string,
        ListType, MapType
    >;
    V v{ std::nullptr_t{} };

    bool is_null()   const noexcept { return std::holds_alternative<std::nullptr_t>(v); }
    bool is_bool()   const noexcept { return std::holds_alternative<bool>(v); }
    bool is_int()    const noexcept { return std::holds_alternative<int64_t>(v); }
    bool is_double() const noexcept { return std::holds_alternative<double>(v); }
    bool is_string() const noexcept { return std::holds_alternative<std::string>(v); }
    bool is_list()   const noexcept { return std::holds_alternative<ListType>(v); }
    bool is_map()    const noexcept { return std::holds_alternative<MapType>(v); }

    bool               as_bool()   const { return std::get<bool>(v); }
    int64_t            as_int()    const { return std::get<int64_t>(v); }
    double             as_double() const { return std::get<double>(v); }
    const std::string& as_str()    const { return std::get<std::string>(v); }
    const ListType&    as_list()   const { return std::get<ListType>(v); }
    const MapType&     as_map()    const { return std::get<MapType>(v); }

    bool operator==(const FieldValue& o) const noexcept { return v == o.v; }
};

// ── Literal value reader (AstSerializer captures / LIT nodes) ─────────────────

[[nodiscard]] inline FieldValue
read_lit_value(uint8_t tag, std::span<const uint8_t>& data)
{
    switch (tag) {
        case LIT_NULL:   return {};
        case LIT_BOOL:   return FieldValue{ read_u8(data) != 0 };
        case LIT_INT8:   return FieldValue{ static_cast<int64_t>(static_cast<int8_t>(read_u8(data))) };
        case LIT_INT16: { uint16_t r = read_u16(data); int16_t v; std::memcpy(&v,&r,2); return FieldValue{(int64_t)v}; }
        case LIT_INT32: { uint32_t r = read_u32(data); int32_t v; std::memcpy(&v,&r,4); return FieldValue{(int64_t)v}; }
        case LIT_INT64: { uint64_t r = read_u64(data); int64_t v; std::memcpy(&v,&r,8); return FieldValue{v}; }
        case LIT_FLOAT:  { uint32_t r = read_u32(data); float  f; std::memcpy(&f,&r,4); return FieldValue{(double)f}; }
        case LIT_DOUBLE: { uint64_t r = read_u64(data); double d; std::memcpy(&d,&r,8); return FieldValue{d}; }
        case LIT_STRING: {
            const uint32_t len = read_u32(data);
            if (data.size() < len) throw std::runtime_error("AstEvaluator: string literal underflow");
            std::string s(reinterpret_cast<const char*>(data.data()), len);
            data = data.subspan(len);
            return FieldValue{ std::move(s) };
        }
        default:
            throw std::runtime_error("AstEvaluator: unknown literal tag " + std::to_string(tag));
    }
}

// ── Schema-driven entity field reader / skipper ───────────────────────────────

inline void skip_schema_value(std::span<const uint8_t>& data, const AkSchema& type);

[[nodiscard]] inline FieldValue
read_schema_value(std::span<const uint8_t>& data, const AkSchema& type)
{
    switch (type.kind) {
        case AkSchema::Kind::BOOL:   return FieldValue{ read_u8(data) != 0 };
        case AkSchema::Kind::INT8:   return FieldValue{ static_cast<int64_t>(static_cast<int8_t>(read_u8(data))) };
        case AkSchema::Kind::INT16: { uint16_t r=read_u16(data); int16_t v; std::memcpy(&v,&r,2); return FieldValue{(int64_t)v}; }
        case AkSchema::Kind::INT32: { uint32_t r=read_u32(data); int32_t v; std::memcpy(&v,&r,4); return FieldValue{(int64_t)v}; }
        case AkSchema::Kind::INT64: { uint64_t r=read_u64(data); int64_t v; std::memcpy(&v,&r,8); return FieldValue{v}; }
        case AkSchema::Kind::FLOAT:  { uint32_t r=read_u32(data); float  f; std::memcpy(&f,&r,4); return FieldValue{(double)f}; }
        case AkSchema::Kind::DOUBLE: { uint64_t r=read_u64(data); double d; std::memcpy(&d,&r,8); return FieldValue{d}; }
        case AkSchema::Kind::STRING: {
            const uint32_t len = read_u32(data);
            if (data.size() < len) throw std::runtime_error("AstEvaluator: string field underflow");
            std::string s(reinterpret_cast<const char*>(data.data()), len);
            data = data.subspan(len);
            return FieldValue{ std::move(s) };
        }
        case AkSchema::Kind::NULLABLE: {
            if (read_u8(data) == 0) return {};
            return read_schema_value(data, *type.inner);
        }
        case AkSchema::Kind::LIST: {
            const uint32_t count = read_u32(data);
            // List<NestedStruct>: not queryable — skip all elements, return null
            if (type.inner->kind == AkSchema::Kind::STRUCT) {
                for (uint32_t i = 0; i < count; ++i) skip_schema_value(data, *type.inner);
                return {};
            }
            FieldValue::ListType elems;
            elems.reserve(count);
            for (uint32_t i = 0; i < count; ++i)
                elems.push_back(read_schema_value(data, *type.inner));
            return FieldValue{ std::move(elems) };
        }
        case AkSchema::Kind::MAP: {
            const uint32_t count = read_u32(data);
            FieldValue::MapType entries;
            entries.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                auto k = read_schema_value(data, *type.inner);
                if (type.inner2->kind == AkSchema::Kind::STRUCT) {
                    skip_schema_value(data, *type.inner2);
                    entries.emplace_back(std::move(k), FieldValue{});
                } else {
                    entries.emplace_back(std::move(k), read_schema_value(data, *type.inner2));
                }
            }
            return FieldValue{ std::move(entries) };
        }
        case AkSchema::Kind::STRUCT:
            throw std::runtime_error("AstEvaluator: unexpected STRUCT in read_schema_value — use parse_entity_into");
    }
    throw std::runtime_error("AstEvaluator: unknown schema kind");
}

inline void skip_schema_value(std::span<const uint8_t>& data, const AkSchema& type)
{
    switch (type.kind) {
        case AkSchema::Kind::BOOL:
        case AkSchema::Kind::INT8:   data = data.subspan(1); return;
        case AkSchema::Kind::INT16:  data = data.subspan(2); return;
        case AkSchema::Kind::INT32:
        case AkSchema::Kind::FLOAT:  data = data.subspan(4); return;
        case AkSchema::Kind::INT64:
        case AkSchema::Kind::DOUBLE: data = data.subspan(8); return;
        case AkSchema::Kind::STRING: { data = data.subspan(read_u32(data)); return; }
        case AkSchema::Kind::NULLABLE: {
            if (read_u8(data) != 0) skip_schema_value(data, *type.inner);
            return;
        }
        case AkSchema::Kind::LIST: {
            const uint32_t n = read_u32(data);
            for (uint32_t i = 0; i < n; ++i) skip_schema_value(data, *type.inner);
            return;
        }
        case AkSchema::Kind::MAP: {
            const uint32_t n = read_u32(data);
            for (uint32_t i = 0; i < n; ++i) {
                skip_schema_value(data, *type.inner);
                skip_schema_value(data, *type.inner2);
            }
            return;
        }
        case AkSchema::Kind::STRUCT: {
            for (const auto& f : type.struct_schema->fields)
                skip_schema_value(data, *f.type);
            return;
        }
    }
}

// ── Parse entity bytes → flat field map, dotted paths for nested structs ──────

inline void parse_entity_into(
    std::span<const uint8_t>&                    data,
    const AkStructSchema&                        schema,
    const std::string&                           prefix,
    std::unordered_map<std::string, FieldValue>& out)
{
    for (const auto& field : schema.fields) {
        const std::string full = prefix.empty() ? field.name : prefix + '.' + field.name;
        if (field.type->kind == AkSchema::Kind::STRUCT) {
            // Transparently flatten: address.city, address.zip, …
            parse_entity_into(data, *field.type->struct_schema, full, out);
        } else {
            out[full] = read_schema_value(data, *field.type);
        }
    }
}

[[nodiscard]] inline std::unordered_map<std::string, FieldValue>
parse_entity(std::span<const uint8_t> data, const AkStructSchema& schema)
{
    std::unordered_map<std::string, FieldValue> fields;
    fields.reserve(schema.fields.size());
    parse_entity_into(data, schema, "", fields);
    return fields;
}

// ── Selective entity parser — reads only the columns referenced by the AST ────
//
// Iterates the schema fields in wire order.  For each field:
//   - If the full dotted name is in `required`: read_schema_value → store in out.
//   - Otherwise: skip_schema_value (advances the span without allocating).
//
// For nested STRUCT fields: recurse only when at least one required column lives
// under that struct's dotted prefix; otherwise skip the entire sub-object.
//
// Benefit: a query that touches 1 field out of 10 pays decode cost for 1 field
// and only skip (byte-advance) cost for the other 9 — no string/variant allocs.

inline void parse_entity_selective_into(
    std::span<const uint8_t>& data,
    const AkStructSchema& schema,
    const std::string& prefix,
    const std::unordered_set<std::string>& required,
    std::unordered_map<std::string, FieldValue>& out
) {
    for (const auto& field : schema.fields) {
        const std::string full = prefix.empty() ? field.name : prefix + '.' + field.name;

        if (field.type->kind == AkSchema::Kind::STRUCT) {
            // Recurse only when at least one required column is nested under this struct.
            const std::string dot_prefix = full + '.';
            bool needed = false;
            for (const auto& col : required) {
                if (col.size() > full.size() && col.compare(0, dot_prefix.size(), dot_prefix) == 0) {
                    needed = true;
                    break;
                }
            }
            if (needed) { parse_entity_selective_into(data, *field.type->struct_schema, full, required, out); }
            else { skip_schema_value(data, *field.type); }
        }
        else {
            if (required.count(full)) { out[full] = read_schema_value(data, *field.type); }
            else { skip_schema_value(data, *field.type); }
        }
    }
}

[[nodiscard]] inline std::unordered_map<std::string, FieldValue> parse_entity_selective(
    std::span<const uint8_t> data,
    const AkStructSchema& schema,
    const std::unordered_set<std::string>& required
) {
    std::unordered_map<std::string, FieldValue> fields;
    fields.reserve(required.size());
    parse_entity_selective_into(data, schema, "", required, fields);
    return fields;
}

// ── AST node ──────────────────────────────────────────────────────────────────

struct AstNode {
    uint8_t  tag{};
    AstOp    op{};
    std::unique_ptr<AstNode> left, right;  // BIN: lhs/rhs; UN: child in left
    FieldValue  lit_val;                   // LIT
    std::string col_name;                  // COL (may be dotted: "address.city")
    int32_t     capture_idx{};             // CAPTURE
};

[[nodiscard]] inline std::unique_ptr<AstNode>
parse_ast_node(std::span<const uint8_t>& data)
{
    auto node = std::make_unique<AstNode>();
    node->tag = read_u8(data);

    switch (node->tag) {
        case NODE_BIN: {
            node->op    = static_cast<AstOp>(read_u8(data));
            node->left  = parse_ast_node(data);
            node->right = parse_ast_node(data);
            break;
        }
        case NODE_UN: {
            node->op   = static_cast<AstOp>(read_u8(data));
            node->left = parse_ast_node(data);
            break;
        }
        case NODE_LIT: {
            node->lit_val = read_lit_value(read_u8(data), data);
            break;
        }
        case NODE_COL: {
            const uint16_t len = read_u16(data);
            if (data.size() < len) throw std::runtime_error("AstEvaluator: col name underflow");
            node->col_name = std::string(reinterpret_cast<const char*>(data.data()), len);
            data = data.subspan(len);
            break;
        }
        case NODE_CAPTURE: {
            const uint32_t raw = read_u32(data);
            std::memcpy(&node->capture_idx, &raw, 4);
            break;
        }
        default:
            throw std::runtime_error("AstEvaluator: unknown node tag "
                                     + std::to_string(node->tag));
    }
    return node;
}

// ── Collect column names referenced by an AST ────────────────────────────────
//
// Walks the AST and inserts every NODE_COL col_name into `out`.
// Called once per ParsedQuery during from_bytes(); the result is reused across
// all matches() calls for that query.

inline void collect_cols(const AstNode& node, std::unordered_set<std::string>& out) {
    switch (node.tag) {
        case NODE_COL: out.insert(node.col_name);
            return;
        case NODE_BIN: collect_cols(*node.left, out);
            if (node.right) collect_cols(*node.right, out);
            return;
        case NODE_UN: collect_cols(*node.left, out);
            return;
        default: return; // NODE_LIT, NODE_CAPTURE — no column references
    }
}

// ── Evaluate an AST node ──────────────────────────────────────────────────────

[[nodiscard]] inline FieldValue eval_node(
    const AstNode&                                      node,
    const std::unordered_map<std::string, FieldValue>&  fields,
    const std::vector<FieldValue>&                      captures)
{
    switch (node.tag) {

        case NODE_LIT:
            return node.lit_val;

        case NODE_CAPTURE: {
            const auto idx = static_cast<size_t>(node.capture_idx);
            if (idx >= captures.size())
                throw std::runtime_error("AstEvaluator: capture index out of range");
            return captures[idx];
        }

        case NODE_COL: {
            const auto it = fields.find(node.col_name);
            return (it != fields.end()) ? it->second : FieldValue{};
        }

        case NODE_UN: {
            auto x = eval_node(*node.left, fields, captures);
            switch (node.op) {
                case AstOp::NOT:
                    if (!x.is_bool()) throw std::runtime_error("NOT: non-bool operand");
                    return FieldValue{ !x.as_bool() };
                case AstOp::IS_NULL:     return FieldValue{ x.is_null() };
                case AstOp::IS_NOT_NULL: return FieldValue{ !x.is_null() };
                default: throw std::runtime_error("AstEvaluator: unknown unary op");
            }
        }

        case NODE_BIN: {
            // Short-circuit AND / OR
            if (node.op == AstOp::AND) {
                auto l = eval_node(*node.left,  fields, captures);
                if (l.is_bool() && !l.as_bool()) return FieldValue{ false };
                auto r = eval_node(*node.right, fields, captures);
                return FieldValue{ l.is_bool() && l.as_bool() && r.is_bool() && r.as_bool() };
            }
            if (node.op == AstOp::OR) {
                auto l = eval_node(*node.left,  fields, captures);
                if (l.is_bool() && l.as_bool()) return FieldValue{ true };
                auto r = eval_node(*node.right, fields, captures);
                return FieldValue{ (l.is_bool()&&l.as_bool()) || (r.is_bool()&&r.as_bool()) };
            }

            // IN / NOT_IN — value ∈ List  or  key ∈ Map
            if (node.op == AstOp::IN || node.op == AstOp::NOT_IN) {
                const auto needle     = eval_node(*node.left,  fields, captures);
                const auto collection = eval_node(*node.right, fields, captures);
                bool found = false;
                if (collection.is_list()) {
                    for (const auto& e : collection.as_list())
                        if (e == needle) { found = true; break; }
                } else if (collection.is_map()) {
                    for (const auto& [k, v] : collection.as_map())
                        if (k == needle) { found = true; break; }
                }
                return FieldValue{ node.op == AstOp::IN ? found : !found };
            }

            // MAP_GET — it.scores["math"] → value at key, or null
            if (node.op == AstOp::MAP_GET) {
                const auto map_fv = eval_node(*node.left,  fields, captures);
                const auto key_fv = eval_node(*node.right, fields, captures);
                if (!map_fv.is_map()) return {};
                for (const auto& [k, val] : map_fv.as_map())
                    if (k == key_fv) return val;
                return {};
            }

            // Standard binary ops
            const auto lv = eval_node(*node.left,  fields, captures);
            const auto rv = eval_node(*node.right, fields, captures);

            if (node.op == AstOp::EQ)  return FieldValue{ lv == rv };
            if (node.op == AstOp::NEQ) return FieldValue{ !(lv == rv) };

            // Numeric comparison — int or double, promote to double
            const auto to_num = [](const FieldValue& fv) -> std::optional<double> {
                if (fv.is_int())    return (double)fv.as_int();
                if (fv.is_double()) return fv.as_double();
                return std::nullopt;
            };
            if (const auto ln = to_num(lv); ln) {
                if (const auto rn = to_num(rv); rn) {
                    switch (node.op) {
                        case AstOp::GT: return FieldValue{ *ln >  *rn };
                        case AstOp::GE: return FieldValue{ *ln >= *rn };
                        case AstOp::LT: return FieldValue{ *ln <  *rn };
                        case AstOp::LE: return FieldValue{ *ln <= *rn };
                        default: break;
                    }
                }
            }

            // String comparison
            if (lv.is_string() && rv.is_string()) {
                const auto& ls = lv.as_str();
                const auto& rs = rv.as_str();
                switch (node.op) {
                    case AstOp::GT: return FieldValue{ ls >  rs };
                    case AstOp::GE: return FieldValue{ ls >= rs };
                    case AstOp::LT: return FieldValue{ ls <  rs };
                    case AstOp::LE: return FieldValue{ ls <= rs };
                    default: break;
                }
            }

            throw std::runtime_error("AstEvaluator: cannot compare types for op "
                                     + std::to_string(static_cast<int>(node.op)));
        }

        default:
            throw std::runtime_error("AstEvaluator: unknown tag in eval "
                                     + std::to_string(node.tag));
    }
}

// ── ParsedQuery: parse once, evaluate many times ─────────────────────────────

struct ParsedQuery {
    std::vector<FieldValue> captures;
    std::unique_ptr<AstNode> root;
    std::unordered_set<std::string> required_cols; ///< COL names referenced by the AST

    [[nodiscard]] static ParsedQuery from_bytes(std::span<const uint8_t> data) {
        ParsedQuery q;
        const uint32_t raw = read_u32(data);
        int32_t num_caps; std::memcpy(&num_caps, &raw, 4);
        q.captures.reserve(static_cast<size_t>(num_caps));
        for (int32_t i = 0; i < num_caps; ++i)
            q.captures.push_back(read_lit_value(read_u8(data), data));
        q.root = parse_ast_node(data);
        collect_cols(*q.root, q.required_cols); // build required column set once per query
        return q;
    }

    [[nodiscard]] bool matches(const std::vector<uint8_t>& entity_bytes,
                               const AkStructSchema&        schema) const
    {
        auto sp     = std::span<const uint8_t>(entity_bytes);
        // Only decode columns referenced by the AST; skip everything else.
        auto fields = parse_entity_selective(sp, schema, required_cols);
        const auto result = eval_node(*root, fields, captures);
        return result.is_bool() && result.as_bool();
    }
};

} // namespace akkaradb::engine
