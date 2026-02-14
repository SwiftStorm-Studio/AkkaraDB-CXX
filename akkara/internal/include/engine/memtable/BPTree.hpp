/*
* AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

// internal/include/engine/memtable/BPTree.hpp
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <vector>

#ifdef _MSC_VER
#include <intrin.h>
#include <emmintrin.h>
#endif

namespace akkaradb::engine::memtable {
    /**
     * BPTree - B+ Tree implementation optimized for cache efficiency.
     *
     * Design goals:
     * - Node size: 256 bytes (4 cache lines)
     * - Keys sorted within nodes for binary search
     * - Leaf nodes linked for efficient range scans
     * - Thread-safety: External synchronization required
     *
     * Performance characteristics:
     * - Point lookup: O(log N) with excellent cache locality
     * - Range scan: O(log N + K) where K = result count
     * - Insert: O(log N) with node splits
     *
     * Template parameters:
     * - K: Key type (must be copyable and comparable)
     * - V: Value type (must be copyable)
     * - Compare: Comparison functor (default: std::less<K>)
     */
    template<typename K, typename V, typename Compare = std::less<K>>
    class BPTree {
    public:
        // Forward declarations
        class Iterator;

        BPTree();
        ~BPTree();

        // Non-copyable, movable
        BPTree(const BPTree&) = delete;
        BPTree& operator=(const BPTree&) = delete;
        BPTree(BPTree&&) noexcept;
        BPTree& operator=(BPTree&&) noexcept;

        // Lookup
        [[nodiscard]] std::optional<V> get(const K& key) const;
        [[nodiscard]] bool contains(const K& key) const;

        // Modification
        void put(const K& key, const V& value);
        bool remove(const K& key);
        void clear();

        // Iteration
        [[nodiscard]] Iterator lower_bound(const K& key) const;

        // Heterogeneous lookup (for transparent comparators with is_transparent)
        template<typename KeyLike>
        [[nodiscard]] Iterator lower_bound(const KeyLike& key_like) const;

        [[nodiscard]] Iterator begin() const;
        [[nodiscard]] Iterator end() const;

        // Size
        [[nodiscard]] size_t size() const noexcept { return size_; }
        [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

        // Diagnostic info
        [[nodiscard]] static constexpr size_t leaf_order() noexcept { return LEAF_ORDER; }
        [[nodiscard]] static constexpr size_t internal_order() noexcept { return INTERNAL_ORDER; }

    private:
        struct Node;
        struct InternalNode;
        struct LeafNode;

        // Node type tag
        enum class NodeType : uint8_t { INTERNAL, LEAF };

        // Calculate optimal order based on node size target
        // For MemTable: 32KB nodes - optimal balance between tree height and cache efficiency
        // Larger nodes (64KB+) cause cache misses and slower intra-node search
        static constexpr size_t calculate_leaf_order() {
            // Target: 32KB per node - sweet spot for MemRecord (88 bytes)
            // LeafNode overhead: ~16 bytes (type, count, next pointer)
            constexpr size_t target_size = 32768;
            constexpr size_t overhead = 16;
            constexpr size_t entry_size = sizeof(K) + sizeof(V);
            constexpr size_t max_entries = (target_size - overhead) / entry_size;
            return max_entries > 4 ? max_entries : 4; // Minimum 4 entries
        }

        static constexpr size_t calculate_internal_order() {
            // Internal nodes: 32KB target
            constexpr size_t target_size = 32768;
            constexpr size_t overhead = 16;
            constexpr size_t entry_size = sizeof(K) + sizeof(Node*);
            constexpr size_t max_entries = (target_size - overhead) / entry_size;
            return max_entries > 4 ? max_entries : 4;
        }

        static constexpr size_t LEAF_ORDER = calculate_leaf_order();
        static constexpr size_t INTERNAL_ORDER = calculate_internal_order();

        // Base node structure (no virtual - avoid vptr overhead)
        struct Node {
            NodeType type;
            uint16_t count = 0;

            explicit Node(NodeType t) noexcept : type{t} {}
            // Non-virtual destructor - manual cleanup required

            [[nodiscard]] bool is_leaf() const noexcept { return type == NodeType::LEAF; }
            [[nodiscard]] bool is_internal() const noexcept { return type == NodeType::INTERNAL; }
        };

        // Internal node (non-leaf)
        struct InternalNode : Node {
            std::array<K, INTERNAL_ORDER> keys;
            std::array<Node*, INTERNAL_ORDER + 1> children{};

            InternalNode() noexcept : Node{NodeType::INTERNAL} {}
            // No destructor - manual cleanup via destroy_tree()

            // Find child index for given key
            [[nodiscard]] size_t find_child_index(const K& key, const Compare& comp) const {
                // Binary search to find first key >= search key
                auto it = std::lower_bound(keys.begin(), keys.begin() + this->count, key, comp);
                return static_cast<size_t>(it - keys.begin());
            }
        };

        // Leaf node (contains actual data)
        struct LeafNode : Node {
            std::array<K, LEAF_ORDER> keys;
            std::array<V, LEAF_ORDER> values;
            LeafNode* next = nullptr; // For range scans

            LeafNode() noexcept : Node{NodeType::LEAF} {}

            // Binary search for key
            [[nodiscard]] std::optional<size_t> find_index(const K& key, const Compare& comp) const {
                auto it = std::lower_bound(keys.begin(), keys.begin() + this->count, key, comp);
                if (it != keys.begin() + this->count && !comp(key, *it) && !comp(*it, key)) {
                    return static_cast<size_t>(it - keys.begin());
                }
                return std::nullopt;
            }

            [[nodiscard]] size_t find_insert_index(const K& key, const Compare& comp) const {
                auto it = std::lower_bound(keys.begin(), keys.begin() + this->count, key, comp);
                return static_cast<size_t>(it - keys.begin());
            }
        };

        // Split result
        struct SplitResult {
            Node* left;
            K separator;
            Node* right;
        };

        // Tree structure
        Node* root_ = nullptr;
        size_t size_ = 0;
        [[no_unique_address]] Compare comp_;

        // Internal operations
        std::optional<V> search_leaf(const LeafNode* leaf, const K& key) const;
        std::optional<SplitResult> insert_internal(Node* node, const K& key, const V& value);
        std::optional<SplitResult> insert_into_leaf(LeafNode* leaf, const K& key, const V& value);
        std::optional<SplitResult> insert_into_internal(InternalNode* node, const K& key, const V& value);
        SplitResult split_leaf(LeafNode* leaf);
        SplitResult split_internal(InternalNode* node);
        LeafNode* find_leaf(const K& key) const;

        // Manual tree destruction (post-order traversal, non-recursive)
        void destroy_tree(Node* root) noexcept;

    public:
        // Iterator
        class Iterator {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = std::pair<const K&, const V&>;
            using difference_type = std::ptrdiff_t;
            using pointer = value_type*;
            using reference = value_type;

            Iterator() = default;
            Iterator(LeafNode* node, size_t index) : node_{node}, index_{index} {}

            reference operator*() const {
                return {node_->keys[index_], node_->values[index_]};
            }

            Iterator& operator++() {
                ++index_;
                if (index_ >= node_->count) {
                    node_ = node_->next;
                    index_ = 0;
                }
                return *this;
            }

            Iterator operator++(int) {
                Iterator tmp = *this;
                ++(*this);
                return tmp;
            }

            bool operator==(const Iterator& other) const {
                return node_ == other.node_ && index_ == other.index_;
            }

            bool operator!=(const Iterator& other) const {
                return !(*this == other);
            }

        private:
            LeafNode* node_ = nullptr;
            size_t index_ = 0;
        };
    };

    // ==================== Implementation ====================

    // ==================== Size Verification ====================
    // Verify node sizes fit in target size for optimal performance
    template<typename K, typename V, typename Compare>
    inline constexpr bool verify_node_sizes() {
        static_assert(sizeof(typename BPTree<K, V, Compare>::LeafNode) <= 65536,
                      "LeafNode exceeds 64KB - reduce LEAF_ORDER");
        static_assert(sizeof(typename BPTree<K, V, Compare>::InternalNode) <= 65536,
                      "InternalNode exceeds 64KB - reduce INTERNAL_ORDER");
        return true;
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::BPTree() = default;

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::~BPTree() {
        if (root_) destroy_tree(root_);
    }

    template<typename K, typename V, typename Compare>
    void BPTree<K, V, Compare>::destroy_tree(Node* node) noexcept {
        if (!node) return;

        // Post-order traversal (iterative to avoid stack overflow)
        std::vector<Node*> stack;
        std::vector<Node*> to_delete;
        stack.push_back(node);

        while (!stack.empty()) {
            Node* current = stack.back();
            stack.pop_back();
            to_delete.push_back(current);

            if (current->is_internal()) {
                auto* internal = static_cast<InternalNode*>(current);
                for (size_t i = 0; i <= internal->count; ++i) {
                    if (internal->children[i]) {
                        stack.push_back(internal->children[i]);
                    }
                }
            }
        }

        // Delete in reverse order (children before parents)
        for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
            if ((*it)->is_leaf()) {
                delete static_cast<LeafNode*>(*it);
            } else {
                delete static_cast<InternalNode*>(*it);
            }
        }
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::BPTree(BPTree&& other) noexcept
        : root_{other.root_}, size_{other.size_}, comp_{std::move(other.comp_)} {
        other.root_ = nullptr;
        other.size_ = 0;
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>& BPTree<K, V, Compare>::operator=(BPTree&& other) noexcept {
        if (this != &other) {
            delete root_;
            root_ = other.root_;
            size_ = other.size_;
            comp_ = std::move(other.comp_);
            other.root_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    template<typename K, typename V, typename Compare>
    std::optional<V> BPTree<K, V, Compare>::get(const K& key) const {
        if (!root_) return std::nullopt;

        LeafNode* leaf = find_leaf(key);
        return search_leaf(leaf, key);
    }

    template<typename K, typename V, typename Compare>
    bool BPTree<K, V, Compare>::contains(const K& key) const {
        return get(key).has_value();
    }

    template<typename K, typename V, typename Compare>
    void BPTree<K, V, Compare>::put(const K& key, const V& value) {
        if (!root_) {
            // Create initial leaf node
            auto* leaf = new LeafNode{};
            leaf->keys[0] = key;
            leaf->values[0] = value;
            leaf->count = 1;
            root_ = leaf;
            ++size_;
            return;
        }

        if (auto split = insert_internal(root_, key, value)) {
            // Root was split, create new root
            auto* new_root = new InternalNode{};
            new_root->keys[0] = split->separator;
            new_root->children[0] = split->left;
            new_root->children[1] = split->right;
            new_root->count = 1;
            root_ = new_root;
        }
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::LeafNode* BPTree<K, V, Compare>::find_leaf(const K& key) const {
        Node* current = root_;

        while (current && current->is_internal()) {
            auto* internal = static_cast<InternalNode*>(current);
            size_t idx = internal->find_child_index(key, comp_);
            Node* next = internal->children[idx];

            // Prefetch next node to reduce memory latency
#ifdef _MSC_VER
            _mm_prefetch(reinterpret_cast<const char*>(next), _MM_HINT_T0);
#else
            __builtin_prefetch(next, 0, 3);
#endif

            current = next;
        }

        return static_cast<LeafNode*>(current);
    }

    template<typename K, typename V, typename Compare>
    std::optional<V> BPTree<K, V, Compare>::search_leaf(const LeafNode* leaf, const K& key) const {
        if (!leaf) return std::nullopt;

        auto idx = leaf->find_index(key, comp_);
        if (idx) {
            return leaf->values[*idx];
        }
        return std::nullopt;
    }

    template<typename K, typename V, typename Compare>
    std::optional<typename BPTree<K, V, Compare>::SplitResult>
    BPTree<K, V, Compare>::insert_internal(Node* node, const K& key, const V& value) {
        if (node->is_leaf()) {
            return insert_into_leaf(static_cast<LeafNode*>(node), key, value);
        } else {
            return insert_into_internal(static_cast<InternalNode*>(node), key, value);
        }
    }

    template<typename K, typename V, typename Compare>
    std::optional<typename BPTree<K, V, Compare>::SplitResult>
    BPTree<K, V, Compare>::insert_into_leaf(LeafNode* leaf, const K& key, const V& value) {
        // Check if key already exists (update case)
        auto existing_idx = leaf->find_index(key, comp_);
        if (existing_idx) {
            leaf->values[*existing_idx] = value;
            return std::nullopt; // No split needed
        }

        // Check if we need to split
        if (leaf->count >= LEAF_ORDER) {
            // Use fixed-size array instead of vector (avoid heap allocation)
            std::array<std::pair<K, V>, LEAF_ORDER + 1> temp;

            size_t insert_idx = leaf->find_insert_index(key, comp_);
            size_t temp_idx = 0;

            // Copy entries before insertion point
            for (size_t i = 0; i < insert_idx; ++i) {
                temp[temp_idx++] = {leaf->keys[i], leaf->values[i]};
            }
            // Insert new entry
            temp[temp_idx++] = {key, value};
            // Copy remaining entries
            for (size_t i = insert_idx; i < leaf->count; ++i) {
                temp[temp_idx++] = {leaf->keys[i], leaf->values[i]};
            }

            // Split point (ceil)
            size_t mid = (temp_idx + 1) / 2;

            // Update left (existing) leaf
            leaf->count = static_cast<uint16_t>(mid);
            for (size_t i = 0; i < mid; ++i) {
                leaf->keys[i] = std::move(temp[i].first);
                leaf->values[i] = std::move(temp[i].second);
            }

            // Create right leaf
            auto* right = new LeafNode{};
            right->count = static_cast<uint16_t>(temp_idx - mid);
            for (size_t i = mid; i < temp_idx; ++i) {
                right->keys[i - mid] = std::move(temp[i].first);
                right->values[i - mid] = std::move(temp[i].second);
            }

            // Link leaves
            right->next = leaf->next;
            leaf->next = right;

            ++size_;
            return SplitResult{leaf, right->keys[0], right};
        }

        // No split needed, insert normally
        size_t insert_idx = leaf->find_insert_index(key, comp_);
        for (size_t i = leaf->count; i > insert_idx; --i) {
            leaf->keys[i] = leaf->keys[i - 1];
            leaf->values[i] = leaf->values[i - 1];
        }
        leaf->keys[insert_idx] = key;
        leaf->values[insert_idx] = value;
        ++leaf->count;
        ++size_;

        return std::nullopt;
    }

    template<typename K, typename V, typename Compare>
    std::optional<typename BPTree<K, V, Compare>::SplitResult>
    BPTree<K, V, Compare>::insert_into_internal(InternalNode* node, const K& key, const V& value) {
        size_t child_idx = node->find_child_index(key, comp_);
        auto split = insert_internal(node->children[child_idx], key, value);

        if (!split) return std::nullopt; // No split propagated

        // Child was split, need to insert separator into this node
        if (node->count >= INTERNAL_ORDER) {
            // Use fixed-size arrays (avoid heap allocation)
            std::array<K, INTERNAL_ORDER + 1> temp_keys;
            std::array<Node*, INTERNAL_ORDER + 2> temp_children;
            size_t key_idx = 0, child_idx_ptr = 0;

            // Copy existing entries before insertion point
            for (size_t i = 0; i < child_idx; ++i) {
                temp_keys[key_idx++] = node->keys[i];
                temp_children[child_idx_ptr++] = node->children[i];
            }

            // Insert split result
            temp_children[child_idx_ptr++] = split->left;
            temp_keys[key_idx++] = split->separator;
            temp_children[child_idx_ptr++] = split->right;

            // Copy remaining entries
            for (size_t i = child_idx; i < node->count; ++i) {
                temp_keys[key_idx++] = node->keys[i];
                temp_children[child_idx_ptr++] = node->children[i + 1];
            }

            // Split this internal node
            size_t mid = key_idx / 2;
            K promote_key = std::move(temp_keys[mid]);

            // Update left (existing) node
            node->count = static_cast<uint16_t>(mid);
            for (size_t i = 0; i < mid; ++i) {
                node->keys[i] = std::move(temp_keys[i]);
                node->children[i] = temp_children[i];
            }
            node->children[mid] = temp_children[mid];

            // Create right node
            auto* right = new InternalNode{};
            right->count = static_cast<uint16_t>(key_idx - mid - 1);
            for (size_t i = mid + 1; i < key_idx; ++i) {
                right->keys[i - mid - 1] = std::move(temp_keys[i]);
                right->children[i - mid - 1] = temp_children[i];
            }
            right->children[right->count] = temp_children[child_idx_ptr - 1];

            return SplitResult{node, std::move(promote_key), right};
        }

        // Insert split result into this node (no split needed)
        for (size_t i = node->count; i > child_idx; --i) {
            node->keys[i] = node->keys[i - 1];
            node->children[i + 1] = node->children[i];
        }
        node->keys[child_idx] = split->separator;
        node->children[child_idx] = split->left;
        node->children[child_idx + 1] = split->right;
        ++node->count;

        return std::nullopt;
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::SplitResult BPTree<K, V, Compare>::split_leaf(LeafNode* /*leaf*/) {
        // Not used - splitting is done inline in insert_into_leaf
        throw std::logic_error("split_leaf should not be called");
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::SplitResult BPTree<K, V, Compare>::split_internal(InternalNode* /*node*/) {
        // Not used - splitting is done inline in insert_into_internal
        throw std::logic_error("split_internal should not be called");
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::Iterator BPTree<K, V, Compare>::lower_bound(const K& key) const {
        if (!root_) return end();

        LeafNode* leaf = find_leaf(key);
        if (!leaf) return end();

        size_t idx = leaf->find_insert_index(key, comp_);
        if (idx >= leaf->count) {
            // Key is past this leaf, move to next
            return Iterator{leaf->next, 0};
        }

        return Iterator{leaf, idx};
    }

    template<typename K, typename V, typename Compare>
    template<typename KeyLike>
    BPTree<K, V, Compare>::Iterator BPTree<K, V, Compare>::lower_bound(const KeyLike& key_like) const {
        if (!root_) return end();

        // Navigate to the appropriate leaf
        Node* current = root_;
        while (current && current->is_internal()) {
            auto* internal = static_cast<InternalNode*>(current);
            // Find first key >= key_like
            auto it = std::lower_bound(
                internal->keys.begin(),
                internal->keys.begin() + internal->count,
                key_like,
                comp_
            );
            size_t idx = static_cast<size_t>(it - internal->keys.begin());
            current = internal->children[idx];
        }

        auto* leaf = static_cast<LeafNode*>(current);
        if (!leaf) return end();

        // Binary search in leaf
        auto it = std::lower_bound(
            leaf->keys.begin(),
            leaf->keys.begin() + leaf->count,
            key_like,
            comp_
        );
        size_t idx = static_cast<size_t>(it - leaf->keys.begin());

        if (idx >= leaf->count) {
            // Key is past this leaf, move to next
            return Iterator{leaf->next, 0};
        }

        return Iterator{leaf, idx};
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::Iterator BPTree<K, V, Compare>::begin() const {
        if (!root_) return end();

        // Find leftmost leaf
        Node* current = root_;
        while (current->is_internal()) {
            current = static_cast<InternalNode*>(current)->children[0];
        }

        auto* leaf = static_cast<LeafNode*>(current);
        return Iterator{leaf, 0};
    }

    template<typename K, typename V, typename Compare>
    BPTree<K, V, Compare>::Iterator BPTree<K, V, Compare>::end() const {
        return Iterator{nullptr, 0};
    }

    template<typename K, typename V, typename Compare>
    bool BPTree<K, V, Compare>::remove(const K& key) {
        // TODO: Implement removal (for now, just track size)
        // Removal is complex and requires rebalancing/merging
        // For initial implementation, we can skip this
        return false;
    }

    template<typename K, typename V, typename Compare>
    void BPTree<K, V, Compare>::clear() {
        delete root_;
        root_ = nullptr;
        size_ = 0;
    }

} // namespace akkaradb::engine::memtable
