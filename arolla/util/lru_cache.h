// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef AROLLA_UTIL_LRU_CACHE_H_
#define AROLLA_UTIL_LRU_CACHE_H_

#include <cstddef>
#include <functional>
#include <list>
#include <utility>

#include "absl//base/nullability.h"
#include "absl//container/flat_hash_set.h"
#include "absl//hash/hash.h"
#include "absl//log/check.h"

namespace arolla {

// A simple LRU Cache implementation.
//
// This class is not thread-safe. If you need to use it from multiple threads,
// you must synchronize access to it yourself.
//
template <typename Key, typename Value, typename KeyHash = absl::Hash<Key>,
          typename KeyEq = std::equal_to<>>
class LruCache {
 public:
  // Initializes the cache with the specified `capacity`.
  //
  // NOTE: `capacity=0` is unsupported.
  explicit LruCache(size_t capacity) : capacity_(capacity) {
    DCHECK_GT(capacity, 0);
    index_.reserve(capacity + 1);
  }

  // Non-copyable, non-movable.
  LruCache(const LruCache&) = delete;
  LruCache& operator=(const LruCache&) = delete;

  // Returns a pointer to the value stored in cache under `key`; or nullptr if
  // the value is not present.
  //
  // The resulting pointer remains valid until the next 'Put' call.
  template <typename K>
  [[nodiscard]] absl::Nullable<const Value*> LookupOrNull(K&& key) {
    if (auto it = index_.find(std::forward<K>(key)); it != index_.end()) {
      entries_.splice(entries_.begin(), entries_, it->entry);
      return &it->entry->second;
    }
    return nullptr;
  }

  // Puts a value to the cache under `key` and returns a pointer to it. It keeps
  // the old entry if it is already present in the cache.
  template <typename K, typename V>
  [[nodiscard]] absl::Nonnull<const Value*> Put(K&& key, V&& value) {
    entries_.emplace_front(std::forward<K>(key), std::forward<V>(value));
    const auto& [it, ok] = index_.emplace(IndexEntry{entries_.begin()});
    if (!ok) {
      entries_.pop_front();  // Keep the original entry.
      entries_.splice(entries_.begin(), entries_, it->entry);
    } else if (entries_.size() > capacity_) {
      index_.erase(entries_.back().first);
      entries_.pop_back();
    }
    DCHECK_LE(entries_.size(), capacity_);
    DCHECK_EQ(entries_.size(), index_.size());
    return &entries_.front().second;
  }

  // Clears the cache.
  void Clear() {
    entries_.clear();
    index_.clear();
  }

 private:
  // The key-value entry; stored within the `records_` linked-list.
  using Entry = std::pair<const Key, const Value>;

  // A table entry; stored within the `index_` hash-table and points to an
  // element of `records_`.
  struct IndexEntry {
    typename std::list<Entry>::iterator entry;
  };

  // A hash-function for the index records.
  struct IndexRecordHash {
    using is_transparent = void;

    size_t operator()(const IndexEntry& index_record) const {
      return KeyHash()(index_record.entry->first);
    }
    template <typename K>
    size_t operator()(K&& key) const {
      return KeyHash()(std::forward<K>(key));
    }
  };

  // An equality predicate the index records.
  struct IndexRecordEq {
    using is_transparent = void;

    bool operator()(const IndexEntry& lhs, const IndexEntry& rhs) const {
      return KeyEq()(lhs.entry->first, rhs.entry->first);
    }
    template <typename K>
    bool operator()(const IndexEntry& lhs, K&& rhs) const {
      return KeyEq()(lhs.entry->first, std::forward<K>(rhs));
    }
  };

  using Index = absl::flat_hash_set<IndexEntry, IndexRecordHash, IndexRecordEq>;

  size_t capacity_;

  // A linked-list that stores key-value entries in order from most recently
  // used (at the beginning) to least recently used (at the end).
  //
  // We use std::list because it has O(1) time complexity for moving elements to
  // the front and its iterators remain valid during insertion and deletion.
  std::list<Entry> entries_;
  Index index_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_LRU_CACHE_H_
