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
#ifndef AROLLA_ARRAY_ID_FILTER_H_
#define AROLLA_ARRAY_ID_FILTER_H_

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <utility>

#include "absl//base/attributes.h"
#include "absl//log/check.h"
#include "absl//types/span.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

// IdFilter is a mapping ID -> optional<OFFSET>.
// If type is kEmpty, then OFFSET is missing for every ID.
// If type is kFull, then OFFSET is always present and always equal to the ID.
// If type is kPartial, then all present IDs are listed in `ids_` and OFFSET is
// an index in this list.
class IdFilter {
 public:
  using IdWithOffset = int64_t;
  enum Type { kEmpty, kPartial, kFull };

  // If more than 25% (`kDenseSparsityLimit`) of Array elements are present,
  // then the dense form of the Array (i.e. with IdFilter::type == kFull) is
  // preferred to the sparse form (IdFilter::type == kPartial).
  // This number is chosen according to benchmarks in `array/benchmarks.cc`.
  // Array conversion is relatively expensive, so this limit doesn't mean that
  // any Array with >25% present value should be immediately converted to
  // the dense from. This limit is used in Array operations to choose between
  // dense/sparse for a newly created Array.
  static constexpr double kDenseSparsityLimit = 0.25;

  // For types kEmpty and kFull.
  /* implicit */ IdFilter(Type type)  // NOLINT(google-explicit-constructor)
      : type_(type) {
    DCHECK_NE(type, kPartial);
  }

  // Values in `ids` must be in increasing order. Each value must be in range
  // [ids_offset, ids_offset + size).
  // IdFilter doesn't store `size`. It is needed only to validate `ids` and set
  // type to kFull if all ids are present.
  explicit IdFilter(int64_t size, Buffer<IdWithOffset> ids,
                    int64_t ids_offset = 0)
      : type_(kPartial), ids_(std::move(ids)), ids_offset_(ids_offset) {
    if (ids.empty()) {
      type_ = kEmpty;
      ids_offset_ = 0;
    } else if (ids.size() == size) {
      type_ = kFull;
      ids_ = Buffer<IdWithOffset>();
      ids_offset_ = 0;
    } else {
      DCHECK_GE(ids.front(), ids_offset);
#ifndef NDEBUG
      for (int64_t i = 1; i < ids.size(); ++i) {
        DCHECK_LT(ids[i - 1], ids[i]);
      }
#endif
      DCHECK_LT(ids.back(), ids_offset + size);
    }
  }

  OptionalValue<int64_t> IdToOffset(int64_t id) const {  // id is zero-based
    switch (type_) {
      case kFull:
        return id;
      case kPartial: {
        auto iter =
            std::lower_bound(ids_.begin(), ids_.end(), id + ids_offset_);
        int64_t offset = std::distance(ids_.begin(), iter);
        bool present = iter != ids_.end() && ids_[offset] == id + ids_offset_;
        return {present, offset};
      }
      case kEmpty:
      default:
        return {};
    }
  }

  int64_t IdsOffsetToId(int64_t offset) const {
    DCHECK_EQ(type_, kPartial);
    DCHECK_GE(offset, 0);
    DCHECK_LT(offset, ids_.size());
    return ids_[offset] - ids_offset_;
  }

  // Note: returns false if filters point to different id buffers, even if the
  // buffers content is same.
  bool IsSame(const IdFilter& other) const {
    if (type_ != other.type_) return false;
    if (type_ == kPartial) {
      return ids_.begin() == other.ids_.begin() &&
             ids_.end() == other.ids_.end() && ids_offset_ == other.ids_offset_;
    } else {
      return true;
    }
  }

  Type type() const { return type_; }
  const Buffer<IdWithOffset>& ids() const {
    DCHECK_NE(type_, kFull)
        << "Requesting ids on full filter is error prone. Ids are empty, which "
           "can be used incorrectly.";  // it is fine for kEmpty
    return ids_;
  }
  int64_t ids_offset() const { return ids_offset_; }

  // Calls `fn(id, offset_in_f1, offset_in_f2)` for each common (id-ids_offset).
  // Both f1 and f2 must be non-empty.
  template <class Id1, class Id2, class Fn>
  static void ForEachCommonId(absl::Span<const Id1> f1, Id1 ids_offset1,
                              absl::Span<const Id2> f2, Id2 ids_offset2,
                              Fn&& fn);

  // Calls `fn(id, offset_in_f1, offset_in_f2)` for each common id.
  // Both f1 and f2 must have type kPartial.
  template <class Fn>
  static void IntersectPartial_ForEach(const IdFilter& f1, const IdFilter& f2,
                                       Fn&& fn) {
    DCHECK_EQ(f1.type_, kPartial);
    DCHECK_EQ(f2.type_, kPartial);
    ForEachCommonId(f1.ids_.span(), f1.ids_offset_, f2.ids_.span(),
                    f2.ids_offset_, std::forward<Fn>(fn));
  }

  // Returns an IdFilter that contains at least all ids from given id filters.
  // For performance reason can return kFull even if some ids are missing.
  template <class... IdFilters>
  static IdFilter UpperBoundMerge(int64_t size, RawBufferFactory* buf_factory,
                                  const IdFilter& f, const IdFilters&... fs);

  // Returns the smallest of all given filters. The resulted filter contains
  // at least all ids that present in intersection of all given filters.
  template <class... IdFilters>
  static const IdFilter& UpperBoundIntersect(const IdFilter& f,
                                             const IdFilters&... fs);

 private:
  static IdFilter UpperBoundMergeImpl(int64_t size,
                                      RawBufferFactory* buf_factory,
                                      const IdFilter& a, const IdFilter& b);
  static const IdFilter& UpperBoundIntersectImpl(const IdFilter& a,
                                                 const IdFilter& b);

  Type type_;
  // Must be in increasing order. Empty if type != kPartial.
  Buffer<IdWithOffset> ids_;
  int64_t ids_offset_ = 0;  // Used if values in ids buffer are not zero based.
};

template <class Id1, class Id2, class Fn>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline void IdFilter::ForEachCommonId(
    absl::Span<const Id1> f1, Id1 ids_offset1, absl::Span<const Id2> f2,
    Id2 ids_offset2, Fn&& fn) {
  // Don't change this code without running benchmarks
  // BM_WithIds.*, BM_Add/.*, BM_Add_Union/.* in benchmarks.cc.
  DCHECK(!f1.empty());
  DCHECK(!f2.empty());
  auto iter1 = f1.begin();
  auto iter2 = f2.begin();
  int64_t id1 = *iter1 - ids_offset1;
  int64_t id2 = *iter2 - ids_offset2;
  int64_t max_id =
      std::min<int64_t>(f1.back() - ids_offset1, f2.back() - ids_offset2);
  // It is important for performance that in all conditions in the loop we use
  // only id1/id2. `max_id` is needed to avoid the end range checks for iters.
  while (id1 < max_id && id2 < max_id) {
    if (id1 == id2) {
      fn(id1, iter1 - f1.begin(), iter2 - f2.begin());
      id1 = *(++iter1) - ids_offset1;
      id2 = *(++iter2) - ids_offset2;
    }
    // In pointwise operations on Arrays this function is used in such a way
    // that `f2` is usually more sparse than `f1`. Because of this for the best
    // performance we use `while` for `id1` and `if` for `id2`.
    while (id1 < std::min(max_id, id2)) {
      id1 = *(++iter1) - ids_offset1;
    }
    if (id2 < std::min(max_id, id1)) {
      id2 = *(++iter2) - ids_offset2;
    }
  }
  while (id1 < max_id) {
    id1 = *(++iter1) - ids_offset1;
  }
  while (id2 < max_id) {
    id2 = *(++iter2) - ids_offset2;
  }
  if (id1 == id2) {
    fn(id1, iter1 - f1.begin(), iter2 - f2.begin());
  }
}

template <class... IdFilters>
IdFilter IdFilter::UpperBoundMerge(
    int64_t size ABSL_ATTRIBUTE_UNUSED,  // unused if sizeof...(fs) == 0
    RawBufferFactory* buf_factory ABSL_ATTRIBUTE_UNUSED, const IdFilter& f,
    const IdFilters&... fs) {
  if constexpr (sizeof...(fs) == 0) {
    return f;
  } else {
    return UpperBoundMergeImpl(size, buf_factory, f,
                               UpperBoundMerge(size, buf_factory, fs...));
  }
}

template <class... IdFilters>
const IdFilter& IdFilter::UpperBoundIntersect(const IdFilter& f,
                                              const IdFilters&... fs) {
  if constexpr (sizeof...(fs) == 0) {
    return f;
  } else {
    return UpperBoundIntersectImpl(f, UpperBoundIntersect(fs...));
  }
}

inline const IdFilter& IdFilter::UpperBoundIntersectImpl(const IdFilter& a,
                                                         const IdFilter& b) {
  if (a.type() == kEmpty || b.type() == kFull) return a;
  if (b.type() == kEmpty || a.type() == kFull) return b;
  return a.ids().size() < b.ids().size() ? a : b;
}

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(IdFilter);

}  // namespace arolla

#endif  // AROLLA_ARRAY_ID_FILTER_H_
