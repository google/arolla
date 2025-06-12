// Copyright 2025 Google LLC
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
#ifndef AROLLA_ARRAY_OPS_UTIL_H_
#define AROLLA_ARRAY_OPS_UTIL_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/util.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"

namespace arolla::array_ops_internal {

inline void empty_missing_fn(int64_t, int64_t) {}

template <bool ConvertToDense, class ArgList>
class ArrayOpsUtil;

// ArrayOpsUtil is used to iterate over a set of Arrays with a callback that
// is called for all valid rows. A valid row is a row where all non-optional
// arguments are present.

// Template pack Ts... defines types and optionality of arguments.
// For example if i-th argument of an operator is Array<V>,
// then i-th type in the pack should be either T or (if the argument is
// optional) OptionalValue<T> where view_type_t<T> == view_type_t<V>.

// `ConvertToDense` is a performance-related setting. If true, then all Arrays
// will be converted to dense form before iteration. It simplifies the produced
// code (better for inlining), but can be inefficient if arguments are sparse.

template <bool ConvertToDense, class... Ts>
class ArrayOpsUtil<ConvertToDense, meta::type_list<Ts...>> {
 public:
  explicit ArrayOpsUtil(int64_t size, const AsArray<Ts>&... args,
                        RawBufferFactory* buf_factory = GetHeapBufferFactory())
      : size_(size) {
    DCHECK(((size == args.size()) && ... && true));
    if constexpr (ConvertToDense) {
      dense_ = std::make_tuple(args.ToDenseForm(buf_factory).dense_data()...);
    } else {
      default_valid_ = !(BoundsValidIds<Ts>(args) || ...);
      if (default_valid_) {
        default_values_ = std::make_tuple(
            MaybeUnwrapOptional<Ts>(args.missing_id_value())...);
      }
      if (IsSameIdFilter(args...)) {
        ids_ = First(args...).id_filter();
        dense_ = std::make_tuple(args.dense_data()...);
      } else {
        if (!default_valid_) {
          IdFilter full(IdFilter::kFull);
          // BoundsValidIds<Ts>(args) == true means that `args.id_filter()`
          // is an upper bound of all valid ids. We intersect all such filters,
          // so `ids_` contains all ids, that theoretically can be valid.
          // Arrays that have missing_id_value or correspond to optional
          // arguments are excluded from the intersecton by substituting
          // with `full`.
          ids_ = IdFilter::UpperBoundIntersect(
              (BoundsValidIds<Ts>(args) ? args.id_filter() : full)...);
        } else {
          ids_ = IdFilter::UpperBoundMerge(First(args...).size(), buf_factory,
                                           args.id_filter()...);
        }
        dense_ = std::make_tuple(
            args.WithIds(ids_, args.missing_id_value(), buf_factory)
                .dense_data()...);
      }
    }
  }

  // Calls `fn` or `repeated_fn` for valid rows in range [from, to) and
  // `missing_fn` for row where some required arguments are missing.
  // `fn` is called with the arguments (int64_t id, X<Ts>... args), where
  // X<T> is either view_type_t<T> (required argument) or
  // OptionalValue<view_type_t<T>> (optional argument).
  // `repeated_fn` is called with the arguments
  // (int64_t first_id, int64_t count, X<Ts>... args).
  // `missing_fn` has two arguments (int64_t first_id, int64_t count).
  template <class Fn, class RepeatedFn, class MissedFn>
  void Iterate(int64_t from, int64_t to, Fn&& fn, MissedFn&& missing_fn,
               RepeatedFn&& repeated_fn) const {
    return Iterate(std::forward<Fn>(fn), std::forward<RepeatedFn>(repeated_fn),
                   std::forward<MissedFn>(missing_fn),
                   std::index_sequence_for<Ts...>{}, from, to);
  }

  template <class Fn, class MissedFn>
  void Iterate(int64_t from, int64_t to, Fn&& fn, MissedFn&& missing_fn) const {
    auto repeated_fn = [&](int64_t id, int64_t count, view_type_t<Ts>... args) {
      for (int64_t i = 0; i < count; ++i) fn(id + i, args...);
    };
    return Iterate(fn, std::move(repeated_fn),
                   std::forward<MissedFn>(missing_fn),
                   std::index_sequence_for<Ts...>{}, from, to);
  }

  template <class Fn>
  void Iterate(int64_t from, int64_t to, Fn&& fn) const {
    Iterate(from, to, std::forward<Fn>(fn), empty_missing_fn);
  }

  // `IterateSimple` is a simplified version of `Iterate`. It doesn't support
  // repeated_fn, missing_fn, and arbitrary iteration range, but produces more
  // compact code.
  template <class Fn>
  void IterateSimple(Fn&& fn) const {
    return IterateSimple(std::forward<Fn>(fn),
                         std::index_sequence_for<Ts...>{});
  }

  int64_t size() const { return size_; }

  // Returns a value that is <= size and >= then the actual number of valid
  // (i.e. where all required aruments are present) rows for iteration.
  int64_t PresentCountUpperEstimate() const {
    if (ids_.type() == IdFilter::kFull || default_valid_) {
      return size_;
    } else {
      return ids_.ids().size();
    }
  }

 private:
  using DenseUtil = dense_ops_internal::DenseOpsUtil<meta::type_list<Ts...>>;

  template <class Fn, class RepeatedFn, class MissedFn, size_t... Is>
  void Iterate(Fn&& fn, RepeatedFn&& repeated_fn, MissedFn&& missing_fn,
               std::index_sequence<Is...>, uint64_t from, uint64_t to) const {
    DCHECK_GE(from, 0);
    DCHECK_GE(to, from);
    if (ids_.type() == IdFilter::kFull) {
      DenseUtil::Iterate(
          [&](int64_t id, bool valid, view_type_t<Ts>... args) {
            if (valid) {
              fn(id, args...);
            } else {
              missing_fn(id, 1);
            }
          },
          from, to, std::get<Is>(dense_)...);
      return;
    }
    DCHECK(!ConvertToDense) << "If ConvertToDense=true, `ids_` must be full";
    if constexpr (!ConvertToDense) {
      auto defaultFn = [&](int64_t id, int64_t row_count) {
        if (default_valid_) {
          repeated_fn(id, row_count, std::get<Is>(default_values_)...);
        } else {
          missing_fn(id, row_count);
        }
      };
      auto ids_iter = std::lower_bound(ids_.ids().begin(), ids_.ids().end(),
                                       from + ids_.ids_offset());
      int64_t offset_from = std::distance(ids_.ids().begin(), ids_iter);
      auto iter_to = std::lower_bound(ids_.ids().begin(), ids_.ids().end(),
                                      to + ids_.ids_offset());
      int64_t offset_to = std::distance(ids_.ids().begin(), iter_to);
      int64_t id = from;
      const int64_t* ids = ids_.ids().begin();
      DenseUtil::Iterate(
          [&](int64_t offset, bool valid, view_type_t<Ts>... args) {
            int64_t new_id = ids[offset] - ids_.ids_offset();
            if (id < new_id) defaultFn(id, new_id - id);
            if (valid) {
              fn(new_id, args...);
            } else {
              missing_fn(new_id, 1);
            }
            id = new_id + 1;
          },
          offset_from, offset_to, std::get<Is>(dense_)...);
      if (id < to) defaultFn(id, to - id);
    }
  }

  template <class Fn, size_t... Is>
  void IterateSimple(Fn&& fn, std::index_sequence<Is...>) const {
    if (ids_.type() == IdFilter::kFull) {
      DenseUtil::IterateFromZero(
          [&](int64_t id, bool valid, view_type_t<Ts>... args) {
            if (valid) fn(id, args...);
          },
          size_, std::get<Is>(dense_)...);
      return;
    }
    DCHECK(!ConvertToDense) << "If ConvertToDense=true, `ids_` must be full";
    if constexpr (!ConvertToDense) {
      int64_t id = 0;
      const int64_t* ids = ids_.ids().begin();
      DenseUtil::IterateFromZero(
          [&](int64_t offset, bool valid, view_type_t<Ts>... args) {
            int64_t new_id = ids[offset] - ids_.ids_offset();
            if (default_valid_ && id < new_id) {
              while (id < new_id) {
                fn(id++, std::get<Is>(default_values_)...);
              }
            }
            if (valid) fn(new_id, args...);
            id = new_id + 1;
          },
          ids_.ids().size(), std::get<Is>(dense_)...);
      if (default_valid_) {
        while (id < size_) {
          fn(id++, std::get<Is>(default_values_)...);
        }
      }
    }
  }

  // `Arg` is one of `Ts...` and `arg` is the Array corresponding to it.
  // Returns true if arg.id_filter() is an upper bound of valid ids for
  // iteration.
  template <class Arg, class A>
  static bool BoundsValidIds(const A& arg) {
    return !is_optional_v<Arg> && !arg.HasMissingIdValue();
  }

  template <class A, class... As>
  static bool IsSameIdFilter(const A& a, const As&... as) {
    return ((a.id_filter().IsSame(as.id_filter()) && ... && true));
  }

  template <class A, class... As>
  static const A& First(const A& a, const As&...) {
    return a;
  }

  template <class To, class From>
  static const To& MaybeUnwrapOptional(const OptionalValue<From>& v) {
    if constexpr (!is_optional_v<To>) {
      DCHECK(v.present);
      return v.value;
    } else {
      return v;
    }
  }

  int64_t size_;                   // Size of the Arrays we iterate on.
  IdFilter ids_{IdFilter::kFull};  // List of ids we iterate on.

  // WithIds(ids_).dense_data for each argument.
  std::tuple<AsDenseArray<Ts>...> dense_;

  // If true, then all ids that are not included to `ids_` are valid. For such
  // ids `fn`/`repreated_fn` should be called with `default_values_`.
  bool default_valid_;
  std::tuple<Ts...> default_values_;  // missing_id_value of all Arrays
};

template <bool ConvertToDense>
class ArrayOpsUtil<ConvertToDense, meta::type_list<>> {
 public:
  explicit ArrayOpsUtil(int64_t size, RawBufferFactory* = nullptr)
      : size_(size) {}

  template <class Fn, class RepeatedFn, class MissedFn>
  void Iterate(int64_t from, int64_t to, Fn&&, MissedFn&&,
               RepeatedFn&& repeated_fn) const {
    repeated_fn(from, to - from);
  }

  template <class Fn, class MissedFn>
  void Iterate(int64_t from, int64_t to, Fn&& fn, MissedFn&&) const {
    for (int64_t i = from; i < to; ++i) fn(i);
  }

  template <class Fn>
  void Iterate(int64_t from, int64_t to, Fn&& fn) const {
    for (int64_t i = from; i < to; ++i) fn(i);
  }

  template <class Fn>
  void IterateSimple(Fn&& fn) const {
    for (int64_t i = 0; i < size_; ++i) fn(i);
  }

  int64_t size() const { return size_; }
  int64_t PresentCountUpperEstimate() const { return size_; }

 private:
  int64_t size_;
};

template <class OptionalityList, class TypeList>
struct ApplyOptionalityToTypes;

template <class... Os, class... Ts>
struct ApplyOptionalityToTypes<meta::type_list<Os...>, meta::type_list<Ts...>> {
  using types =
      meta::type_list<std::conditional_t<is_optional_v<Os>,
                                         ::arolla::OptionalValue<Ts>, Ts>...>;
};

}  // namespace arolla::array_ops_internal

namespace arolla {

// Iterating over several Arrays of the same size with a callback.
// `fn` is called with the arguments (int64_t id, X<Ts>... args), where
// X<T> is either view_type_t<T> (required argument) or
// OptionalValue<view_type_t<T>> (optional argument). Optionality of arguments
// is deduced from the signature of `fn`.
// `fn` is called once for every row where all required arguments are present.
template <class Fn, class T, class... Ts>
void ArraysIterate(Fn&& fn, const Array<T>& first_array,
                   const Array<Ts>&... arrays) {
  using FnTraits = meta::function_traits<std::decay_t<Fn>>;
  static_assert(std::is_same_v<typename FnTraits::return_type, void>,
                "Callback shouldn't return value");
  static_assert(FnTraits::arity == sizeof...(arrays) + 2);
  using arg_list = typename array_ops_internal::ApplyOptionalityToTypes<
      meta::tail_t<typename FnTraits::arg_types>,
      meta::type_list<T, Ts...>>::types;
  array_ops_internal::ArrayOpsUtil<false, arg_list> util(
      first_array.size(), first_array, arrays...);
  util.IterateSimple(std::forward<Fn>(fn));
}

// A variation of `ArraysIterate` that converts all Arrays to dense form
// before iterating. It is less efficient if Arrays are very sparse, but it
// produces less code and sometimes can be better inlined.
template <class Fn, class T, class... Ts>
void ArraysIterateDense(Fn&& fn, const Array<T>& first_array,
                        const Array<Ts>&... arrays) {
  using FnTraits = meta::function_traits<std::decay_t<Fn>>;
  static_assert(std::is_same_v<typename FnTraits::return_type, void>,
                "Callback shouldn't return value");
  static_assert(FnTraits::arity == sizeof...(arrays) + 2);
  using arg_list = typename array_ops_internal::ApplyOptionalityToTypes<
      meta::tail_t<typename FnTraits::arg_types>,
      meta::type_list<T, Ts...>>::types;
  array_ops_internal::ArrayOpsUtil<true, arg_list> util(first_array.size(),
                                                        first_array, arrays...);
  util.IterateSimple(std::forward<Fn>(fn));
}

}  // namespace arolla

#endif  // AROLLA_ARRAY_OPS_UTIL_H_
