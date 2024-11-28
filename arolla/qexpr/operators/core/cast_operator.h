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
#ifndef AROLLA_OPERATORS_CORE_CAST_OPERATOR_H_
#define AROLLA_OPERATORS_CORE_CAST_OPERATOR_H_

#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_cat.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"

namespace arolla {

// Cast operator, that converts any type to the requested using static_cast.
template <typename DST>
struct CastOp {
  using run_on_missing = std::true_type;
  using DstTypes =
      meta::type_list<bool, int32_t, int64_t, uint64_t, float, double>;

  using SrcTypes =
      meta::type_list<bool, int32_t, int64_t, uint64_t, float, double>;

  static_assert(meta::contains_v<DstTypes, DST>);

  // Returns the maximal value of SRC type, that can be safely casted to DST.
  //
  // The formula for the value:
  //
  //   2**int_precision - 2**(int_precision - float_precision)
  //
  template <typename SRC>
  static constexpr SRC max_float_to_int_safe_value() {
    using dst_limits = std::numeric_limits<DST>;
    using src_limits = std::numeric_limits<SRC>;
    static_assert(dst_limits::is_integer);
    static_assert(std::is_floating_point_v<SRC>);
    SRC result = 0;
    int i = 0;
    for (; i < src_limits::digits; ++i) {
      result *= 2;
      result += 1;
    }
    for (; i < dst_limits::digits; ++i) {
      result *= 2;
    }
    for (; i > dst_limits::digits; --i) {
      result /= 2;
    }
    return result;
  }

  // Returns the maximal value of SRC type, that can be safely casted to DST.
  //
  // The formula for the value:
  //
  //   -(2**int_precision + 1 - 2**min(0, int_precision - float_precision))
  //
  template <typename SRC>
  static constexpr SRC min_float_to_int_safe_value() {
    using dst_limits = std::numeric_limits<DST>;
    using src_limits = std::numeric_limits<SRC>;
    static_assert(dst_limits::is_integer);
    static_assert(std::is_floating_point_v<SRC>);
    if constexpr (!dst_limits::is_signed) {
      return 0.0;
    } else {
      SRC result = 1;
      int i = 0;
      for (; i < src_limits::digits; ++i) {
        result *= 2;
      }
      for (; i < dst_limits::digits; ++i) {
        result *= 2;
      }
      for (; i > dst_limits::digits; --i) {
        result += 1;
        result /= 2;
      }
      return -result;
    }
  }

  // Returns a tuple<SRC, SRC>(min_value, max_value)
  // that is safe for the source value.
  // Returns a tuple(), if all source values are safe.
  template <typename SRC>
  static constexpr auto safe_range() {
    static_assert(meta::contains_v<SrcTypes, SRC>);
    using dst_limits = std::numeric_limits<DST>;
    using src_limits = std::numeric_limits<SRC>;
    if constexpr (std::is_same_v<SRC, DST>) {
      // No extra checks.
      return std::make_tuple();
    } else if constexpr (std::is_integral_v<DST> && std::is_integral_v<SRC>) {
      // safe_min and safe_max are always fit to both SRC and DST.
      constexpr SRC safe_min =
          std::max<int64_t>(dst_limits::min(), src_limits::min());
      constexpr SRC safe_max =
          std::min<uint64_t>(dst_limits::max(), src_limits::max());
      // [safe_min, safe_max] is intersection of SRC and DST ranges.
      if constexpr (safe_min <= src_limits::min() &&
                    safe_max >= src_limits::max()) {
        // Casting to wider type.
        // No extra checks.
        return std::make_tuple();
      } else {
        // Casting from a wide integer type to a narrow integer type.
        return std::tuple<SRC, SRC>(safe_min, safe_max);
      }
    } else if constexpr (std::is_integral_v<DST> &&
                         std::is_floating_point_v<SRC>) {
      // Casting from a floating point type to an integer type.
      return std::tuple<SRC, SRC>(min_float_to_int_safe_value<SRC>(),
                                  max_float_to_int_safe_value<SRC>());
    } else if constexpr (std::is_floating_point_v<DST> &&
                         std::is_floating_point_v<SRC>) {
      // According to the C++ standard, casting between floating-point types can
      // have UB if the source value is out-of-range for the destination type.
      // But we can't find a practical case when it happens.
      //
      // Here we have a sanity test -- if static_cast<dst>(src_max) within a
      // constexpr initialization triggers UB, the compiler has to complain.
      constexpr bool ub_check =
          (src_limits::max() <= dst_limits::max() ||
           static_cast<DST>(src_limits::max()) == dst_limits::max() ||
           static_cast<DST>(src_limits::max()) == dst_limits::infinity());
      static_assert(ub_check);
      return std::make_tuple();
    } else {
      // Otherwise no extra checks.
      return std::make_tuple();
    }
  }

  template <typename SRC>
  auto operator()(SRC src) const {
    constexpr auto src_range = safe_range<SRC>();
    if constexpr (std::tuple_size_v<decltype(src_range)> == 0) {
      // All source values are safe.
      return static_cast<DST>(src);
    } else {
      // In C++, some type casts have undefined/unspecified behaviour. In
      // particular, if the target integer type cannot represent the result.
      // For that reason, we do extra checks to make the behaviour stable.
      using ReturnType = absl::StatusOr<DST>;
      const auto& [range_min, range_max] = src_range;
      if (range_min <= src && src <= range_max) {
        return ReturnType(static_cast<DST>(src));
      } else {
        return ReturnType(absl::InvalidArgumentError(absl::StrCat(
            "cannot cast ", ::arolla::Repr(src), " to ",
            std::is_unsigned_v<DST> ? "u" : "", "int", 8 * sizeof(DST))));
      }
    }
  }
};

// core.to_bool operator.
struct ToBoolOp {
  using run_on_missing = std::true_type;

  template <typename T>
  bool operator()(const T& x) const {
    return x != 0;
  }
};

// Cast operator, that converts any type T to OptionalValue<T>.
struct ToOptionalOp {
  using run_on_missing = std::true_type;

  template <typename T>
  OptionalValue<T> operator()(const T& x) const {
    return OptionalValue<T>(x);
  }
};

// `core._get_optional_value` converts any type OptionalValue<T> to T.
// Returns error if value is missing.
struct GetOptionalValueOp {
  template <typename T>
  absl::StatusOr<T> operator()(const OptionalValue<T>& x) const {
    if (!x.present) {
      return absl::FailedPreconditionError(
          "core.get_optional_value expects present value, got missing");
    }
    return x.value;
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_CORE_CAST_OPERATOR_H_
