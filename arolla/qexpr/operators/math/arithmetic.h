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
#ifndef AROLLA_OPERATORS_MATH_ARITHMETIC_H_
#define AROLLA_OPERATORS_MATH_ARITHMETIC_H_

#include <cmath>
#include <limits>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/memory/optional_value.h"

namespace arolla {

// Returns sign of the value:
// -1 for negative numbers
//  0 for 0
//  1 for positive numbers
//  nan for nan
struct SignOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T x) const {
    return std::isnan(x) ? x : ((x > 0) - (x < 0));
  }
};

struct AddOp {
  using run_on_missing = std::true_type;

  template <typename T, typename... Ts,
            std::enable_if_t<(std::is_same_v<T, Ts> && ...), bool> = true>
  T operator()(T arg1, Ts... args) const {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      // Use unsigned type to mitigate signed integer overflow UB.
      using UT = std::make_unsigned_t<T>;
      return static_cast<T>(
          (static_cast<UT>(arg1) + ... + static_cast<UT>(args)));
    } else {
      return (arg1 + ... + args);
    }
  }
};

struct SubtractOp {
  using run_on_missing = std::true_type;

  template <typename T>
  T operator()(T lhs, T rhs) const {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      // Use unsigned type to mitigate signed integer overflow UB.
      using UT = std::make_unsigned_t<T>;
      return static_cast<T>(static_cast<UT>(lhs) - static_cast<UT>(rhs));
    } else {
      return lhs - rhs;
    }
  }
};

struct MultiplyOp {
  using run_on_missing = std::true_type;

  template <typename T>
  T operator()(T lhs, T rhs) const {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      // Use unsigned type to mitigate signed integer overflow UB.
      using UT = std::make_unsigned_t<T>;
      return static_cast<T>(static_cast<UT>(lhs) * static_cast<UT>(rhs));
    } else {
      return lhs * rhs;
    }
  }
};

// Returns the integral part of the quotient rounded towards negative infinity.
// Unspecified behavour if the result cannot be represented.
struct FloorDivOp {
  template <typename T>
  absl::StatusOr<T> operator()(T lhs, T rhs) const {
    if (rhs == 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "division by zero");
    }
    if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_unsigned_v<T>) {
        return lhs / rhs;
      } else {
        // In C++ `operator/` for integers works as TruncDiv, so for signed
        // integers we have to implement FloorDiv manually. We also have to pay
        // attention to signed integer overflow UB.
        using UT = std::make_unsigned_t<T>;
        if ((lhs ^ rhs) >= 0) {  // if have the same sign
          return rhs != -1 ? lhs / rhs
                           : static_cast<T>(-static_cast<UT>(
                                 lhs));  // mitigate INT_MIN / -1
        }
        return static_cast<T>(static_cast<UT>(lhs / rhs) -
                              static_cast<UT>((lhs % rhs) != 0));
      }

    } else {
      if (const auto div = lhs / rhs; div != 0) {
        return std::isfinite(div)
                   ? std::floor(div)                       // a regular case
                   : std::numeric_limits<T>::quiet_NaN();  // div is nan or inf
      } else {
        return (lhs && std::signbit(div)) ? -1.0  // div is "virtually" -eps
                                          : div;  // div is 0
      }
    }
  }
};

// Modulo operator, consistent with FloorDiv operator:
//   x = FloorDiv(x, y) * y + Mod(x, y)
struct ModOp {
  template <typename T>
  absl::StatusOr<T> operator()(T lhs, T rhs) const {
    if (rhs == 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "division by zero");
    }
    if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_unsigned_v<T>) {
        return lhs % rhs;
      } else {
        if ((lhs ^ rhs) >= 0) {              // if have the same sign
          return rhs != -1 ? lhs % rhs : 0;  // mitigate INT_MIN / -1
        }
        using UT = std::make_unsigned_t<T>;
        return static_cast<T>(static_cast<UT>(rhs) +
                              static_cast<UT>(lhs % rhs)) %
               rhs;
      }
    } else {
      const auto div = lhs / rhs;
      if (std::isfinite(div)) {
        T ret;
        if (div != 0) {
          ret = lhs - std::floor(div) * rhs;
        } else {
          ret = (lhs == 0 || (lhs > 0) == (rhs > 0))
                    ? lhs
                    : std::numeric_limits<T>::infinity();
        }
        return std::copysign(ret, rhs);
      } else {
        return std::numeric_limits<T>::quiet_NaN();
      }
    }
  }
};

// True division. Division by 0 returns +/-infinity.
struct DivideOp {
  float operator()(float lhs, float rhs) const { return lhs / rhs; }
  double operator()(double lhs, double rhs) const { return lhs / rhs; }
};

struct FmodOp {
  float operator()(float lhs, float rhs) const { return std::fmod(lhs, rhs); }
  double operator()(double lhs, double rhs) const {
    return std::fmod(lhs, rhs);
  }
};

struct FloorOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T x) const {
    return std::floor(x);
  }
};

struct CeilOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T x) const {
    return std::ceil(x);
  }
};

struct PosOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T a) const {
    return a;
  }
};

struct NegOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T a) const {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      // Use unsigned type to mitigate signed integer overflow UB.
      using UT = std::make_unsigned_t<T>;
      return static_cast<T>(-static_cast<UT>(a));
    } else {
      return -a;
    }
  }
};

struct AbsOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T a) const {
    if constexpr (!std::is_signed_v<T>) {
      return a;
    } else if constexpr (!std::is_integral_v<T>) {
      return std::fabs(a);
    } else {
      if (a == std::numeric_limits<T>::min()) {
        // Suppress integer overflow UB.
        return std::numeric_limits<T>::min();
      } else {
        return std::abs(a);
      }
    }
  }
};

struct RoundOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T arg) const {
    return std::round(arg);
  }
};

// math.max operator returns maximum of the two given numbers.
struct MaxOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(const T& lhs, const T& rhs) const {
    return (std::isnan(lhs) || lhs >= rhs) ? lhs : rhs;
  }
};

// math.min operator returns minimum of the two given numbers.
struct MinOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(const T& lhs, const T& rhs) const {
    return (std::isnan(lhs) || lhs <= rhs) ? lhs : rhs;
  }
};

struct IsInfOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(T x) const {
    return OptionalUnit{std::isinf(x)};
  }
};

struct IsFiniteOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(T x) const {
    return OptionalUnit{std::isfinite(x)};
  }
};

struct IsNanOp {
  using run_on_missing = std::true_type;
  template <typename T>
  OptionalUnit operator()(T x) const {
    return OptionalUnit{std::isnan(x)};
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_MATH_ARITHMETIC_H_
