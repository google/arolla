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
#ifndef AROLLA_QEXPR_OPERATORS_BITWISE_BITWISE_H_
#define AROLLA_QEXPR_OPERATORS_BITWISE_BITWISE_H_

#include <type_traits>

// See go/rl2-arithmetic for more details on bitwise operators.
namespace arolla {

// Computes bitwise AND between two integral numbers.
struct BitwiseAndOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T lhs, T rhs) const {
    return lhs & rhs;
  }
};

// Computes bitwise OR between two integral numbers.
struct BitwiseOrOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T lhs, T rhs) const {
    return lhs | rhs;
  }
};

// Computes bitwise XOR between two integral numbers.
struct BitwiseXorOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T lhs, T rhs) const {
    return lhs ^ rhs;
  }
};

// Computes bitwise NOT of an integral number.
struct InvertOp {
  using run_on_missing = std::true_type;
  template <typename T>
  T operator()(T x) const {
    return ~x;
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_BITWISE_BITWISE_H_
