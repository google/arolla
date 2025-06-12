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
#ifndef AROLLA_QEXPR_OPERATORS_BOOL_COMPARISON_H_
#define AROLLA_QEXPR_OPERATORS_BOOL_COMPARISON_H_

#include <type_traits>

namespace arolla {

// bool.equal
struct EqualOp {
  using run_on_missing = std::true_type;
  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return lhs == rhs;
  }
};

// bool.not_equal
struct NotEqualOp {
  using run_on_missing = std::true_type;
  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return lhs != rhs;
  }
};

// bool.less
struct LessOp {
  using run_on_missing = std::true_type;
  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return lhs < rhs;
  }
};

// bool.less_equal
struct LessEqualOp {
  using run_on_missing = std::true_type;
  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return lhs <= rhs;
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_BOOL_COMPARISON_H_
