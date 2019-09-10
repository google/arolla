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
#ifndef AROLLA_QEXPR_OPERATORS_BOOL_LOGIC_H_
#define AROLLA_QEXPR_OPERATORS_BOOL_LOGIC_H_

#include <type_traits>

#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"

namespace arolla {

// Ternary And.
// Returns True if all args are True. Returns False if at least one arg is
// False. Otherwise returns missing.
struct LogicalAndOp {
  using run_on_missing = std::true_type;

  bool operator()(bool lhs, bool rhs) const { return lhs && rhs; }

  OptionalValue<bool> operator()(const OptionalValue<bool>& lhs,
                                 const OptionalValue<bool>& rhs) const {
    if (lhs.present) {
      return !lhs.value ? false : rhs;
    } else if (rhs.present) {
      return !rhs.value ? false : lhs;
    } else {
      return OptionalValue<bool>{};
    }
  }
};

// Ternary Or. Returns:
// * True if at least one arg is True.
// * False if all args are False
// * Missing otherwise.
struct LogicalOrOp {
  using run_on_missing = std::true_type;

  bool operator()(bool lhs, bool rhs) const { return lhs || rhs; }

  OptionalValue<bool> operator()(const OptionalValue<bool>& lhs,
                                 const OptionalValue<bool>& rhs) const {
    if (lhs.present) {
      return lhs.value ? true : rhs;
    } else if (rhs.present) {
      return rhs.value ? true : lhs;
    } else {
      // Both inputs are missing. Should we return lhs here?
      return OptionalValue<bool>{};
    }
  }
};

// bool.logical_not operator returns not(arg) if arg is present, missing
// otherwise.
struct LogicalNotOp {
  using run_on_missing = std::true_type;
  bool operator()(bool arg) const { return !arg; }
};

// bool.logical_if operator. Depending on whether the first argument is
// true/false/missing returns the second/third/fourth argument correspondingly.
struct LogicalIfOp {
  using run_on_missing = std::true_type;

  template <typename T, typename = std::enable_if_t<is_scalar_type_v<T>>>
  const T& operator()(const OptionalValue<bool>& condition, const T& true_value,
                      const T& false_value, const T& missing_value) const {
    if (condition.present) {
      return condition.value ? true_value : false_value;
    } else {
      return missing_value;
    }
  }

  template <typename T>
  OptionalValue<T> operator()(const OptionalValue<bool>& condition,
                              const OptionalValue<T>& true_value,
                              const OptionalValue<T>& false_value,
                              const OptionalValue<T>& missing_value) const {
    if (condition.present) {
      return condition.value ? true_value : false_value;
    } else {
      return missing_value;
    }
  }
  // Specialization for DenseArray condition and scalar values. This pattern is
  // pretty common and broadcasting true_/false_/missing_value to match
  // condition shape appears can be too expensive.
  template <typename T>
  DenseArray<T> operator()(EvaluationContext* ctx,
                           const DenseArray<bool>& condition,
                           const OptionalValue<T>& true_value,
                           const OptionalValue<T>& false_value,
                           const OptionalValue<T>& missing_value) const {
    auto fn = [&true_value, &false_value, &missing_value](
                  OptionalValue<bool> condition) -> OptionalValue<T> {
      return LogicalIfOp()(condition, true_value, false_value, missing_value);
    };
    auto op = CreateDenseOp<DenseOpFlags::kRunOnMissing, decltype(fn), T>(
        fn, &ctx->buffer_factory());
    return op(condition);
  }

  // Specialization for arguments passed as lambdas.
  template <typename TrueFn, typename FalseFn, typename MissingFn,
            std::enable_if_t<std::is_invocable_v<TrueFn> ||
                                 std::is_invocable_v<FalseFn> ||
                                 std::is_invocable_v<MissingFn>,
                             bool> = true>
  auto operator()(const OptionalValue<bool>& condition,
                  const TrueFn& true_value, const FalseFn& false_value,
                  const MissingFn& missing_value) const {
    auto unwrap = [](const auto& fn) {
      if constexpr (std::is_invocable_v<decltype(fn)>) {
        return fn();
      } else {
        return fn;
      }
    };
    return condition.present
               ? (condition.value ? unwrap(true_value) : unwrap(false_value))
               : unwrap(missing_value);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_BOOL_LOGIC_H_
