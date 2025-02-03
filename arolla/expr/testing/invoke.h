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
#ifndef AROLLA_EXPR_TESTING_INVOKE_H_
#define AROLLA_EXPR_TESTING_INVOKE_H_

// IWYU pragma: private, include "arolla/expr/testing/testing.h"

#include <array>
#include <string>
#include <tuple>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::testing {

// Keyword, Value pair for invoking operators accepting keyword arguments.
struct KeywordArg {
  KeywordArg(std::string keyword, TypedValue value)
      : keyword(std::move(keyword)), value(std::move(value)) {}

  template <typename ValueT>
  KeywordArg(std::string keyword, ValueT&& value)
      : keyword(std::move(keyword)),
        value(TypedValue::FromValue(std::forward<ValueT>(value))) {}

  std::string keyword;
  TypedValue value;
};

// Constructs an expression with the given Expr operator and invokes it on the
// given inputs using QExpr backend.
absl::StatusOr<TypedValue> InvokeExprOperator(
    const expr::ExprOperatorPtr& op, absl::Span<const TypedValue> args,
    absl::Span<const KeywordArg> kwargs = {});

// Constructs an expression with the given Expr operator and invokes it on the
// given inputs using QExpr backend.
absl::StatusOr<TypedValue> InvokeExprOperator(
    absl::string_view op_name, absl::Span<const TypedValue> args,
    absl::Span<const KeywordArg> kwargs = {});

// Constructs an expression with the given Expr operator and invokes it on the
// given inputs using QExpr backend, converts the result to ResultT. Arguments
// wrapped in KeywordArg are passed to the operator as keyword arguments.
template <typename ResultT, typename... ArgTs>
absl::StatusOr<ResultT> InvokeExprOperator(const expr::ExprOperatorPtr& op,
                                           const ArgTs&... args) {
  constexpr auto wrap_arg = [](const auto& arg) -> TypedValue {
    if constexpr (std::is_same_v<decltype(arg), const TypedValue&>) {
      return arg;
    } else {
      return TypedValue::FromValue(arg);
    }
  };

  auto arg_types = {std::is_same_v<ArgTs, KeywordArg>...};
  if (!std::is_sorted(arg_types.begin(), arg_types.end())) {
    return absl::InvalidArgumentError(
        "Keyword arguments must follow positional arguments");
  }

  // Build tuple of positional args wrapped with TypedValues.
  auto pos_args_tuple = std::tuple_cat([&wrap_arg](const ArgTs& arg) {
    if constexpr (std::is_same_v<ArgTs, KeywordArg>) {
      return std::tuple<>();
    } else {
      return std::tuple<TypedValue>(wrap_arg(arg));
    }
  }(args)...);

  // Convert tuple into array.
  auto pos_args_arr = std::apply(
      [](auto&&... pos_args) {
        return std::array<TypedValue, sizeof...(pos_args)>{pos_args...};
      },
      std::move(pos_args_tuple));

  // Build tuple of keyword args.
  auto kw_args_tuple = std::tuple_cat([&wrap_arg](const ArgTs& arg) {
    if constexpr (std::is_same_v<ArgTs, KeywordArg>) {
      return std::make_tuple(arg);
    } else {
      return std::tuple<>();
    }
  }(args)...);

  // Convert tuple into array.
  auto kw_args_arr = std::apply(
      [](auto&&... kw_args) {
        return std::array<KeywordArg, sizeof...(kw_args)>{kw_args...};
      },
      std::move(kw_args_tuple));

  // Invoke the operator on the positional and keyword arguments
  ASSIGN_OR_RETURN(TypedValue result,
                   InvokeExprOperator(op, pos_args_arr, kw_args_arr));

  if constexpr (std::is_same_v<ResultT, TypedValue>) {
    return result;
  } else {
    // Drop std::reference_wrapper.
    ASSIGN_OR_RETURN(ResultT result_value, result.As<ResultT>());
    return result_value;
  }
}

// Constructs an expression with the given Expr operator and invokes it on the
// given inputs using QExpr backend, converts the result to ResultT.
template <typename ResultT, typename... ArgTs>
absl::StatusOr<ResultT> InvokeExprOperator(absl::string_view op_name,
                                           const ArgTs&... args) {
  ASSIGN_OR_RETURN(auto op, expr::LookupOperator(op_name));
  return InvokeExprOperator<ResultT>(op, args...);
}

}  // namespace arolla::testing

#endif  // AROLLA_EXPR_TESTING_INVOKE_H_
