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
#ifndef AROLLA_EXPR_OPERATORS_REGISTRATION_H_
#define AROLLA_EXPR_OPERATORS_REGISTRATION_H_

#include <type_traits>  // IWYU pragma: keep

#include "absl/base/no_destructor.h"      // IWYU pragma: keep
#include "absl/status/status.h"           // IWYU pragma: keep
#include "absl/status/statusor.h"         // IWYU pragma: keep
#include "arolla/expr/expr.h"            // IWYU pragma: keep
#include "arolla/expr/expr_operator.h"   // IWYU pragma: keep
#include "arolla/util/status_macros_backport.h"

// Tools for operators registration.

namespace arolla::expr_operators {

// Declares Expr operator registration function with the provided name.
// Example:
//    AROLLA_DECLARE_EXPR_OPERATOR(MathAdd);
// declares
//   - RegisterMathAdd() function that registers an operator during the first
//   call.
//   - GetMathAdd() function that registers an operator (if needed) and returns
//   a registered operator pointing to it.
//   - MathAdd(args...) function that calls the operator with a guarantee that
//   it is already registered.
//
#define AROLLA_DECLARE_EXPR_OPERATOR(op_function_name)                        \
  absl::StatusOr<::arolla::expr::ExprOperatorPtr> Get##op_function_name();    \
  inline absl::Status Register##op_function_name() {                          \
    return Get##op_function_name().status();                                  \
  }                                                                           \
  template <typename... Args>                                                 \
  std::enable_if_t<(std::is_convertible_v<                                    \
                        Args, absl::StatusOr<::arolla::expr::ExprNodePtr>> && \
                    ...),                                                     \
                   absl::StatusOr<::arolla::expr::ExprNodePtr>>               \
  op_function_name(Args... args) {                                            \
    ASSIGN_OR_RETURN(auto op, Get##op_function_name());                       \
    return ::arolla::expr::CallOp(op, {args...});                             \
  }

// Defines Expr operator registration function with the provided name and body.
// The registration_call must contain a call to RegisterOperator (or similar)
// function and return StatusOr<RegisteredOperatorPtr>.
// Example:
//   AROLLA_DEFINE_EXPRE_OPERATOR(MathAdd, RegisterBackendOperator("math.add"));
// defines RegisterMathAdd and MathAdd functions.
//
#define AROLLA_DEFINE_EXPR_OPERATOR(op_function_name, registration_call)    \
  absl::StatusOr<::arolla::expr::ExprOperatorPtr> Get##op_function_name() { \
    static const absl::NoDestructor<                                        \
        absl::StatusOr<::arolla::expr::ExprOperatorPtr>>                    \
        registered(registration_call);                                      \
    return *registered;                                                     \
  }

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_REGISTRATION_H_
