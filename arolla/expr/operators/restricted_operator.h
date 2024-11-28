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
#ifndef AROLLA_EXPR_OPERATORS_RESTRICTED_OPERATOR_H_
#define AROLLA_EXPR_OPERATORS_RESTRICTED_OPERATOR_H_

#include "absl//status/statusor.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"

namespace arolla::expr_operators {

// Restrict `wrapped_op` operator to the types accepted by `restriction`
// strategy.
//
// The resulting operator will behave similarly to `wrapped_op`, unless
// `restriction` is evaluated to an error. In this case the error will be
// forwarded out of InferAttributes or ToLower. The operator requires all the
// input types to be available for InferAttributes and ToLower, even if
// `wrapped_op` can operate on incomplete types.
//
// The version accepting StatusOr is a convenience wrapper that just forwards
// the incoming error to the resulting status.
//
expr::ExprOperatorPtr RestrictOperator(expr::ExprOperatorPtr wrapped_op,
                                       type_meta::Strategy restriction);
absl::StatusOr<expr::ExprOperatorPtr> RestrictOperator(
    absl::StatusOr<expr::ExprOperatorPtr> wrapped_op,
    absl::StatusOr<type_meta::Strategy> restriction);

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_RESTRICTED_OPERATOR_H_
