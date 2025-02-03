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
#include "arolla/expr/operators/dynamic_lifting.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/restricted_operator.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/overloaded_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOverloadedOperator;
using ::arolla::expr::Placeholder;
using ::arolla::expr_operators::type_meta::QTypes;

// MetaEval strategy that successfully evaluates if there are no array
// arguments.
absl::StatusOr<QTypes> NoArrayArgs(absl::Span<const QTypePtr> types) {
  for (QTypePtr t : types) {
    if (IsArrayLikeQType(t)) {
      return absl::InvalidArgumentError("array argument found");
    }
  }
  return QTypes{types.begin(), types.end()};
}

}  // namespace

absl::StatusOr<ExprOperatorPtr> LiftDynamically(
    const absl::StatusOr<ExprOperatorPtr>& op_or) {
  ASSIGN_OR_RETURN(const ExprOperatorPtr& op, op_or);
  ASSIGN_OR_RETURN(ExprOperatorPtr map_op, expr::LookupOperator("core.map"));
  return MakeOverloadedOperator(
      op->display_name(), RestrictOperator(op, NoArrayArgs),
      MakeLambdaOperator(
          ExprOperatorSignature::Make("*args"),
          ::arolla::expr::CallOp(
              "core.apply_varargs",
              {Literal(std::move(map_op)), Literal(op), Placeholder("args")})));
}

}  // namespace arolla::expr_operators
