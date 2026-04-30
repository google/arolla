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
#include "arolla/expr/operator_loader/qtype_inference.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/helper.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::MakeTupleOperator;
using ::arolla::expr::ToDebugString;

absl::StatusOr<ExprNodePtr> PrepareQTypeInferenceExpr(
    const ExprOperatorSignature& signature,
    absl::Span<const QTypeConstraint> qtype_constraints,
    ExprNodePtr qtype_inference_expr) {
  std::vector<ExprNodePtr> prepared_exprs;
  prepared_exprs.reserve(2 * qtype_constraints.size() + 2);
  for (const auto& constraint : qtype_constraints) {
    ASSIGN_OR_RETURN((auto [resolved_expr, readiness_expr]),
                     ResolvePlaceholders(signature, constraint.predicate_expr,
                                         GetQType<OptionalUnit>()),
                     std::move(_).SetPrepend()
                         << "problem with a qtype constraint: "
                         << ToDebugString(constraint.predicate_expr) << "; ");
    prepared_exprs.push_back(std::move(readiness_expr));
    prepared_exprs.push_back(std::move(resolved_expr));
  }
  {
    ASSIGN_OR_RETURN(
        (auto [resolved_expr, readiness_expr]),
        ResolvePlaceholders(signature, qtype_inference_expr, GetQTypeQType()),
        std::move(_).SetPrepend()
            << "problem with a qtype inference expression: "
            << ToDebugString(qtype_inference_expr) << "; ");
    prepared_exprs.push_back(std::move(readiness_expr));
    prepared_exprs.push_back(std::move(resolved_expr));
  }
  return MakeOpNode(MakeTupleOperator::Make(), std::move(prepared_exprs));
}

}  // namespace

absl::StatusOr<QTypeInferenceFn absl_nonnull> MakeQTypeInferenceFn(
    const ExprOperatorSignature& signature,
    absl::Span<const QTypeConstraint> qtype_constraints,
    const ExprNodePtr absl_nonnull& qtype_inference_expr) {
  ASSIGN_OR_RETURN(auto prepared_qtype_inference_expr,
                   PrepareQTypeInferenceExpr(signature, qtype_constraints,
                                             qtype_inference_expr));
  std::vector<std::string> error_messages;
  error_messages.reserve(qtype_constraints.size());
  for (const auto& constraint : qtype_constraints) {
    error_messages.emplace_back(constraint.error_message);
  }
  auto accessor = [](const Sequence& input_qtype_sequence, absl::string_view,
                     WildcardInputLoaderCallback callback) -> absl::Status {
    DCHECK_EQ(input_qtype_sequence.value_qtype(), GetQTypeQType());
    return callback(input_qtype_sequence);
  };
  ASSIGN_OR_RETURN(
      auto input_loader,
      WildcardInputLoader<Sequence>::BuildFromCallbackAccessorFn(
          accessor, {{"input_qtype_sequence", GetInputQTypeSequenceQType()}}));
  ASSIGN_OR_RETURN(
      auto model_executor,
      CompileModelExecutor<TypedValue>(std::move(prepared_qtype_inference_expr),
                                       *input_loader));
  return [model_executor = std::move(model_executor),
          error_messages = std::move(error_messages),
          signature = signature](const Sequence& input_qtype_sequence)
             -> absl::StatusOr<QTypePtr absl_nullable> {
    ASSIGN_OR_RETURN(auto result,
                     model_executor.ExecuteOnHeap(
                         /*eval_options=*/{}, input_qtype_sequence));
    const size_t num_constraints = error_messages.size();
    DCHECK_EQ(result.GetFieldCount(), 2 * num_constraints + 2);
    bool all_constraints_ready = true;
    for (size_t i = 0; i < num_constraints; ++i) {
      const bool constraint_ready =
          result.GetField(2 * i).UnsafeAs<OptionalUnit>().present;
      if (!constraint_ready) {
        all_constraints_ready = false;
        continue;
      }
      const bool constraint_satisfied =
          result.GetField(2 * i + 1).UnsafeAs<OptionalUnit>().present;
      if (!constraint_satisfied) {
        ASSIGN_OR_RETURN(
            auto parameter_qtypes,
            ExtractParameterQTypes(signature, input_qtype_sequence));
        return absl::InvalidArgumentError(
            FormatParameterQTypes(error_messages[i], parameter_qtypes));
      }
    }
    const bool output_qtype_ready =
        (all_constraints_ready &&
         result.GetField(2 * num_constraints).UnsafeAs<OptionalUnit>());
    if (!output_qtype_ready) {
      return nullptr;
    }
    const QTypePtr output_qtype =
        result.GetField(2 * num_constraints + 1).UnsafeAs<QTypePtr>();
    DCHECK(output_qtype != nullptr);
    if (output_qtype == nullptr || output_qtype == GetNothingQType()) {
      return absl::FailedPreconditionError(
          "operator attribute inference failure: qtype inference expression"
          " produced no qtype for complete operator inputs");
    }
    return output_qtype;
  };
}

}  // namespace arolla::operator_loader
