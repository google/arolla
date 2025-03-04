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
#include "arolla/expr/eval/eval.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/dynamic_compiled_expr.h"
#include "arolla/expr/eval/expr_stack_trace.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<std::unique_ptr<CompiledExpr>> CompileForDynamicEvaluation(
    const DynamicEvaluationEngineOptions& options, const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const absl::flat_hash_map<std::string, ExprNodePtr>& side_outputs) {
  auto expr_with_side_outputs = expr;

  std::vector<std::string> side_output_names;
  if (!side_outputs.empty()) {
    side_output_names.reserve(side_outputs.size());
    for (const auto& [name, _] : side_outputs) {
      side_output_names.push_back(name);
    }
    std::sort(side_output_names.begin(), side_output_names.end());
    std::vector<ExprNodePtr> exprs = {expr_with_side_outputs};
    exprs.reserve(side_outputs.size() + 1);
    for (const auto& name : side_output_names) {
      exprs.push_back(side_outputs.at(name));
    }
    ASSIGN_OR_RETURN(
        expr_with_side_outputs,
        BindOp(eval_internal::InternalRootOperator(), std::move(exprs), {}));
  }

  std::shared_ptr<ExprStackTrace> stack_trace = nullptr;
  if (options.enable_expr_stack_trace) {
    stack_trace = std::make_shared<LightweightExprStackTrace>();
  }

  ASSIGN_OR_RETURN(
      ExprNodePtr prepared_expr,
      eval_internal::PrepareExpression(expr_with_side_outputs, input_types,
                                       options, stack_trace));
  auto placeholder_keys = GetPlaceholderKeys(prepared_expr);
  if (!placeholder_keys.empty()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "placeholders should be substituted before "
        "evaluation: %s, got %s",
        absl::StrJoin(placeholder_keys, ","), ToDebugString(prepared_expr)));
  }

  absl::flat_hash_map<Fingerprint, QTypePtr> node_types;
  ASSIGN_OR_RETURN(prepared_expr, eval_internal::ExtractQTypesForCompilation(
                                      prepared_expr, &node_types, stack_trace));
  ASSIGN_OR_RETURN(auto used_input_types,
                   eval_internal::LookupLeafQTypes(prepared_expr, node_types));
  ASSIGN_OR_RETURN(auto named_output_types,
                   eval_internal::LookupNamedOutputTypes(
                       prepared_expr, side_output_names, node_types));

  for (const auto& [key, qtype] : used_input_types) {
    if (qtype == nullptr) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "unable to deduce input type for L.%s in the expression %s", key,
          GetDebugSnippet(prepared_expr)));
    }
  }

  ASSIGN_OR_RETURN(QTypePtr output_type,
                   eval_internal::LookupQType(prepared_expr, node_types));
  if (output_type == nullptr) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unable to deduce output type in the expression %s",
                        GetDebugSnippet(prepared_expr)));
  }
  return std::unique_ptr<CompiledExpr>(new eval_internal::DynamicCompiledExpr(
      options, std::move(used_input_types), output_type,
      std::move(named_output_types), std::move(prepared_expr),
      std::move(side_output_names), std::move(node_types),
      std::move(stack_trace)));
}

absl::StatusOr<std::unique_ptr<BoundExpr>> CompileAndBindForDynamicEvaluation(
    const DynamicEvaluationEngineOptions& options,
    FrameLayout::Builder* layout_builder, const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    std::optional<TypedSlot> output_slot,
    const absl::flat_hash_map<std::string, ExprNodePtr>& side_outputs) {
  ASSIGN_OR_RETURN(auto compiled_expr,
                   CompileForDynamicEvaluation(
                       options, expr, SlotsToTypes(input_slots), side_outputs));
  ASSIGN_OR_RETURN(
      auto executable_expr,
      compiled_expr->Bind(layout_builder, input_slots, output_slot));
  if (output_slot.has_value() &&
      executable_expr->output_slot() != *output_slot) {
    return absl::InternalError("expression bound to a wrong output slot");
  }
  return executable_expr;
}

absl::StatusOr<std::shared_ptr<BoundExpr>> CompileAndBindExprOperator(
    const DynamicEvaluationEngineOptions& options,
    FrameLayout::Builder* layout_builder, const ExprOperatorPtr& op,
    absl::Span<const TypedSlot> input_slots,
    std::optional<TypedSlot> output_slot) {
  std::vector<absl::StatusOr<ExprNodePtr>> inputs;
  inputs.reserve(input_slots.size());
  absl::flat_hash_map<std::string, TypedSlot> input_slots_map;
  input_slots_map.reserve(input_slots.size());
  for (size_t i = 0; i < input_slots.size(); ++i) {
    std::string name = absl::StrFormat("input_%d", i);
    inputs.push_back(Leaf(name));
    input_slots_map.emplace(name, input_slots[i]);
  }
  ASSIGN_OR_RETURN(auto expr, CallOp(op, inputs));
  ASSIGN_OR_RETURN(auto evaluator, CompileAndBindForDynamicEvaluation(
                                       options, layout_builder, expr,
                                       input_slots_map, output_slot));
  return std::shared_ptr<BoundExpr>(std::move(evaluator));
}

}  // namespace arolla::expr
