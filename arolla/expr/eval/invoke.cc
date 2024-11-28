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
#include "arolla/expr/eval/invoke.h"

#include <string>
#include <utility>

#include "absl//container/flat_hash_map.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<TypedValue> Invoke(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, TypedValue>& leaf_values,
    DynamicEvaluationEngineOptions options) {
  absl::flat_hash_map<std::string, QTypePtr> leaf_types;
  leaf_types.reserve(leaf_values.size());
  for (const auto& [name, value] : leaf_values) {
    leaf_types.emplace(name, value.GetType());
  }

  ASSIGN_OR_RETURN(auto compiled_expr,
                   CompileForDynamicEvaluation(options, expr, leaf_types));

  FrameLayout::Builder layout_builder;
  // Note that due to optimizations, some inputs can be not used by
  // compiled expression anymore.
  const auto leaf_slots =
      AddSlotsMap(compiled_expr->input_types(), &layout_builder);
  ASSIGN_OR_RETURN(auto executable_expr,
                   compiled_expr->Bind(
                       &layout_builder, leaf_slots,
                       AddSlot(compiled_expr->output_type(), &layout_builder)));
  FrameLayout layout = std::move(layout_builder).Build();
  RootEvaluationContext ctx(&layout);
  RETURN_IF_ERROR(executable_expr->InitializeLiterals(&ctx));

  for (const auto& [name, slot] : leaf_slots) {
    if (!leaf_values.contains(name)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("value was not specified for leaf %s", name));
    }
    RETURN_IF_ERROR(leaf_values.at(name).CopyToSlot(slot, ctx.frame()));
  }
  RETURN_IF_ERROR(executable_expr->Execute(&ctx));
  return TypedValue::FromSlot(executable_expr->output_slot(), ctx.frame());
}

}  // namespace arolla::expr
