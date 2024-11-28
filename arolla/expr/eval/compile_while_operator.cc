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
#include "arolla/expr/eval/compile_while_operator.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "absl//types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/evaluator_operators.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/operators/while_loop/while_loop.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

struct BoundLoopOperators {
  std::shared_ptr<const BoundExpr> condition;
  std::shared_ptr<const BoundExpr> body;
};

// Bound operator for while_loop.
class WhileLoopBoundOperator : public BoundOperator {
 public:
  // The operator accepts two copies of body and condition evaluators:
  //   operators_on_out.condition: output_state_slot -> condition_slot
  //   operators_on_tmp.condition: tmp_state_slot -> condition_slot
  //   operators_on_out.body: output_state_slot -> tmp_state_slot
  //   operators_on_tmp.body: tmp_state_slot -> output_state_slot
  //
  // The operators are executed in turn, in order to avoid copying data from
  // tmp_state_slot to output_state_slot.
  //
  WhileLoopBoundOperator(BoundLoopOperators operators_on_out,
                         BoundLoopOperators operators_on_tmp,
                         FrameLayout::Slot<OptionalUnit> condition_slot,
                         TypedSlot initial_state_slot, TypedSlot tmp_state_slot,
                         TypedSlot output_state_slot)
      : operators_on_out_(std::move(operators_on_out)),
        operators_on_tmp_(std::move(operators_on_tmp)),
        condition_slot_(condition_slot),
        initial_state_slot_(initial_state_slot),
        tmp_state_slot_(tmp_state_slot),
        output_state_slot_(output_state_slot) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    initial_state_slot_.CopyTo(frame, output_state_slot_, frame);
    for (;;) {
      operators_on_out_.condition->Execute(ctx, frame);
      if (!ctx->status().ok() || !frame.Get(condition_slot_)) {
        break;
      }
      operators_on_out_.body->Execute(ctx, frame);
      if (!ctx->status().ok()) {
        break;
      }
      operators_on_tmp_.condition->Execute(ctx, frame);
      if (!ctx->status().ok() || !frame.Get(condition_slot_)) {
        tmp_state_slot_.CopyTo(frame, output_state_slot_, frame);
        break;
      }
      operators_on_tmp_.body->Execute(ctx, frame);
      if (!ctx->status().ok()) {
        break;
      }
    }
  }

 private:
  BoundLoopOperators operators_on_out_;
  BoundLoopOperators operators_on_tmp_;
  FrameLayout::Slot<OptionalUnit> condition_slot_;
  TypedSlot initial_state_slot_;
  TypedSlot tmp_state_slot_;
  TypedSlot output_state_slot_;
};

absl::StatusOr<std::shared_ptr<BoundExpr>> CompileAndBindExprOperator(
    const DynamicEvaluationEngineOptions& options, const ExprOperatorPtr& op,
    absl::Span<const TypedSlot> input_slots,
    std::optional<TypedSlot> output_slot,
    ExecutableBuilder& executable_builder) {
  ASSIGN_OR_RETURN(
      auto evaluator,
      CompileAndBindExprOperator(options, executable_builder.layout_builder(),
                                 op, input_slots, output_slot),
      _ << "in loop condition");
  executable_builder.AddInitOp(
      std::make_unique<InitializeAstLiteralsBoundOperator>(evaluator),
      "internal.while_loop:initialize_literals()");
  return evaluator;
}

absl::StatusOr<BoundLoopOperators> BindLoopOperators(
    const DynamicEvaluationEngineOptions& options,
    const expr_operators::WhileLoopOperator& while_op,
    absl::Span<const TypedSlot> constant_slots, TypedSlot current_state_slot,
    TypedSlot next_state_slot, FrameLayout::Slot<OptionalUnit> condition_slot,
    ExecutableBuilder& executable_builder) {
  std::vector<TypedSlot> input_slots;
  input_slots.reserve(1 + constant_slots.size());
  input_slots.push_back(current_state_slot);
  input_slots.insert(input_slots.end(), constant_slots.begin(),
                     constant_slots.end());
  ASSIGN_OR_RETURN(auto condition_on_out_op,
                   CompileAndBindExprOperator(
                       options, while_op.condition(), input_slots,
                       TypedSlot::FromSlot(condition_slot), executable_builder),
                   _ << "in loop condition");
  ASSIGN_OR_RETURN(
      auto body_out_to_tmp_op,
      CompileAndBindExprOperator(options, while_op.body(), input_slots,
                                 next_state_slot, executable_builder),
      _ << "in loop body");
  return BoundLoopOperators{std::move(condition_on_out_op),
                            std::move(body_out_to_tmp_op)};
}

}  // namespace

absl::Status CompileWhileOperator(
    const DynamicEvaluationEngineOptions& options,
    const expr_operators::WhileLoopOperator& while_op,
    absl::Span<const TypedSlot> input_slots, TypedSlot output_slot,
    ExecutableBuilder& executable_builder) {
  if (input_slots.empty()) {
    return absl::InvalidArgumentError(
        "unexpected number of input slots: expected at least 1 slot, got 0");
  }
  TypedSlot initial_state_slot = input_slots[0];
  if (output_slot.GetType() != initial_state_slot.GetType()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unexpected type of output slot: expected %s slot, got %s",
        initial_state_slot.GetType()->name(), output_slot.GetType()->name()));
  }

  FrameLayout::Slot<OptionalUnit> condition_slot =
      executable_builder.layout_builder()->AddSlot<OptionalUnit>();
  // Temporaty slot to store intermediate loop state.
  TypedSlot tmp_state_slot =
      AddSlot(output_slot.GetType(), executable_builder.layout_builder());

  DynamicEvaluationEngineOptions subexpression_options(options);
  // Some preparation stages may be disabled, but we restore the defaults for
  // the wrapped operator.
  subexpression_options.enabled_preparation_stages =
      DynamicEvaluationEngineOptions::PreparationStage::kAll;
  ASSIGN_OR_RETURN(auto operators_on_out,
                   BindLoopOperators(subexpression_options, while_op,
                                     /*constant_slots=*/input_slots.subspan(1),
                                     /*current_state_slot=*/output_slot,
                                     /*next_state_slot=*/tmp_state_slot,
                                     condition_slot, executable_builder));
  ASSIGN_OR_RETURN(auto operators_on_tmp,
                   BindLoopOperators(subexpression_options, while_op,
                                     /*constant_slots=*/input_slots.subspan(1),
                                     /*current_state_slot=*/tmp_state_slot,
                                     /*next_state_slot=*/output_slot,
                                     condition_slot, executable_builder));

  std::vector<TypedSlot> used_slots(input_slots.begin(), input_slots.end());
  used_slots.push_back(tmp_state_slot);
  used_slots.push_back(TypedSlot::FromSlot(condition_slot));

  executable_builder.AddEvalOp(
      std::make_unique<WhileLoopBoundOperator>(
          std::move(operators_on_out), std::move(operators_on_tmp),
          condition_slot, initial_state_slot, tmp_state_slot, output_slot),
      eval_internal::FormatOperatorCall("internal.while_loop", input_slots,
                                        {output_slot}),
      "internal.while_loop");
  return absl::OkStatus();
}

}  // namespace arolla::expr::eval_internal
