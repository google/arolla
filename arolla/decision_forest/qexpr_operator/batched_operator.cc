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
#include "arolla/decision_forest/qexpr_operator/batched_operator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/batched_evaluation/batched_forest_evaluator.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

class DecisionForestBoundOperator : public BoundOperator {
 public:
  DecisionForestBoundOperator(std::shared_ptr<BatchedForestEvaluator> evaluator,
                              absl::Span<const TypedSlot> input_slots,
                              absl::Span<const TypedSlot> output_slots)
      : evaluator_(std::move(evaluator)),
        input_slots_(input_slots.begin(), input_slots.end()),
        output_slots_(output_slots.begin(), output_slots.end()) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const final {
    ctx->set_status(evaluator_->EvalBatch(input_slots_, output_slots_, frame,
                                          &ctx->buffer_factory()));
  }

 private:
  std::shared_ptr<BatchedForestEvaluator> evaluator_;
  std::vector<TypedSlot> input_slots_;
  std::vector<TypedSlot> output_slots_;
};

class BatchedDecisionForestOperator : public QExprOperator {
 public:
  BatchedDecisionForestOperator(
      std::shared_ptr<BatchedForestEvaluator> evaluator, std::string op_name,
      const QExprOperatorSignature* op_type)
      : QExprOperator(std::move(op_name), op_type),
        evaluator_(std::move(evaluator)) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    std::vector<TypedSlot> output_subslots;
    output_subslots.reserve(output_slot.SubSlotCount());
    for (int64_t i = 0; i < output_slot.SubSlotCount(); ++i) {
      output_subslots.push_back(output_slot.SubSlot(i));
    }
    return std::unique_ptr<BoundOperator>(new DecisionForestBoundOperator(
        evaluator_, input_slots, output_subslots));
  }

  std::shared_ptr<BatchedForestEvaluator> evaluator_;
};

}  // namespace

absl::StatusOr<OperatorPtr> CreateBatchedDecisionForestOperator(
    const DecisionForestPtr& decision_forest,
    const QExprOperatorSignature* op_signature,
    absl::Span<const TreeFilter> groups) {
  for (const auto& kv : decision_forest->GetRequiredQTypes()) {
    if (kv.first >= op_signature->input_types().size()) {
      return absl::InvalidArgumentError("not enough arguments");
    }
    QTypePtr input_type = op_signature->input_types()[kv.first];
    ASSIGN_OR_RETURN(const ArrayLikeQType* array_type,
                     ToArrayLikeQType(input_type));
    ASSIGN_OR_RETURN(QTypePtr opt_t,
                     ToOptionalQType(array_type->value_qtype()));
    if (opt_t != kv.second) {
      return absl::InvalidArgumentError(
          absl::StrFormat("type mismatch for input #%d: %s expected, %s found",
                          kv.first, kv.second->name(), opt_t->name()));
    }
  }
  RETURN_IF_ERROR(ValidateBatchedDecisionForestOutputType(
      op_signature->output_type(), groups.size()));
  ASSIGN_OR_RETURN(auto evaluator,
                   BatchedForestEvaluator::Compile(*decision_forest, groups));

  FingerprintHasher hasher("::arolla::BatchedDecisionForestOperator");
  hasher.Combine(decision_forest->fingerprint()).CombineSpan(groups);
  std::string op_name =
      absl::StrFormat("core.batched_decision_forest_evaluator_%s",
                      std::move(hasher).Finish().AsString());
  return OperatorPtr(std::make_shared<BatchedDecisionForestOperator>(
      std::move(evaluator), std::move(op_name), op_signature));
}

absl::Status ValidateBatchedDecisionForestOutputType(const QTypePtr output,
                                                     int group_count) {
  if (!IsTupleQType(output)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "DecisionForest output should be a tuple, got %s", output->name()));
  }
  if (output->type_fields().size() != group_count) {
    return absl::InvalidArgumentError(
        absl::StrFormat("incorrect number of outputs: required %d, got %d",
                        group_count, output->type_fields().size()));
  }
  for (auto field : output->type_fields()) {
    if (!IsArrayLikeQType(field.GetType())) {
      return absl::InvalidArgumentError(
          absl::StrFormat("%s is not an array", field.GetType()->name()));
    }
  }
  return absl::OkStatus();
}

}  // namespace arolla
