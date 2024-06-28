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
#include "arolla/decision_forest/qexpr_operator/pointwise_operator.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

class DecisionForestBoundOperator : public BoundOperator {
 public:
  explicit DecisionForestBoundOperator(ForestEvaluator&& evaluator)
      : evaluator_(std::move(evaluator)) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const final {
    evaluator_.Eval(frame, frame);
  }

 private:
  ForestEvaluator evaluator_;
};

class PointwiseDecisionForestOperator : public QExprOperator {
 public:
  PointwiseDecisionForestOperator(DecisionForestPtr decision_forest,
                                  std::string op_name,
                                  const QExprOperatorSignature* op_signature,
                                  absl::Span<const TreeFilter> groups)
      : QExprOperator(std::move(op_name), op_signature),
        decision_forest_(std::move(decision_forest)),
        groups_(groups.begin(), groups.end()) {}

 private:
  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const override {
    if (output_slot.SubSlotCount() != groups_.size()) {
      return absl::InvalidArgumentError("incorrect output type");
    }
    std::vector<ForestEvaluator::Output> outputs;
    outputs.reserve(groups_.size());
    for (size_t i = 0; i < groups_.size(); ++i) {
      ASSIGN_OR_RETURN(auto slot, output_slot.SubSlot(i).ToSlot<float>());
      outputs.push_back({groups_[i], slot});
    }
    ASSIGN_OR_RETURN(
        ForestEvaluator evaluator,
        ForestEvaluator::Compile(*decision_forest_, input_slots, outputs));

    return std::unique_ptr<BoundOperator>(
        new DecisionForestBoundOperator(std::move(evaluator)));
  }

 private:
  DecisionForestPtr decision_forest_;
  std::vector<TreeFilter> groups_;
};

}  // namespace

absl::StatusOr<OperatorPtr> CreatePointwiseDecisionForestOperator(
    const DecisionForestPtr& decision_forest,
    const QExprOperatorSignature* op_signature,
    absl::Span<const TreeFilter> groups) {
  for (const auto& kv : decision_forest->GetRequiredQTypes()) {
    if (kv.first >= op_signature->input_types().size()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("not enough arguments: input #%d is required, "
                          "but only %d arguments are provided",
                          kv.first, op_signature->input_types().size()));
    }
    if (op_signature->input_types()[kv.first] != kv.second) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "type mismatch for input #%d: %s expected, %s found", kv.first,
          kv.second->name(), op_signature->input_types()[kv.first]->name()));
    }
  }
  RETURN_IF_ERROR(ValidatePointwiseDecisionForestOutputType(
      op_signature->output_type(), groups.size()));

  FingerprintHasher hasher("::arolla::PointwiseDecisionForestOperator");
  hasher.Combine(decision_forest->fingerprint()).CombineSpan(groups);
  std::string op_name =
      absl::StrFormat("core.pointwise_decision_forest_evaluator_%s",
                      std::move(hasher).Finish().AsString());
  return OperatorPtr(std::make_shared<PointwiseDecisionForestOperator>(
      decision_forest, std::move(op_name), op_signature, groups));
}

absl::Status ValidatePointwiseDecisionForestOutputType(const QTypePtr output,
                                                       int group_count) {
  auto required_output =
      MakeTupleQType(std::vector<QTypePtr>(group_count, GetQType<float>()));
  if (output != required_output) {
    return absl::InvalidArgumentError(
        absl::StrFormat("incorrect output type: expected %s, got %s",
                        required_output->name(), output->name()));
  }
  return absl::OkStatus();
}

}  // namespace arolla
