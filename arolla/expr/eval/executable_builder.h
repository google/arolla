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
#ifndef AROLLA_EXPR_EVAL_EXECUTABLE_BUILDER_H_
#define AROLLA_EXPR_EVAL_EXECUTABLE_BUILDER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_stack_trace.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr::eval_internal {

std::string FormatSlot(TypedSlot slot);

// Creates a readable description of an operator call.
std::string FormatOperatorCall(absl::string_view op_name,
                               absl::Span<const TypedSlot> input_slots,
                               absl::Span<const TypedSlot> output_slots);

// A helper class to construct executable expression incrementally.
class ExecutableBuilder {
 public:
  explicit ExecutableBuilder(
      FrameLayout::Builder* layout_builder,
      // Populate the init_op_descriptions() / eval_op_descriptions() in the
      // generated DynamicBoundExpr.
      bool collect_op_descriptions = false,
      std::shared_ptr<const ExprStackTrace> stack_trace = nullptr);

  FrameLayout::Builder* layout_builder() const { return layout_builder_; }

  // Adds literal initialization command.
  absl::Status AddLiteralInitialization(const TypedValue& literal_value,
                                        TypedSlot output_slot);

  // Binds and appends the operator for program evaluation.
  absl::StatusOr<int64_t> BindEvalOp(const QExprOperator& op,
                                     absl::Span<const TypedSlot> input_slots,
                                     TypedSlot output_slot,
                                     absl::string_view display_name);

  // Appends the operator for program initialization. If collect_op_descriptions
  // was true, the `description` will be recorded.
  int64_t AddInitOp(std::unique_ptr<BoundOperator> op, std::string description);

  // Appends the operator for program evaluation. If collect_op_descriptions
  // was true, the `description` will be recorded.
  int64_t AddEvalOp(std::unique_ptr<BoundOperator> op, std::string description,
                    std::string display_name);

  // Skips one operator, returning its position so it can be placed later.
  int64_t SkipEvalOp();

  // Puts the operator at the given position in the evaluation sequence. It is
  // only allowed if this position was previously skipped using SkipEvalOp().
  absl::Status SetEvalOp(int64_t offset, std::unique_ptr<BoundOperator> op,
                         std::string description, std::string display_name);

  // Offset after the last of the already added operators.
  int64_t current_eval_ops_size() { return eval_ops_.size(); }

  // Adds named output.
  absl::Status AddNamedOutput(absl::string_view name, TypedSlot slot);

  // Builds an executable expression from the added operators.
  std::unique_ptr<BoundExpr> Build(
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      TypedSlot output_slot) &&;

  void RegisterStacktrace(int64_t ip, const ExprNodePtr& node);

 private:
  FrameLayout::Builder* layout_builder_;
  std::vector<std::unique_ptr<BoundOperator>> init_ops_;

  std::vector<std::unique_ptr<BoundOperator>> eval_ops_;
  absl::flat_hash_map<std::string, TypedSlot> named_outputs_;

  bool collect_op_descriptions_;
  std::vector<std::string> init_op_descriptions_;
  std::vector<std::string> eval_op_descriptions_;
  std::vector<std::string> op_display_names_;
  std::vector<std::pair<TypedValue, TypedSlot>> literal_values_and_slots_;
  std::string init_literals_description_;
  std::optional<BoundExprStackTraceBuilder> stack_trace_builder_;
};

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_EXECUTABLE_BUILDER_H_
