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
#include "arolla/expr/eval/executable_builder.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/eval/dynamic_compiled_expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_stack_trace.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

std::string FormatSlots(absl::Span<const TypedSlot> slots) {
  return absl::StrJoin(slots, ", ", [](std::string* out, TypedSlot s) {
    absl::StrAppend(out, FormatSlot(s));
  });
}

class DynamicBoundExprImpl : public DynamicBoundExpr {
 public:
  DynamicBoundExprImpl(
      absl::flat_hash_map<std::string, TypedSlot> input_slots,
      TypedSlot output_slot,
      std::vector<std::unique_ptr<BoundOperator>> init_ops,
      std::vector<std::unique_ptr<BoundOperator>> eval_ops,
      absl::flat_hash_map<std::string, TypedSlot> named_output_slots,
      std::vector<std::string> init_op_descriptions,
      std::vector<std::string> eval_op_descriptions,
      DenseArray<Text> op_display_names, DenseArray<Text> op_stack_traces)
      : DynamicBoundExpr(std::move(input_slots), output_slot,
                         std::move(named_output_slots)),
        init_ops_(std::move(init_ops)),
        eval_ops_(std::move(eval_ops)),
        init_op_descriptions_(std::move(init_op_descriptions)),
        eval_op_descriptions_(std::move(eval_op_descriptions)),
        op_display_names_(std::move(op_display_names)),
        op_stack_traces_(std::move(op_stack_traces)) {}

  void InitializeLiterals(EvaluationContext* ctx, FramePtr frame) const final {
    RunBoundOperators(init_ops_, ctx, frame);
  }

  void Execute(EvaluationContext* ctx, FramePtr frame) const final {
    int64_t last_ip = RunBoundOperators(eval_ops_, ctx, frame);
    if (!ctx->status().ok()) {
      RETURN_IF_ERROR(std::move(*ctx).status()).With([&](auto status_builder)
      {
        DCHECK_LT(last_ip, op_display_names_.size());
        status_builder << "during evaluation of operator "
          << op_display_names_[last_ip].AsOptional().value_or("");
      if (!op_stack_traces_.empty()) {
        status_builder << "\n"
                       <<
                       op_stack_traces_[last_ip].AsOptional().value_or("");
      }
        ctx->set_status(absl::Status(status_builder));
      });
    }
  }

  absl::Span<const std::string> init_op_descriptions() const final {
    return init_op_descriptions_;
  }
  absl::Span<const std::string> eval_op_descriptions() const final {
    return eval_op_descriptions_;
  }

 private:
  std::vector<std::unique_ptr<BoundOperator>> init_ops_;
  std::vector<std::unique_ptr<BoundOperator>> eval_ops_;
  std::vector<std::string> init_op_descriptions_;
  std::vector<std::string> eval_op_descriptions_;
  // Using DenseArray<Text> instead of std::vector<std::string> to reduce
  // standby memory usage.
  DenseArray<Text> op_display_names_;
  DenseArray<Text> op_stack_traces_;
};

absl::Status VerifyNoNulls(
    absl::Span<const std::unique_ptr<BoundOperator>> ops) {
  for (size_t i = 0; i < ops.size(); ++i) {
    if (ops[i] == nullptr) {
      return absl::InternalError(
          absl::StrFormat("missing operator at position %d", i));
    }
  }
  return absl::OkStatus();
}

}  // namespace

std::string FormatSlot(TypedSlot slot) {
  return absl::StrFormat("%s [0x%02X]", slot.GetType()->name(),
                         slot.byte_offset());
}

std::string FormatOperatorCall(absl::string_view op_name,
                               absl::Span<const TypedSlot> input_slots,
                               absl::Span<const TypedSlot> output_slots) {
  if (output_slots.empty()) {
    return absl::StrFormat("%s(%s)", op_name, FormatSlots(input_slots));
  } else {
    return absl::StrFormat("%s = %s(%s)", FormatSlots(output_slots), op_name,
                           FormatSlots(input_slots));
  }
}

ExecutableBuilder::ExecutableBuilder(
    FrameLayout::Builder* layout_builder, bool collect_op_descriptions,
    std::shared_ptr<const ExprStackTrace> stack_trace)
    : layout_builder_(layout_builder),
      collect_op_descriptions_(collect_op_descriptions) {
  if (stack_trace != nullptr) {
    stack_trace_builder_ = BoundExprStackTraceBuilder(stack_trace);
  }
}

absl::Status ExecutableBuilder::AddLiteralInitialization(
    const TypedValue& literal_value, TypedSlot output_slot) {
  if (literal_value.GetType() != output_slot.GetType()) {
    return absl::InternalError(absl::StrFormat(
        "incompatible types for literal and its slot: %s vs %s",
        literal_value.GetType()->name(), output_slot.GetType()->name()));
  }

  if (collect_op_descriptions_) {
    absl::StrAppendFormat(&init_literals_description_, "%s = %s\n",
                          FormatSlots({output_slot}), literal_value.Repr());
  }

  // We gather literals and slots into a vector in order initialize them all
  // in one operator in Build()
  literal_values_and_slots_.push_back({literal_value, output_slot});
  return absl::OkStatus();
}

absl::StatusOr<int64_t> ExecutableBuilder::BindEvalOp(
    const QExprOperator& op, absl::Span<const TypedSlot> input_slots,
    TypedSlot output_slot, absl::string_view display_name) {
  ASSIGN_OR_RETURN(auto bound_op, op.Bind(input_slots, output_slot),
                   _ << "while binding operator " << display_name);
  std::string description;
  if (collect_op_descriptions_) {
    description = FormatOperatorCall(display_name, input_slots, {output_slot});
  }
  return AddEvalOp(std::move(bound_op), std::move(description),
                   std::string(display_name));
}

int64_t ExecutableBuilder::AddInitOp(std::unique_ptr<BoundOperator> op,
                                     std::string description) {
  if (collect_op_descriptions_) {
    init_op_descriptions_.push_back(std::move(description));
  }
  init_ops_.push_back(std::move(op));
  return init_ops_.size() - 1;
}

int64_t ExecutableBuilder::AddEvalOp(std::unique_ptr<BoundOperator> op,
                                     std::string description,
                                     std::string display_name) {
  if (collect_op_descriptions_) {
    eval_op_descriptions_.push_back(std::move(description));
  }
  eval_ops_.push_back(std::move(op));
  op_display_names_.push_back(std::move(display_name));
  return eval_ops_.size() - 1;
}

int64_t ExecutableBuilder::SkipEvalOp() { return AddEvalOp(nullptr, "", ""); }

absl::Status ExecutableBuilder::SetEvalOp(int64_t offset,
                                          std::unique_ptr<BoundOperator> op,
                                          std::string description,
                                          std::string display_name) {
  if (offset < 0 || offset >= eval_ops_.size()) {
    return absl::InternalError(absl::StrFormat(
        "illegal operator offset: must be in range [0, %d), got %d",
        eval_ops_.size(), offset));
  }
  if (eval_ops_[offset] != nullptr) {
    return absl::InternalError(absl::StrFormat(
        "attempt to override existing operator at position %d", offset));
  }
  if (collect_op_descriptions_) {
    DCHECK_EQ(eval_ops_.size(), eval_op_descriptions_.size());
    eval_op_descriptions_[offset] = std::move(description);
  }
  eval_ops_[offset] = std::move(op);
  op_display_names_[offset] = std::move(display_name);
  return absl::OkStatus();
}

absl::Status ExecutableBuilder::AddNamedOutput(absl::string_view name,
                                               TypedSlot slot) {
  if (!named_outputs_.emplace(name, slot).second) {
    return absl::FailedPreconditionError(
        absl::StrCat("duplicated output slot name: ", name));
  }
  return absl::OkStatus();
}

void ExecutableBuilder::RegisterStacktrace(int64_t ip,
                                           const ExprNodePtr& node) {
  if (stack_trace_builder_.has_value()) {
    stack_trace_builder_->RegisterIp(ip, node);
  }
}

std::unique_ptr<BoundExpr> ExecutableBuilder::Build(
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    TypedSlot output_slot) && {
  if (!literal_values_and_slots_.empty()) {
    // Remove the final newline if init_literal_description is non-empty.
    if (!init_literals_description_.empty()) {
      init_literals_description_.pop_back();
    }
    AddInitOp(MakeBoundOperator(
                  [values_and_slots = std::move(literal_values_and_slots_)](
                      EvaluationContext* ctx, FramePtr frame) {
                    for (const auto& [value, slot] : values_and_slots) {
                      auto ref = value.AsRef();
                      ref.GetType()->UnsafeCopy(
                          ref.GetRawPointer(),
                          frame.GetRawPointer(slot.byte_offset()));
                    }
                  }),
              std::move(init_literals_description_));
  }
  DCHECK_OK(VerifyNoNulls(init_ops_));
  DCHECK_OK(VerifyNoNulls(eval_ops_));

  DenseArray<Text> stack_trace;
  if (stack_trace_builder_.has_value()) {
    stack_trace = stack_trace_builder_->Build(eval_ops_.size());
  }
  return std::make_unique<DynamicBoundExprImpl>(
      input_slots, output_slot, std::move(init_ops_), std::move(eval_ops_),
      std::move(named_outputs_), std::move(init_op_descriptions_),
      std::move(eval_op_descriptions_),
      CreateFullDenseArray<Text>(op_display_names_.begin(),
                                 op_display_names_.end()),
      std::move(stack_trace));
}

}  // namespace arolla::expr::eval_internal
