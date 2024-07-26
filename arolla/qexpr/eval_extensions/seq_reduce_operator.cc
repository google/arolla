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
#include "arolla/qexpr/eval_extensions/seq_reduce_operator.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/dynamic_compiled_expr.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/seq_reduce_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

// Converts seq.reduce operators in expression to PackedSeqReduceOp.
absl::StatusOr<ExprNodePtr> SeqReduceOperatorTransformation(
    const DynamicEvaluationEngineOptions&, ExprNodePtr node) {
  ASSIGN_OR_RETURN(auto seq_reduce_op, DecayRegisteredOperator(node->op()));
  if (seq_reduce_op == nullptr ||
      typeid(*seq_reduce_op) != typeid(SeqReduceOperator)) {
    return node;
  }
  const auto& node_deps = node->node_deps();
  if (node_deps.size() != 3) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unexpected number of arguments: expected 3, got %d",
                        node_deps.size()));
  }
  const auto& op_node = node_deps[0];
  if (op_node->qtype() == nullptr) {
    return absl::FailedPreconditionError("missing node_deps[0].qtype");
  }
  if (op_node->qtype() != GetQType<ExprOperatorPtr>()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected node_deps[0].qtype: expected %s, got %s",
        GetQType<ExprOperatorPtr>()->name(), op_node->qtype()->name()));
  }
  const auto& op_qvalue = op_node->qvalue();
  if (!op_qvalue.has_value()) {
    return absl::FailedPreconditionError("missing node_deps[0].literal_value");
  }
  DCHECK(op_qvalue->GetType() == GetQType<ExprOperatorPtr>());
  const auto& op = op_qvalue->UnsafeAs<ExprOperatorPtr>();
  return MakeOpNode(
      std::make_shared<PackedSeqReduceOperator>(op),
      std::vector<ExprNodePtr>(node_deps.begin() + 1, node_deps.end()));
}

// Compiles SeqReduceOperator into the executable_builder.
std::optional<absl::Status> CompilePackedSeqReduceOperator(
    const CompileOperatorFnArgs& args) {
  const auto* reduce_op =
      dynamic_cast<const PackedSeqReduceOperator*>(args.op.get());
  if (reduce_op == nullptr) {
    return std::nullopt;
  }

  if (args.input_slots.size() != 2) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unexpected number of input slots: expected 2, got %d",
                        args.input_slots.size()));
  }
  const auto& seq_slot = args.input_slots[0];
  const auto& initial_slot = args.input_slots[1];
  if (!IsSequenceQType(seq_slot.GetType())) {
    return absl::FailedPreconditionError(
        absl::StrFormat("expected a sequence type, got seq_qtype = %s",
                        seq_slot.GetType()->name()));
  }
  const auto value_qtype = seq_slot.GetType()->value_qtype();
  DCHECK(value_qtype != nullptr);
  const auto output_qtype = args.output_slot.GetType();
  if (initial_slot.GetType() != output_qtype) {
    return absl::FailedPreconditionError(
        absl::StrFormat("expected initial_qtype == output_qtype: %s != %s",
                        initial_slot.GetType()->name(), output_qtype->name()));
  }
  auto reducer_arg_1_slot =
      AddSlot(output_qtype, args.executable_builder->layout_builder());
  auto reducer_arg_2_slot =
      AddSlot(value_qtype, args.executable_builder->layout_builder());
  DynamicEvaluationEngineOptions subexpression_options(args.options);
  // Some preparation stages may be disabled, but we restore the defaults for
  // the wrapped operator.
  subexpression_options.enabled_preparation_stages =
      DynamicEvaluationEngineOptions::PreparationStage::kAll;

  ASSIGN_OR_RETURN(
      std::shared_ptr<BoundExpr> reducer_bound_expr,
      CompileAndBindExprOperator(
          subexpression_options, args.executable_builder->layout_builder(),
          reduce_op->op(), {reducer_arg_1_slot, reducer_arg_2_slot},
          args.output_slot));

  std::string init_op_description;
  std::string eval_op_description;
  if (args.options.collect_op_descriptions) {
    auto dynamic_bound_expr =
        dynamic_cast<const DynamicBoundExpr*>(reducer_bound_expr.get());
    if (dynamic_bound_expr == nullptr) {
      return absl::InternalError("expected DynamicBoundExpr");
    }
    auto init_op_name = absl::StrFormat(
        "%s:init{%s}", reduce_op->display_name(),
        absl::StrJoin(dynamic_bound_expr->init_op_descriptions(), "; "));
    init_op_description = FormatOperatorCall(init_op_name, {}, {});
    auto eval_op_name = absl::StrFormat(
        "%s:eval{%s}", reduce_op->display_name(),
        absl::StrJoin(dynamic_bound_expr->eval_op_descriptions(), "; "));
    eval_op_description =
        FormatOperatorCall(eval_op_name, args.input_slots, {args.output_slot});
  }

  args.executable_builder->AddInitOp(
      MakeBoundOperator(
          [reducer_bound_expr](EvaluationContext* ctx, FramePtr frame) {
            reducer_bound_expr->InitializeLiterals(ctx, frame);
          }),
      init_op_description);
  args.executable_builder->AddEvalOp(
      MakeBoundOperator(
          [reducer_bound_expr, initial_slot, seq_slot,
           output_slot = args.output_slot, reducer_arg_1_slot,
           reducer_arg_2_slot](EvaluationContext* ctx, FramePtr frame) {
            const auto& seq = frame.Get(seq_slot.UnsafeToSlot<Sequence>());
            const auto* value_qtype = seq.value_qtype();
            const size_t seq_size = seq.size();
            const size_t value_size = value_qtype->type_layout().AllocSize();
            initial_slot.CopyTo(frame, output_slot, frame);
            for (size_t i = 0; i < seq_size && ctx->status().ok(); ++i) {
              output_slot.CopyTo(frame, reducer_arg_1_slot, frame);
              value_qtype->UnsafeCopy(
                  seq.RawAt(i, value_size),
                  frame.GetRawPointer(reducer_arg_2_slot.byte_offset()));
              reducer_bound_expr->Execute(ctx, frame);
            }
          }),
      eval_op_description,
      /*display_name=*/"seq.reduce");
  return absl::OkStatus();
}

}  // namespace

PackedSeqReduceOperator::PackedSeqReduceOperator(ExprOperatorPtr op)
    : ExprOperatorWithFixedSignature(
          absl::StrFormat("packed_seq_reduce[%s]", op->display_name()),
          ExprOperatorSignature{{"seq"}, {"initial"}},
          "(internal operator) packed seq.reduce",
          FingerprintHasher(
              "arolla::expr::eval_internal::PackedSeqReduceOperator")
              .Combine(op->fingerprint())
              .Finish()),
      op_(std::move(op)) {}

absl::StatusOr<ExprAttributes> PackedSeqReduceOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  std::vector<ExprAttributes> new_inputs;
  new_inputs.reserve(inputs.size() + 1);
  new_inputs.emplace_back(GetQType<ExprOperatorPtr>(),
                          TypedValue::FromValue(op_));
  new_inputs.insert(new_inputs.end(), inputs.begin(), inputs.end());
  return SeqReduceOperator::Make()->InferAttributes(new_inputs);
}

AROLLA_INITIALIZER(
        .reverse_deps =
            {
                ::arolla::initializer_dep::kOperators,
                ::arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = [] {
          CompilerExtensionRegistry::GetInstance().RegisterNodeTransformationFn(
              SeqReduceOperatorTransformation);
          CompilerExtensionRegistry::GetInstance().RegisterCompileOperatorFn(
              CompilePackedSeqReduceOperator);
        })

}  // namespace arolla::expr::eval_internal
