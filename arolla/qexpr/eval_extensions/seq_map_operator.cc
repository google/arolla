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
#include "arolla/qexpr/eval_extensions/seq_map_operator.h"

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
#include "arolla/expr/seq_map_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

// Converts seq.map operators in expression to PackedSeqMapOp.
absl::StatusOr<ExprNodePtr> SeqMapOperatorTransformation(
    const DynamicEvaluationEngineOptions&, ExprNodePtr node) {
  ASSIGN_OR_RETURN(auto seq_map_op, DecayRegisteredOperator(node->op()));
  if (seq_map_op == nullptr || typeid(*seq_map_op) != typeid(SeqMapOperator)) {
    return node;
  }
  const auto& node_deps = node->node_deps();
  if (node_deps.size() < 2) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "unexpected number of arguments: expected at least two, got %d",
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
      std::make_shared<PackedSeqMapOperator>(op),
      std::vector<ExprNodePtr>(node_deps.begin() + 1, node_deps.end()));
}

// Compiles SeqMapOperator into the executable_builder.
std::optional<absl::Status> CompilePackedSeqMapOperator(
    const CompileOperatorFnArgs& args) {
  const auto* map_op = dynamic_cast<const PackedSeqMapOperator*>(args.op.get());
  if (map_op == nullptr) {
    return std::nullopt;
  }

  if (args.input_slots.empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("expected at least one input slot, got none"));
  }
  if (!IsSequenceQType(args.output_slot.GetType())) {
    return absl::FailedPreconditionError(
        absl::StrFormat("expected a sequence type, got output_qtype = %s",
                        args.output_slot.GetType()->name()));
  }
  std::vector<QTypePtr> value_qtypes;
  value_qtypes.reserve(args.input_slots.size());
  for (size_t i = 0; i < args.input_slots.size(); ++i) {
    value_qtypes.push_back(args.input_slots[i].GetType()->value_qtype());
    DCHECK(value_qtypes.back() != nullptr);
  }

  std::vector<TypedSlot> mapper_arg_slots;
  mapper_arg_slots.reserve(args.input_slots.size());
  for (size_t i = 0; i < args.input_slots.size(); ++i) {
    mapper_arg_slots.push_back(
        AddSlot(value_qtypes[i], args.executable_builder->layout_builder()));
  }
  auto mapper_output_slot = AddSlot(args.output_slot.GetType()->value_qtype(),
                                    args.executable_builder->layout_builder());

  DynamicEvaluationEngineOptions subexpression_options(args.options);
  // Some preparation stages may be disabled, but we restore the defaults for
  // the wrapped operator.
  subexpression_options.enabled_preparation_stages =
      DynamicEvaluationEngineOptions::PreparationStage::kAll;

  ASSIGN_OR_RETURN(
      std::shared_ptr<BoundExpr> mapper_bound_expr,
      CompileAndBindExprOperator(
          subexpression_options, args.executable_builder->layout_builder(),
          map_op->op(), mapper_arg_slots, mapper_output_slot));

  std::string init_op_description;
  std::string eval_op_description;
  if (args.options.collect_op_descriptions) {
    auto dynamic_bound_expr =
        dynamic_cast<const DynamicBoundExpr*>(mapper_bound_expr.get());
    if (dynamic_bound_expr == nullptr) {
      return absl::InternalError("expected DynamicBoundExpr");
    }
    auto init_op_name = absl::StrFormat(
        "%s:init{%s}", map_op->display_name(),
        absl::StrJoin(dynamic_bound_expr->init_op_descriptions(), "; "));
    init_op_description = FormatOperatorCall(init_op_name, {}, {});
    auto eval_op_name = absl::StrFormat(
        "%s:eval{%s}", map_op->display_name(),
        absl::StrJoin(dynamic_bound_expr->eval_op_descriptions(), "; "));
    eval_op_description =
        FormatOperatorCall(eval_op_name, args.input_slots, {args.output_slot});
  }

  args.executable_builder->AddInitOp(
      MakeBoundOperator(
          [mapper_bound_expr](EvaluationContext* ctx, FramePtr frame) {
            mapper_bound_expr->InitializeLiterals(ctx, frame);
          }),
      init_op_description);
  args.executable_builder->AddEvalOp(
      MakeBoundOperator([input_slots = std::vector(args.input_slots.begin(),
                                                   args.input_slots.end()),
                         output_slot = args.output_slot, mapper_bound_expr,
                         mapper_arg_slots, mapper_output_slot](
                            EvaluationContext* ctx, FramePtr frame) {
        std::optional<size_t> seq_size = std::nullopt;
        for (size_t i = 0; i < input_slots.size(); ++i) {
          const auto& cur_slot = input_slots[i];
          const auto& seq = frame.Get(cur_slot.UnsafeToSlot<Sequence>());
          if (seq_size.has_value() && *seq_size != seq.size()) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "expected all sequences to have the same length, got %d and %d",
                *seq_size, seq.size())));
            return;
          }
          seq_size = seq.size();
        }
        const size_t output_value_size =
            mapper_output_slot.GetType()->type_layout().AllocSize();

        ASSIGN_OR_RETURN(
            auto mutable_sequence,
            MutableSequence::Make(mapper_output_slot.GetType(), *seq_size),
            ctx->set_status(std::move(_)));

        for (size_t i = 0; i < seq_size && ctx->status().ok(); ++i) {
          for (size_t arg_id = 0; arg_id < input_slots.size(); ++arg_id) {
            const auto& cur_slot = input_slots[arg_id];
            const auto& seq = frame.Get(cur_slot.UnsafeToSlot<Sequence>());
            seq.GetRef(i)
                .CopyToSlot(mapper_arg_slots[arg_id], frame)
                .IgnoreError();
          }
          mapper_bound_expr->Execute(ctx, frame);
          mapper_output_slot.GetType()->UnsafeCopy(
              frame.GetRawPointer(mapper_output_slot.byte_offset()),
              mutable_sequence.RawAt(i, output_value_size));
        }
        frame.Set(output_slot.UnsafeToSlot<Sequence>(),
                  std::move(mutable_sequence).Finish());
      }),
      eval_op_description,
      /*display_name=*/"seq.map");
  return absl::OkStatus();
}

}  // namespace

PackedSeqMapOperator::PackedSeqMapOperator(ExprOperatorPtr op)
    : ExprOperatorWithFixedSignature(
          absl::StrFormat("packed_seq_map[%s]", op->display_name()),
          ExprOperatorSignature::MakeVariadicArgs(),
          "(internal operator) packed seq.map",
          FingerprintHasher("arolla::expr::eval_internal::PackedSeqMapOperator")
              .Combine(op->fingerprint())
              .Finish()),
      op_(std::move(op)) {}

absl::StatusOr<ExprAttributes> PackedSeqMapOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  std::vector<ExprAttributes> new_inputs;
  new_inputs.reserve(inputs.size() + 1);
  new_inputs.emplace_back(GetQType<ExprOperatorPtr>(),
                          TypedValue::FromValue(op_));
  new_inputs.insert(new_inputs.end(), inputs.begin(), inputs.end());
  return SeqMapOperator::Make()->InferAttributes(new_inputs);
}

AROLLA_INITIALIZER(
        .reverse_deps =
            {
                ::arolla::initializer_dep::kOperators,
                ::arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = [] {
          CompilerExtensionRegistry::GetInstance().RegisterNodeTransformationFn(
              SeqMapOperatorTransformation);
          CompilerExtensionRegistry::GetInstance().RegisterCompileOperatorFn(
              CompilePackedSeqMapOperator);
        })

}  // namespace arolla::expr::eval_internal
