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
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/dynamic_compiled_expr.h"
#include "arolla/expr/eval/dynamic_compiled_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/eval_extensions/prepare_core_map_operator.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/array_like/frame_iter.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

// BoundOperator for core.map operator. It does not have Operator/OperatorFamily
// counterparts, but is bound via CompilePackedCoreMapOperator instead.
class MapBoundOperator : public BoundOperator {
 public:
  MapBoundOperator(std::unique_ptr<BoundExpr> mapper_bound_expr,
                   absl::Span<const TypedSlot> input_slots,
                   TypedSlot output_slot, FrameLayout&& scalar_layout,
                   std::vector<TypedSlot>&& optional_scalar_input_slots,
                   std::vector<TypedSlot>&& mapper_input_slots,
                   std::vector<FrameLayout::Slot<bool>>&& presence_slots,
                   std::vector<int>&& broadcast_arg_ids,
                   TypedSlot scalar_out_slot, TypedSlot mapper_output_slot)
      : mapper_bound_expr_(std::move(mapper_bound_expr)),
        input_slots_(input_slots.begin(), input_slots.end()),
        output_slot_(output_slot),
        scalar_layout_(std::move(scalar_layout)),
        optional_scalar_input_slots_(std::move(optional_scalar_input_slots)),
        mapper_input_slots_(std::move(mapper_input_slots)),
        presence_slots_(std::move(presence_slots)),
        broadcast_arg_ids_(std::move(broadcast_arg_ids)),
        scalar_out_slot_(scalar_out_slot),
        mapper_output_slot_(mapper_output_slot) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    // Construct FrameIterator.
    std::vector<TypedRef> input_arrays;
    input_arrays.reserve(input_slots_.size() - broadcast_arg_ids_.size());
    auto broadcast_id_iter = broadcast_arg_ids_.begin();
    for (size_t i = 0; i < input_slots_.size(); ++i) {
      if (broadcast_id_iter != broadcast_arg_ids_.end() &&
          *broadcast_id_iter == i) {
        broadcast_id_iter++;
      } else {
        input_arrays.push_back(TypedRef::FromSlot(input_slots_[i], frame));
      }
    }

    auto frame_iterator_or = FrameIterator::Create(
        input_arrays, optional_scalar_input_slots_, {output_slot_},
        {scalar_out_slot_}, &scalar_layout_,
        FrameIterator::Options{.buffer_factory = &ctx->buffer_factory()});
    if (!frame_iterator_or.ok()) {
      ctx->set_status(std::move(frame_iterator_or).status());
      return;
    }

    auto& frame_iterator = *frame_iterator_or;
    // Populate literals & scalar arguments.
    frame_iterator.CustomFrameInitialization([&](FramePtr scalar_frame) {
      mapper_bound_expr_->InitializeLiterals(ctx, scalar_frame);
      for (int arg_id : broadcast_arg_ids_) {
        DCHECK_LT(arg_id, input_slots_.size());
        DCHECK_LT(arg_id, mapper_input_slots_.size());
        input_slots_[arg_id].CopyTo(frame, mapper_input_slots_[arg_id],
                                    scalar_frame);
      }
    });

    // Evaluate the operator.
    if (presence_slots_.empty()) {
      DCHECK_EQ(scalar_out_slot_, mapper_output_slot_);
      // Here we don't care about presence bit because either scalar_out_slot_
      // is not optional, or the presence bit is set by the op itself.
      frame_iterator.ForEachFrame([&](FramePtr scalar_frame) {
        if (ctx->status().ok()) {
          mapper_bound_expr_->Execute(ctx, scalar_frame);
        }
      });
    } else {
      // TODO: This branch is not needed with the current
      // implementation of Expr-level operator. We need to either remove the
      // former, or extend the latter.
      auto presence_out_slot_or =
          GetPresenceSubslotFromOptional(scalar_out_slot_);
      if (!presence_out_slot_or.ok()) {
        ctx->set_status(std::move(presence_out_slot_or).status());
        return;
      }
      FrameLayout::Slot<bool>& presence_out_slot = *presence_out_slot_or;
      frame_iterator.ForEachFrame([&](FramePtr scalar_frame) {
        bool valid_args = true;
        for (auto slot : presence_slots_) {
          valid_args = valid_args && scalar_frame.Get(slot);
        }
        scalar_frame.Set(presence_out_slot, valid_args);
        if (valid_args && ctx->status().ok()) {
          mapper_bound_expr_->Execute(ctx, scalar_frame);
        }
      });
    }
    if (ctx->status().ok()) {
      ctx->set_status(frame_iterator.StoreOutput(frame));
    }
  }

 private:
  std::unique_ptr<BoundExpr> mapper_bound_expr_;
  std::vector<TypedSlot> input_slots_;
  TypedSlot output_slot_;
  FrameLayout scalar_layout_;

  // Scalar slots that will be passed to FrameIterator. Each slot correspond
  // to one of the input arrays we are iterating on. Slots are optional because
  // arrays can theoretically contain missed values. This vector doesn't contain
  // arguments from broadcast_arg_ids_, so the size can differ from the size of
  // mapper_input_slots_.
  std::vector<TypedSlot> optional_scalar_input_slots_;

  // Scalar inputs for the op. Differs from optional_scalar_input_slots_ if the
  // op has non-optional arguments.
  std::vector<TypedSlot> mapper_input_slots_;

  // Presence slots that correspond to those of mapper_input_slots_ that are
  // non-optional. op will be called only for rows where all the presence_slots_
  // contain `true`. The order of slots is not important.
  std::vector<FrameLayout::Slot<bool>> presence_slots_;

  // Indices of arguments that shouldn't be passed directly to evalund,
  // without using FrameIterator.
  std::vector<int> broadcast_arg_ids_;

  // Scalar slot that is used to construct the output array. It is optional if
  // either mapper_output_slot_ is optional or presence_slots_ is not empty.
  TypedSlot scalar_out_slot_;

  // Scalar output slot of the op operator.
  TypedSlot mapper_output_slot_;
};

// expr/eval extension to bind PackedCoreMapOperator
// (preprocessed core.map) operators.
std::optional<absl::Status> CompilePackedCoreMapOperator(
    const CompileOperatorFnArgs& args) {
  const auto* map_op =
      dynamic_cast<const PackedCoreMapOperator*>(args.op.get());
  if (map_op == nullptr) {
    return std::nullopt;
  }
  const auto& mapper = map_op->mapper();

  if (mapper.input_qtypes().size() != args.input_slots.size()) {
    return absl::InternalError(
        absl::StrFormat("unexpected number of input slots for packed map "
                        "operator with mapper %s: expected %d, got %d",
                        mapper.display_name(), mapper.input_qtypes().size(),
                        args.input_slots.size()));
  }

  FrameLayout::Builder scalar_layout_builder;

  // See comments to the corresponding private fields in MapBoundOperator.
  std::vector<TypedSlot> optional_scalar_input_slots;
  std::vector<TypedSlot> mapper_input_slots;
  std::vector<FrameLayout::Slot<bool>> presence_slots;
  std::vector<int> broadcast_arg_ids;
  optional_scalar_input_slots.reserve(mapper.input_qtypes().size());
  mapper_input_slots.reserve(mapper.input_qtypes().size());
  for (size_t i = 0; i < mapper.input_qtypes().size(); ++i) {
    const auto* input_type = mapper.input_qtypes()[i];
    if (input_type == args.input_slots[i].GetType()) {
      broadcast_arg_ids.push_back(i);
      auto slot = AddSlot(input_type, &scalar_layout_builder);
      mapper_input_slots.push_back(slot);
    } else if (IsOptionalQType(input_type)) {
      auto slot = AddSlot(input_type, &scalar_layout_builder);
      optional_scalar_input_slots.push_back(slot);
      mapper_input_slots.push_back(slot);
    } else {
      // TODO: This branch is not needed with the current
      // implementation of Expr-level operator. We need to either remove the
      // former, or extend the latter.
      ASSIGN_OR_RETURN(auto opt_input_type, ToOptionalQType(input_type));
      auto slot = AddSlot(opt_input_type, &scalar_layout_builder);
      optional_scalar_input_slots.push_back(slot);

      ASSIGN_OR_RETURN(FrameLayout::Slot<bool> presence_subslot,
                       GetPresenceSubslotFromOptional(slot));
      presence_slots.push_back(presence_subslot);
      ASSIGN_OR_RETURN(
          TypedSlot value_subslot,
          opt_input_type->value_qtype() != ::arolla::GetQType<Unit>()
              ? GetValueSubslotFromOptional(slot)
              // OptionalValue<Unit> does not contain a Unit slot, so we need
              // to create a fake one.
              : AddSlot(::arolla::GetQType<Unit>(), &scalar_layout_builder));
      mapper_input_slots.emplace_back(value_subslot);
    }
  }

  auto scalar_out_type = mapper.output_qtype();
  if (!presence_slots.empty() && !IsOptionalQType(scalar_out_type)) {
    ASSIGN_OR_RETURN(scalar_out_type, ToOptionalQType(scalar_out_type));
  }
  auto scalar_out_slot = AddSlot(scalar_out_type, &scalar_layout_builder);
  ASSIGN_OR_RETURN(auto mapper_output_slot,
                   scalar_out_type == mapper.output_qtype()
                       ? scalar_out_slot
                       : GetValueSubslotFromOptional(scalar_out_slot));

  ExecutableBuilder scalar_executable_builder(
      &scalar_layout_builder, args.options.collect_op_descriptions);
  RETURN_IF_ERROR(mapper.BindTo(scalar_executable_builder, mapper_input_slots,
                                mapper_output_slot));
  std::unique_ptr<BoundExpr> mapper_bound_expr =
      std::move(scalar_executable_builder)
          // We do not rely on mapper_bound_expr->input_slots(), so we don't
          // pass them.
          .Build(/*input_slots=*/{}, mapper_output_slot);

  std::string op_description;
  if (args.options.collect_op_descriptions) {
    auto dynamic_bound_expr =
        dynamic_cast<const DynamicBoundExpr*>(mapper_bound_expr.get());
    if (dynamic_bound_expr == nullptr) {
      return absl::InternalError("expected DynamicBoundExpr");
    }
    auto op_name = absl::StrFormat(
        "%s:init{%s}:eval{%s}", map_op->display_name(),
        absl::StrJoin(dynamic_bound_expr->init_op_descriptions(), "; "),
        absl::StrJoin(dynamic_bound_expr->eval_op_descriptions(), "; "));
    op_description =
        FormatOperatorCall(op_name, args.input_slots, {args.output_slot});
  }

  args.executable_builder->AddEvalOp(
      std::make_unique<MapBoundOperator>(
          std::move(mapper_bound_expr), args.input_slots, args.output_slot,
          std::move(scalar_layout_builder).Build(),
          std::move(optional_scalar_input_slots), std::move(mapper_input_slots),
          std::move(presence_slots), std::move(broadcast_arg_ids),
          scalar_out_slot, mapper_output_slot),
      op_description,
      /*display_name=*/"core.map");

  return absl::OkStatus();
}

AROLLA_INITIALIZER(
        .reverse_deps =
            {
                ::arolla::initializer_dep::kOperators,
                ::arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = [] {
          CompilerExtensionRegistry::GetInstance().RegisterCompileOperatorFn(
              CompilePackedCoreMapOperator);
        })

}  // namespace
}  // namespace arolla::expr::eval_internal
