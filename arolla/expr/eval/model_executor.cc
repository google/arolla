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
#include "arolla/expr/eval/model_executor.h"

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operators/bootstrap_operators.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/simple_executable.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/string.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::model_executor_impl {
namespace {

struct CompiledOutputCastings {
  std::unique_ptr<BoundExpr> casting_executable_expr;
  absl::flat_hash_map<std::string, TypedSlot> named_output_slots;
};

// NOTE: The function skips given_named_output_slots without corresponding
// desired_named_output_types.
absl::StatusOr<CompiledOutputCastings> CompiledOutputCastsIfNeeded(
    const ModelExecutorOptions& options, TypedSlot given_output_slot,
    const absl::flat_hash_map<std::string, TypedSlot>& given_named_output_slots,
    TypedSlot desired_output_slot,
    const absl::flat_hash_map<std::string, QTypePtr>&
        desired_named_output_types,
    FrameLayout::Builder& layout_builder) {
  constexpr absl::string_view kMainOutputLeafName = "main_output";
  // extra prefix to all casting inputs in order to avoid collision with
  // kMainOutputLeafName
  constexpr absl::string_view kSideOutputPrefix = "_";

  absl::flat_hash_map<std::string, TypedSlot> casting_input_slots;

  ExprNodePtr main_casting_expr;
  TypedSlot casting_expr_output_slot = TypedSlot::UnsafeFromOffset(
      GetQType<Unit>(), 0);  // Overwritten in the next block.
  absl::flat_hash_map<std::string, ExprNodePtr> named_output_casting_exprs;
  absl::flat_hash_map<std::string, TypedSlot> named_output_slots;
  for (const auto& [name, slot] : given_named_output_slots) {
    if (auto type_it = desired_named_output_types.find(name);
        type_it != desired_named_output_types.end()) {
      QTypePtr desired_qtype = type_it->second;
      if (desired_qtype != slot.GetType()) {
        std::string input_name = absl::StrCat(kSideOutputPrefix, name);
        ASSIGN_OR_RETURN(
            auto casted_named_output,
            expr_operators::CoreCast(Leaf(input_name), Literal(desired_qtype)));
        casting_input_slots.emplace(input_name, slot);
        // Populating named_output_casting_exprs instead of named_output_slots.
        named_output_casting_exprs.emplace(name, casted_named_output);
      } else {
        named_output_slots.emplace(name, slot);
      }
    }
  }
  if (!named_output_casting_exprs.empty() &&
      !options.allow_side_outputs_casting) {
    std::vector<std::string> names;
    names.reserve(named_output_casting_exprs.size());
    for (const auto& [name, _] : named_output_casting_exprs) {
      names.push_back(name);
    }
    std::sort(names.begin(), names.end());
    return absl::InvalidArgumentError(absl::StrCat(
        "side outputs casting is not allowed: ", absl::StrJoin(names, ", "),
        "; to fix add explicit `AllowSideOutputsCasting()` in model compiler"));
  }

  if (given_output_slot != desired_output_slot) {
    bool allow_casting = options.allow_output_casting;
    if (given_output_slot.GetType() == desired_output_slot.GetType()) {
      // ForNonOptionalOutput in combination with side outputs can cause extra
      // copying of the output slot (that is considered as casting to the same
      // type). We temporary allow it even if casting is disable in options.
      // TODO: Refactor ModelExecutor and fix ForceNonOptional
      allow_casting = true;
    }
    if (!allow_casting) {
      return absl::InvalidArgumentError(absl::StrCat(
          "output casting is not allowed: ",
          given_output_slot.GetType()->name(), " -> ",
          desired_output_slot.GetType()->name(),
          "; to fix add explicit `AllowOutputCasting()` in model compiler"));
    }
    ASSIGN_OR_RETURN(
        main_casting_expr,
        expr_operators::CoreCast(Leaf(kMainOutputLeafName),
                                 Literal(desired_output_slot.GetType())));
    casting_input_slots.emplace(std::string(kMainOutputLeafName),
                                given_output_slot);
    casting_expr_output_slot = desired_output_slot;
  } else {
    if (casting_input_slots.empty()) {  // no casting required
      return CompiledOutputCastings{nullptr, given_named_output_slots};
    }

    main_casting_expr = Literal(kUnit);
    casting_expr_output_slot = AddSlot(GetQType<Unit>(), &layout_builder);
  }

  ASSIGN_OR_RETURN(auto casting_executable_expr,
                   CompileAndBindForDynamicEvaluation(
                       options.eval_options, &layout_builder, main_casting_expr,
                       casting_input_slots,
                       /*output_slot=*/casting_expr_output_slot,
                       /*side_outputs=*/named_output_casting_exprs));
  named_output_slots.insert(
      casting_executable_expr->named_output_slots().begin(),
      casting_executable_expr->named_output_slots().end());
  return CompiledOutputCastings{std::move(casting_executable_expr),
                                named_output_slots};
}

// Wrapper around BoundExpr that implements `force_decay_optional`
// functionality.
class DecayOptionalBoundExpr : public BoundExpr {
 public:
  static std::unique_ptr<BoundExpr> Create(std::unique_ptr<BoundExpr> expr) {
    QTypePtr out_type = expr->output_slot().GetType();
    if (IsOptionalQType(out_type) && out_type->type_fields().size() == 2 &&
        out_type->type_fields()[0].GetType() == GetQType<bool>()) {
      return std::unique_ptr<BoundExpr>(
          new DecayOptionalBoundExpr(std::move(expr)));
    } else {
      return expr;
    }
  }

  void InitializeLiterals(EvaluationContext* ctx,
                          FramePtr frame) const override {
    expr_->InitializeLiterals(ctx, frame);
  }

  void Execute(EvaluationContext* ctx, FramePtr frame) const override {
    expr_->Execute(ctx, frame);
    if (!frame.Get(presence_) && ctx->status().ok()) {
      ctx->set_status(absl::FailedPreconditionError(
          "expects a present value, got missing"));
    }
  }

 private:
  explicit DecayOptionalBoundExpr(std::unique_ptr<BoundExpr> expr)
      : BoundExpr(expr->input_slots(), expr->output_slot().SubSlot(1),
                  expr->named_output_slots()),
        expr_(std::move(expr)),
        presence_(expr_->output_slot().SubSlot(0).UnsafeToSlot<bool>()) {}

  std::unique_ptr<BoundExpr> expr_;
  FrameLayout::Slot<bool> presence_;
};

// Wrapper around CompiledExpr that provides additional casting to output_type
// and side_output_types.
class CastingCompiledExpr : public CompiledExpr {
 public:
  CastingCompiledExpr(
      const CompiledExpr& compiled_expr, QTypePtr output_type,
      absl::flat_hash_map<std::string, QTypePtr> side_output_types,
      const ModelExecutorOptions& options)
      : CompiledExpr(compiled_expr.input_types(), output_type,
                     side_output_types),
        compiled_expr_(compiled_expr),
        options_(options) {}

  absl::StatusOr<std::unique_ptr<BoundExpr>> Bind(
      FrameLayout::Builder* layout_builder,
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      std::optional<TypedSlot> output_slot) const final {
    TypedSlot inner_output_slot =
        TypedSlot::UnsafeFromOffset(output_type(), 0);  // not initialized
    if (output_slot.has_value() &&
        output_slot->GetType() == compiled_expr_.output_type()) {
      inner_output_slot = *output_slot;
    } else {
      inner_output_slot = AddSlot(compiled_expr_.output_type(), layout_builder);
      if (!output_slot.has_value()) {
        if (output_type() == inner_output_slot.GetType()) {
          output_slot = inner_output_slot;
        } else if (options_.force_non_optional_output &&
                   output_type() ==
                       DecayOptionalQType(inner_output_slot.GetType()) &&
                   inner_output_slot.SubSlotCount() == 2) {
          output_slot = inner_output_slot.SubSlot(1);
        } else {
          output_slot = AddSlot(output_type(), layout_builder);
        }
      }
    }

    ASSIGN_OR_RETURN(
        std::unique_ptr<BoundExpr> main_executable_expr,
        compiled_expr_.Bind(layout_builder, input_slots, inner_output_slot));

    if (IsOptionalQType(compiled_expr_.output_type()) &&
        IsScalarQType(output_slot->GetType())) {
      if (options_.force_non_optional_output) {
        main_executable_expr =
            DecayOptionalBoundExpr::Create(std::move(main_executable_expr));
        inner_output_slot = main_executable_expr->output_slot();
      } else {
        return absl::InvalidArgumentError(
            "model output is deduced to optional, while non-optional is "
            "requested; to fix either wrap the desired output type with "
            "std::optional<...>/arolla::OptionalValue<...>, or pass "
            "ForceNonOptionalOutput() to model compiler, or make the model "
            "full");
      }
    }

    ASSIGN_OR_RETURN(
        (auto [casting_executable_expr, named_output_slots]),
        CompiledOutputCastsIfNeeded(options_, inner_output_slot,
                                    main_executable_expr->named_output_slots(),
                                    *output_slot, named_output_types(),
                                    *layout_builder),
        _ << "while casting model outputs due to `AllowOutputCasting()` or "
             "`AllowSideOutputsCasting()` options");
    if (casting_executable_expr == nullptr) {
      return main_executable_expr;
    } else {
      std::vector<std::unique_ptr<BoundExpr>> subexprs;
      subexprs.push_back(std::move(main_executable_expr));
      subexprs.push_back(std::move(casting_executable_expr));
      return std::make_unique<CombinedBoundExpr>(input_slots, *output_slot,
                                                 std::move(named_output_slots),
                                                 std::move(subexprs));
    }
  }

 private:
  const CompiledExpr& compiled_expr_;
  ModelExecutorOptions options_;
};

// absl::StrJoin formatter that returns the first element of std::pair.
struct FirstFormatter {
  template <typename Pair>
  void operator()(std::string* out, const Pair& p) const {
    out->append(p.first);
  }
};

}  // namespace

std::unique_ptr<CompiledExpr> CastOutputsIfNeeded(
    const CompiledExpr& expr, QTypePtr desired_output_type,
    absl::Nullable<const SlotListenerBase*> slot_listener,
    const ModelExecutorOptions& options) {
  absl::flat_hash_map<std::string, QTypePtr> side_output_types;
  side_output_types.reserve(expr.named_output_types().size());
  if (slot_listener != nullptr) {
    for (const auto& [name, desired_qtype] : expr.named_output_types()) {
      const QType* available_qtype =
          slot_listener->GetQTypeOf(name, desired_qtype);
      if (available_qtype != nullptr) {
        side_output_types.emplace(name, available_qtype);
      }
    }
  }
  return std::make_unique<CastingCompiledExpr>(
      std::move(expr), desired_output_type, side_output_types, options);
}

absl::Status VerifyAllNamedOutputsAreListened(
    const absl::flat_hash_map<std::string, QTypePtr>&
        available_named_output_types,
    const SlotListenerBase& slot_listener) {
  std::set<std::string> not_listened_named_outputs;
  for (const auto& [name, desired_qtype] : available_named_output_types) {
    if (slot_listener.GetQTypeOf(name, desired_qtype) == nullptr) {
      not_listened_named_outputs.emplace(name);
    }
  }
  if (!not_listened_named_outputs.empty()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "slot listener does not listen for named outputs {%s} (it listens to "
        "{%s}); check that output/export names of your nodes match the slot "
        "listener names (pay attention to slashes) or set "
        "IgnoreNotListenedNamedOutputs() to disable this check if you have "
        "a good reason",
        Truncate(absl::StrJoin(not_listened_named_outputs, ", "), 100),
        Truncate(absl::StrJoin(slot_listener.SuggestAvailableNames(), ", "),
                 100)));
  }
  return absl::OkStatus();
}

}  // namespace arolla::expr::model_executor_impl
