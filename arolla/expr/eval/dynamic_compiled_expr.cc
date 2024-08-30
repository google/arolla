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
#include "arolla/expr/eval/dynamic_compiled_expr.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/eval/compile_std_function_operator.h"
#include "arolla/expr/eval/compile_where_operator.h"
#include "arolla/expr/eval/compile_while_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/eval/slot_allocator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_stack_trace.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/expr/operators/while_loop/while_loop.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/core/utility_operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/demangle.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

const OperatorDirectory& GetOperatorDirectory(
    const DynamicEvaluationEngineOptions& options) {
  return options.operator_directory != nullptr
             ? *options.operator_directory
             : *OperatorRegistry::GetInstance();
}

struct OutputInfo {
  ExprNodePtr expr;
  TypedSlot forced_output_slot;
};

absl::Status VerifySlotsCount(absl::string_view op_name,
                              absl::Span<const TypedSlot> input_slots,
                              int64_t expected_count) {
  if (input_slots.size() != expected_count) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s operator expects %d argument(s), got %d", op_name,
                        expected_count, input_slots.size()));
  }
  return absl::OkStatus();
}

class EvalVisitor {
 public:
  EvalVisitor(DynamicEvaluationEngineOptions options,
              const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
              OutputInfo output_info, ExecutableBuilder* executable_builder,
              const std::vector<std::string>& side_output_names,
              absl::flat_hash_map<Fingerprint, QTypePtr> node_types,
              eval_internal::SlotAllocator& slot_allocator)
      : options_(std::move(options)),
        expr_input_slots_(input_slots),
        output_info_(std::move(output_info)),
        executable_builder_(executable_builder),
        side_output_names_(side_output_names),
        node_types_(std::move(node_types)),
        slot_allocator_(slot_allocator),
        compiler_extensions_(CompilerExtensionRegistry::GetInstance()
                                 .GetCompilerExtensionSet()) {}

  absl::StatusOr<TypedSlot> operator()(
      const ExprNodePtr& node, absl::Span<const TypedSlot* const> visits) {
    auto inputs = DereferenceVisitPointers(visits);
    ASSIGN_OR_RETURN(QTypePtr output_type, LookupQType(node, node_types_));
    if (output_type == nullptr) {
      return absl::FailedPreconditionError(
          absl::StrFormat("unable to deduce output type of the node %s",
                          GetDebugSnippet(node)));
    }
    ASSIGN_OR_RETURN(
        TypedSlot output_slot, ConstructOutputSlot(node, inputs, output_type),
        _ << "while compiling node " << GetDebugSnippet(node)
          << "; the expression is likely not fully compiled and is using "
             "derived operators that are not supported in the backend");

    if (output_slot.GetType() != output_type) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "unexpected output type of the node %s: MetaEval: %s, "
          "backend: %s; operator signatures "
          "are inconsistent on argument types %s",
          GetDebugSnippet(node), output_type->name(),
          output_slot.GetType()->name(),
          FormatTypeVector(SlotsToTypes(inputs))));
    }

    // Inputs for InternalRootOperator are named side outputs so cannot be
    // released. Also its first dep writes to `output_slot` that is not known to
    // `slot_allocator_`.
    if (node->op() != eval_internal::InternalRootOperator()) {
      RETURN_IF_ERROR(slot_allocator_.ReleaseSlotsNotNeededAfter(node));
    }
    return output_slot;
  }

 private:
  using AddSlotFn = std::function<TypedSlot(bool allow_recycled)>;
  using CopySlotFn = std::function<absl::StatusOr<TypedSlot>(
      TypedSlot slot, const ExprNodePtr& slot_origin)>;

  absl::StatusOr<TypedSlot> ConstructOutputSlot(
      const ExprNodePtr& node, absl::Span<const TypedSlot> input_slots,
      QTypePtr output_type) {
    std::optional<TypedSlot> forced_output_slot;
    if (node.get() == output_info_.expr.get()) {
      forced_output_slot = output_info_.forced_output_slot;
    }
    AddSlotFn maybe_add_output_slot =
        [this, output_type, &node, forced_output_slot](bool allow_recycled) {
          if (forced_output_slot.has_value()) {
            return *forced_output_slot;
          }
          auto slot =
              slot_allocator_.AddSlotForNode(node, output_type, allow_recycled);
          return slot;
        };
    CopySlotFn maybe_copy_slot =
        [this, &node, forced_output_slot](
            TypedSlot slot,
            const ExprNodePtr& slot_origin) -> absl::StatusOr<TypedSlot> {
      if (forced_output_slot.has_value()) {
        RETURN_IF_ERROR(this->executable_builder_
                            ->BindEvalOp(*MakeCopyOp(slot.GetType()), {slot},
                                         *forced_output_slot, "core._copy")
                            .status());
        slot = *forced_output_slot;
      } else {
        RETURN_IF_ERROR(slot_allocator_.ExtendSlotLifetime(slot_origin, node));
      }
      return slot;
    };
    switch (node->type()) {
      case ExprNodeType::kPlaceholder:
        return absl::InternalError(  // Verified in Compile
            absl::StrFormat("placeholder should be substituted before "
                            "evaluation: P.%s",
                            node->placeholder_key()));
      case ExprNodeType::kLeaf: {
        if (!expr_input_slots_.contains(node->leaf_key())) {
          return absl::InvalidArgumentError(
              absl::StrCat("unbound leaf: ", node->leaf_key()));
        }
        return maybe_copy_slot({expr_input_slots_.at(node->leaf_key())}, node);
      }
      case ExprNodeType::kLiteral: {
        // We add slots for literals unconditionally (instead of using
        // maybe_add_output_slot), because if they are used as outputs, the
        // literal value may be accidentally overwritten or moved-out.
        TypedSlot output_slot =
            slot_allocator_.AddSlotForNode(node, output_type,
                                           /*allow_recycled=*/false);
        RETURN_IF_ERROR(executable_builder_->AddLiteralInitialization(
            *node->qvalue(), output_slot));
        return maybe_copy_slot(output_slot, node);
      }
      case ExprNodeType::kOperator: {
        ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
        if (!HasBuiltinExprOperatorTag(op) && !HasBackendExprOperatorTag(op)) {
          return absl::InvalidArgumentError(
              absl::StrCat(node->op()->display_name(),
                           " is not a builtin or backend ExprOperator"));
        }
        const auto& op_typeid = typeid(*op);
        if (HasBackendExprOperatorTag(op)) {
          if (op->display_name() == "core.has._optional") {
            // FIXME: Remove the special handling for 'core.has'.
            return HandleHas(node->node_deps(), input_slots, maybe_copy_slot,
                             maybe_add_output_slot);
          }
          return CompileBackendOperator(
              op->display_name(), input_slots,
              maybe_add_output_slot(/*allow_recycled=*/true), node);
        } else if (HasAnnotationExprOperatorTag(op)) {
          return maybe_copy_slot(input_slots[0], node->node_deps()[0]);
        } else if (op == eval_internal::InternalRootOperator()) {
          return HandleInternalRoot(input_slots);
        } else if (op_typeid == typeid(GetNthOperator)) {
          return HandleGetNth(op, node->node_deps(), input_slots,
                              maybe_copy_slot);
        } else if (auto* where_op =
                       fast_dynamic_downcast_final<const PackedWhereOp*>(
                           op.get())) {
          DynamicEvaluationEngineOptions options(options_);
          options.allow_overriding_input_slots = false;
          return CompileWhereOperator(
              options, *where_op, input_slots,
              maybe_add_output_slot(/*allow_recycled=*/true),
              executable_builder_);
        } else if (auto* while_op = fast_dynamic_downcast_final<
                       const expr_operators::WhileLoopOperator*>(op.get())) {
          DynamicEvaluationEngineOptions options(options_);
          options.allow_overriding_input_slots = false;
          auto output_slot = maybe_add_output_slot(/*allow_recycled=*/true);
          RETURN_IF_ERROR(eval_internal::CompileWhileOperator(
              options, *while_op, input_slots, output_slot,
              *executable_builder_));
          return output_slot;
        } else if (op_typeid == typeid(DerivedQTypeUpcastOperator) ||
                   op_typeid == typeid(DerivedQTypeDowncastOperator)) {
          return HandleDerivedQTypeCast(*op, node->node_deps(), input_slots,
                                        maybe_copy_slot);
        } else if (auto* std_function_op =
                       dynamic_cast<const expr_operators::StdFunctionOperator*>(
                           op.get())) {
          auto output_slot = maybe_add_output_slot(/*allow_recycled=*/true);
          RETURN_IF_ERROR(eval_internal::CompileStdFunctionOperator(
              *std_function_op, input_slots, output_slot, *executable_builder_,
              node));
          return output_slot;
        }

        auto output_slot = maybe_add_output_slot(/*allow_recycled=*/true);
        if (auto result =
                compiler_extensions_.compile_operator_fn(CompileOperatorFnArgs{
                    .options = options_,
                    .op = op,
                    .input_slots = input_slots,
                    .output_slot = output_slot,
                    .executable_builder = executable_builder_});
            result.has_value()) {
          RETURN_IF_ERROR(*result);
          return output_slot;
        }

        return absl::InvalidArgumentError(absl::StrCat(
            "unsupported builtin ExprOperator: name=",
            node->op()->display_name(), ", CxxType=", TypeName(op_typeid)));
      }
    }
    return absl::InternalError(absl::StrFormat("unexpected ExprNodeType: %d",
                                               static_cast<int>(node->type())));
  }

  absl::StatusOr<TypedSlot> HandleInternalRoot(
      absl::Span<const TypedSlot> input_slots) const {
    if (input_slots.size() != 1 + side_output_names_.size()) {
      return absl::InternalError(
          absl::StrFormat("InternalRootOperator bound with %d "
                          "arguments, %d expected",
                          input_slots.size(), 1 + side_output_names_.size()));
    }
    if (input_slots[0] != output_info_.forced_output_slot) {
      // we expect InternalRootOperator to be actual output
      return absl::InternalError(
          "InternalRootOperator first slot was handled incorrectly");
    }
    for (size_t i = 0; i < side_output_names_.size(); ++i) {
      RETURN_IF_ERROR(executable_builder_->AddNamedOutput(side_output_names_[i],
                                                          input_slots[i + 1]));
    }
    return input_slots[0];
  }

  absl::StatusOr<TypedSlot> HandleHas(absl::Span<const ExprNodePtr> node_deps,
                                      absl::Span<const TypedSlot> input_slots,
                                      CopySlotFn copy_slot_fn,
                                      AddSlotFn add_slot_fn) {
    RETURN_IF_ERROR(VerifySlotsCount("core.has._optional", input_slots, 1));
    if (!IsOptionalQType(input_slots[0].GetType())) {
      return CompileBackendOperator("core.has._optional", input_slots,
                                    add_slot_fn(/*allow_recycled=*/true));
    }

    static_assert(sizeof(OptionalUnit) == sizeof(bool));
    static_assert(alignof(OptionalUnit) == alignof(bool));
    auto mask_slot = FrameLayout::Slot<OptionalUnit>::UnsafeSlotFromOffset(
        input_slots[0].byte_offset());
    // Prevent "unregistered slot" error.
    RETURN_IF_ERROR(executable_builder_->layout_builder()->RegisterUnsafeSlot(
        mask_slot, /*allow_duplicates=*/true));
    DCHECK_EQ(node_deps.size(), 1);
    return copy_slot_fn(TypedSlot::FromSlot(mask_slot), node_deps[0]);
  }

  absl::StatusOr<TypedSlot> HandleGetNth(
      const ExprOperatorPtr& op, absl::Span<const ExprNodePtr> node_deps,
      absl::Span<const TypedSlot> input_slots, CopySlotFn copy_slot_fn) const {
    RETURN_IF_ERROR(VerifySlotsCount(op->display_name(), input_slots, 1));
    const GetNthOperator& get_nth =
        *static_cast<const GetNthOperator*>(op.get());
    if (get_nth.index() < 0 ||
        get_nth.index() >= input_slots[0].SubSlotCount()) {
      return absl::InternalError(  // Must not happen in a valid expression.
          absl::StrFormat("input type %s is not compatible with %s, index %d "
                          "is out of range",
                          input_slots[0].GetType()->name(),
                          get_nth.display_name(), get_nth.index()));
    }
    DCHECK_EQ(node_deps.size(), 1);
    return copy_slot_fn(input_slots[0].SubSlot(get_nth.index()), node_deps[0]);
  }

  absl::StatusOr<TypedSlot> HandleDerivedQTypeCast(
      const ExprOperator& op, absl::Span<const ExprNodePtr> node_deps,
      absl::Span<const TypedSlot> input_slots, CopySlotFn copy_slot_fn) const {
    RETURN_IF_ERROR(VerifySlotsCount(op.display_name(), input_slots, 1));
    DCHECK(typeid(op) == typeid(DerivedQTypeUpcastOperator) ||
           typeid(op) == typeid(DerivedQTypeDowncastOperator));
    // Type propagation for DerivedQType[Up,Down]castOperator does not
    // depend on the literal value, so it's ok to pass just qtype.
    ASSIGN_OR_RETURN(
        auto output_attr,
        op.InferAttributes({ExprAttributes(input_slots[0].GetType())}));
    DCHECK_EQ(node_deps.size(), 1);
    DCHECK(output_attr.qtype());
    return copy_slot_fn(TypedSlot::UnsafeFromOffset(
                            output_attr.qtype(), input_slots[0].byte_offset()),
                        node_deps[0]);
  }

  absl::StatusOr<TypedSlot> CompileBackendOperator(
      absl::string_view name, absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot, absl::Nullable<ExprNodePtr> node = nullptr) {
    ASSIGN_OR_RETURN(
        auto op, GetOperatorDirectory(options_).LookupOperator(
                     name, SlotsToTypes(input_slots), output_slot.GetType()));
    ASSIGN_OR_RETURN(auto ip, executable_builder_->BindEvalOp(
                                  *op, input_slots, output_slot, name));
    if (node != nullptr) {
      executable_builder_->RegisterStacktrace(ip, node);
    }
    return output_slot;
  }

  DynamicEvaluationEngineOptions options_;
  const absl::flat_hash_map<std::string, TypedSlot>& expr_input_slots_;
  OutputInfo output_info_;
  ExecutableBuilder* executable_builder_;
  const std::vector<std::string>& side_output_names_;
  absl::flat_hash_map<Fingerprint, QTypePtr> node_types_;
  eval_internal::SlotAllocator& slot_allocator_;
  CompilerExtensionSet compiler_extensions_;
};

}  // namespace

DynamicCompiledExpr::DynamicCompiledExpr(
    DynamicEvaluationEngineOptions options,
    absl::flat_hash_map<std::string, QTypePtr> input_types,
    QTypePtr output_type,
    absl::flat_hash_map<std::string, QTypePtr> named_output_types,
    ExprNodePtr prepared_expr, std::vector<std::string> side_output_names,
    absl::flat_hash_map<Fingerprint, QTypePtr> types,
    std::shared_ptr<const ExprStackTrace> stack_trace)
    : CompiledExpr(std::move(input_types), output_type,
                   std::move(named_output_types)),
      options_(std::move(options)),
      prepared_expr_(std::move(prepared_expr)),
      side_output_names_(std::move(side_output_names)),
      types_(std::move(types)),
      stack_trace_(std::move(stack_trace)) {}

absl::StatusOr<std::unique_ptr<BoundExpr>> DynamicCompiledExpr::Bind(
    FrameLayout::Builder* layout_builder,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    std::optional<TypedSlot> output_slot) const {
  ExecutableBuilder executable_builder(
      layout_builder,
      /*collect_op_descriptions=*/options_.collect_op_descriptions,
      stack_trace_);
  if (!output_slot.has_value()) {
    output_slot = AddSlot(output_type(), layout_builder);
  }
  RETURN_IF_ERROR(
      BindToExecutableBuilder(executable_builder, input_slots, *output_slot));
  return std::move(executable_builder).Build(input_slots, *output_slot);
}

absl::Status DynamicCompiledExpr::BindToExecutableBuilder(
    ExecutableBuilder& executable_builder,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    TypedSlot output_slot) const {
  RETURN_IF_ERROR(VerifySlotTypes(input_types(), input_slots,
                                  /*verify_unwanted_slots=*/false,
                                  /*verify_missed_slots=*/true));

  // Special handling for InternalRootOperator.
  ExprNodePtr output_expr = prepared_expr_;
  if (output_expr->op() == eval_internal::InternalRootOperator()) {
    if (output_expr->node_deps().empty()) {
      return absl::InternalError("InternalRootOperator bound with 0 arguments");
    }
    output_expr = output_expr->node_deps()[0];
  }

  eval_internal::SlotAllocator slot_allocator(
      prepared_expr_, *executable_builder.layout_builder(), input_slots,
      /*allow_reusing_leaves=*/options_.allow_overriding_input_slots);
  EvalVisitor visitor(options_, input_slots, {output_expr, output_slot},
                      &executable_builder, side_output_names_, types_,
                      slot_allocator);
  ASSIGN_OR_RETURN(TypedSlot new_output_slot,
                   PostOrderTraverse(prepared_expr_, std::ref(visitor)));
  if (output_slot != new_output_slot) {
    return absl::InternalError(
        absl::StrFormat("expression %s bound to a wrong output slot",
                        GetDebugSnippet(prepared_expr_)));
  }
  return absl::OkStatus();
}

}  // namespace arolla::expr::eval_internal
