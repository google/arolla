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
#include "arolla/serving/inplace_expr_compiler.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/frame.h"
#include "arolla/naming/table.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::inplace_expr_compiler_impl {

namespace {

// Returns map from field name to its internal slot.
// Names are created using naming/table.h library.
TypedSlotMap CollectInternalSlots(TypedSlot root_slot) {
  TypedSlotMap result;
  if (GetFieldNames(root_slot.GetType()).empty()) {
    return result;
  }
  std::vector<std::pair<TypedSlot, naming::TablePath>> stack{{root_slot, {}}};
  while (!stack.empty()) {
    auto [slot, table] = stack.back();
    stack.pop_back();
    auto field_names = GetFieldNames(slot.GetType());
    for (size_t i = 0; i < field_names.size(); ++i) {
      const auto& field_name = field_names[i];
      const TypedSlot& field_slot = slot.SubSlot(i);
      result.emplace(table.Column(naming::FieldAccess(field_name)).FullName(),
                     field_slot);
      if (!GetFieldNames(field_slot.GetType()).empty()) {
        stack.emplace_back(field_slot,
                           table.Child(naming::FieldAccess(field_name)));
      }
    }
  }
  return result;
}

// Verifies that type for `field_name` in `slot_map` equals to `field_qtype`.
absl::Status CheckField(QTypePtr qtype, const TypedSlotMap& slot_map,
                        QTypePtr field_qtype, absl::string_view field_name) {
  if (GetFieldNames(qtype).empty()) {
    return absl::FailedPreconditionError(
        absl::StrCat("no registered field names for ", qtype->name(),
                     " in Compile.*ExprOnStructInput"));
  }
  if (!slot_map.contains(field_name)) {
    return absl::FailedPreconditionError(
        absl::StrCat("input `", field_name, "` not found in ", qtype->name(),
                     " in Compile.*ExprOnStructInput"));
  }
  QTypePtr result_type = slot_map.at(field_name).GetType();
  if (result_type != field_qtype) {
    return absl::FailedPreconditionError(absl::StrCat(
        "input `", field_name, "` type mismatch for ", qtype->name(),
        " in Compile.*ExprOnStructInput, expected in struct: ",
        result_type->name(), ", found in expr: ", field_qtype->name()));
  }
  return absl::OkStatus();
}

using ExprMap = absl::flat_hash_map<std::string, expr::ExprNodePtr>;

// Returns map from field name to the expression requesting that slot.
// Names are created using naming/table.h library.
absl::StatusOr<ExprMap> CollectFieldRequestExprs(QTypePtr root_qtype,
                                                 absl::string_view input_name) {
  ExprMap result;
  if (GetFieldNames(root_qtype).empty()) {
    return result;
  }
  ASSIGN_OR_RETURN(auto root_leaf, expr::CallOp("annotation.qtype",
                                                {expr::Leaf(input_name),
                                                 expr::Literal(root_qtype)}));
  std::vector<std::pair<expr::ExprNodePtr, naming::TablePath>> stack{
      {root_leaf, {}}};
  while (!stack.empty()) {
    auto [node, table] = stack.back();
    stack.pop_back();
    auto field_names = GetFieldNames(node->qtype());
    for (size_t i = 0; i < field_names.size(); ++i) {
      const auto& field_name = field_names[i];
      ASSIGN_OR_RETURN(expr::ExprNodePtr field_expr,
                       expr::CallOp("namedtuple.get_field",
                                    {node, expr::Literal(Text(field_name))}));
      result.emplace(table.Column(naming::FieldAccess(field_name)).FullName(),
                     field_expr);
      if (!GetFieldNames(field_expr->qtype()).empty()) {
        stack.emplace_back(field_expr,
                           table.Child(naming::FieldAccess(field_name)));
      }
    }
  }
  return result;
}

// Collects and verifies inner input slots for expression evaluation.
absl::StatusOr<TypedSlotMap> CollectInputSlots(
    QTypePtr qtype, const TypedSlotMap& struct_slot_map,
    const CompiledExpr& compiled_expr) {
  TypedSlotMap input_slots;
  input_slots.reserve(compiled_expr.input_types().size());
  for (const auto& [name, field_qtype] : compiled_expr.input_types()) {
    RETURN_IF_ERROR(CheckField(qtype, struct_slot_map, field_qtype, name));
    input_slots.emplace(name, struct_slot_map.at(name));
  }
  return input_slots;
}

// CompiledExpr with single struct input that deligates evaluation.
class StructInputDelegatingCompiledExpr : public CompiledExpr {
 public:
  StructInputDelegatingCompiledExpr(const CompiledExpr& compiled_expr,
                                    QTypePtr qtype, std::string input_name)
      : CompiledExpr({{input_name, qtype}}, compiled_expr.output_type(),
                     compiled_expr.named_output_types()),
        compiled_expr_(compiled_expr),
        qtype_(qtype),
        input_name_(input_name) {}

  absl::StatusOr<std::unique_ptr<BoundExpr>> Bind(
      FrameLayout::Builder* layout_builder,
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      std::optional<TypedSlot> output_slot) const final {
    RETURN_IF_ERROR(VerifySlotTypes(input_types(), input_slots));
    TypedSlotMap struct_slot_map =
        CollectInternalSlots(input_slots.at(input_name_));
    ASSIGN_OR_RETURN(
        TypedSlotMap inner_input_slots,
        CollectInputSlots(qtype_, struct_slot_map, compiled_expr_));
    return compiled_expr_.Bind(layout_builder, inner_input_slots, output_slot);
  }

 private:
  const CompiledExpr& compiled_expr_;
  QTypePtr qtype_;
  std::string input_name_;
};

}  // namespace

absl::StatusOr<IoSlots> CollectIoSlots(QTypePtr qtype,
                                       const CompiledExpr& compiled_expr,
                                       absl::string_view final_output_name) {
  TypedSlotMap struct_slot_map =
      CollectInternalSlots(TypedSlot::UnsafeFromOffset(qtype, 0));
  ASSIGN_OR_RETURN(TypedSlotMap input_slots,
                   CollectInputSlots(qtype, struct_slot_map, compiled_expr));
  RETURN_IF_ERROR(CheckField(qtype, struct_slot_map,
                             compiled_expr.output_type(), final_output_name));
  if (compiled_expr.input_types().contains(final_output_name)) {
    return absl::FailedPreconditionError(absl::StrCat(
        final_output_name, " present both as an input and as final output"));
  }
  if (compiled_expr.named_output_types().contains(final_output_name)) {
    return absl::FailedPreconditionError(
        absl::StrCat(final_output_name,
                     " present both as final output and as named output"));
  }
  for (const auto& [name, field_qtype] : compiled_expr.input_types()) {
    if (compiled_expr.named_output_types().contains(name)) {
      return absl::FailedPreconditionError(
          absl::StrCat(name, " present both as an input and as named output"));
    }
  }
  for (const auto& [name, field_qtype] : compiled_expr.named_output_types()) {
    RETURN_IF_ERROR(CheckField(qtype, struct_slot_map, field_qtype, name));
  }

  absl::flat_hash_map<std::string, TypedSlot> named_output_slots;
  named_output_slots.reserve(compiled_expr.named_output_types().size());
  for (const auto& [name, _] : compiled_expr.named_output_types()) {
    named_output_slots.emplace(name, struct_slot_map.at(name));
  }
  return IoSlots{.input_slots = input_slots,
                 .output_slot = struct_slot_map.at(final_output_name),
                 .named_output_slots = named_output_slots};
}

absl::StatusOr<expr::ExprNodePtr> TransformExprForInplaceEvaluation(
    const expr::ExprNodePtr& expr, QTypePtr qtype,
    absl::string_view input_name) {
  ASSIGN_OR_RETURN(ExprMap requester_map,
                   CollectFieldRequestExprs(qtype, input_name));
  return expr::SubstituteLeaves(expr, requester_map);
}

absl::StatusOr<std::unique_ptr<CompiledExpr>>
TransformCompiledExprForEvaluationOnStruct(const CompiledExpr& expr,
                                           QTypePtr qtype,
                                           absl::string_view input_name) {
  return std::make_unique<StructInputDelegatingCompiledExpr>(
      expr, qtype, std::string(input_name));
}

}  // namespace arolla::inplace_expr_compiler_impl
