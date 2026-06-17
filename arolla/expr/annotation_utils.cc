// Copyright 2025 Google LLC
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
#include "arolla/expr/annotation_utils.h"

#include <cstdint>
#include <optional>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/class_info.h"
#include "arolla/util/text.h"

namespace arolla::expr {

absl::StatusOr<bool> IsAnnotation(const ExprNodePtr absl_nonnull& node) {
  if (node->node_deps().empty()) {
    return false;
  }
  ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
  return HasAnnotationExprOperatorTag(op);
}

absl::StatusOr<ExprNodePtr absl_nonnull> StripTopmostAnnotations(
    ExprNodePtr absl_nonnull expr) {
  ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(expr));
  while (is_annotation) {
    expr = expr->node_deps()[0];
    ASSIGN_OR_RETURN(is_annotation, IsAnnotation(expr));
  }
  return expr;
}

absl::StatusOr<ExprNodePtr absl_nonnull> StripAnnotations(  // clang-format hint
    const ExprNodePtr absl_nonnull& expr) {
  return Transform(
      expr,
      [](const ExprNodePtr absl_nonnull& node)  // clang-format hint
      -> absl::StatusOr<ExprNodePtr absl_nonnull> {
        ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(node));
        DCHECK(!is_annotation ||  // IsAnnotation() checks node_deps size.
               !node->node_deps().empty());
        return is_annotation ? node->node_deps()[0] : node;
      });
}

bool IsQTypeAnnotation(const ExprNodePtr absl_nonnull& node) {
  if (node->node_deps().size() != 2) {
    return false;
  }
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  return IsInstanceOf<QTypeAnnotation>(op.get());
}

bool IsNameAnnotation(const ExprNodePtr absl_nonnull& node) {
  if (node->node_deps().size() != 2) {
    return false;
  }
  const auto& name_qvalue = node->node_deps()[1]->qvalue();
  if (!name_qvalue.has_value() || name_qvalue->GetType() != GetQType<Text>()) {
    return false;
  }
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  return IsInstanceOf<NameAnnotation>(op.get());
}

bool IsExportAnnotation(const ExprNodePtr absl_nonnull& node) {
  if (node->node_deps().size() < 2 || node->node_deps().size() > 3) {
    return false;
  }
  const auto& tag_qvalue = node->node_deps()[1]->qvalue();
  if (!tag_qvalue.has_value() || tag_qvalue->GetType() != GetQType<Text>()) {
    return false;
  }
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  return (IsInstanceOf<ExportAnnotation>(op.get()) &&
          node->node_deps().size() == 2) ||
         (IsInstanceOf<ExportValueAnnotation>(op.get()) &&
          node->node_deps().size() == 3);
}

QTypePtr absl_nullable ReadQTypeAnnotation(  // clang-format hint
    const ExprNodePtr absl_nonnull& node) {
  if (IsQTypeAnnotation(node)) {
    DCHECK_EQ(node->node_deps().size(), 2);
    if (const auto& qvalue = node->node_deps()[1]->qvalue()) {
      if (qvalue->GetType() == GetQTypeQType()) {
        return qvalue->UnsafeAs<QTypePtr>();
      }
    }
  }
  return nullptr;
}

absl::string_view ReadNameAnnotation(const ExprNodePtr absl_nonnull& node) {
  if (IsNameAnnotation(node)) {
    // Note: This chain of access is safe because
    // everything was verified in IsNameAnnotation().
    return node->node_deps()[1]->qvalue()->UnsafeAs<Text>().view();
  }
  return {};
}

absl::string_view ReadExportAnnotationTag(  // clang-format hint
    const ExprNodePtr absl_nonnull& node) {
  if (IsExportAnnotation(node)) {
    // Note: This chain of access is safe because
    // everything was verified in IsExportAnnotation().
    return node->node_deps()[1]->qvalue()->UnsafeAs<Text>().view();
  }
  return {};
}

ExprNodePtr absl_nullable ReadExportAnnotationValue(  // clang-format hint
    const ExprNodePtr absl_nonnull& node) {
  if (IsExportAnnotation(node)) {
    if (node->node_deps().size() == 2) {
      // annotation.export(expr, tag)
      return node->node_deps()[0];
    } else if (node->node_deps().size() == 3) {
      // annotation.export_value(expr, tag, value)
      return node->node_deps()[2];
    }
  }
  return nullptr;
}

absl::Status VerifySourceLocationType(
    QTypePtr absl_nullable source_location_type) {
  if (source_location_type == nullptr) {
    return absl::InvalidArgumentError(
        "expected a source location namedtuple, got nullptr");
  }
  if (!IsNamedTupleQType(source_location_type)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a namedtuple literal, got: %s",
        source_location_type->name()));
  }
  const auto* named_tuple_interface =
      dynamic_cast<const NamedFieldQTypeInterface*>(source_location_type);
  auto validate_field =
      [&](absl::string_view name, QTypePtr expected_type) -> absl::Status {
    std::optional<int64_t> idx =
        named_tuple_interface->GetFieldIndexByName(name);
    if (!idx.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("missing field: %s", name));
    }
    // NOLINTNEXTLINE
    QTypePtr actual_type = source_location_type->type_fields()[*idx].GetType();
    if (actual_type != expected_type) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "field %s in source_location must be %s, got %s", name,
          expected_type->name(), actual_type->name()));
    }
    return absl::OkStatus();
  };
  RETURN_IF_ERROR(validate_field("function_name", GetQType<Text>()));
  RETURN_IF_ERROR(validate_field("file_name", GetQType<Text>()));
  RETURN_IF_ERROR(validate_field("line", GetQType<int32_t>()));
  RETURN_IF_ERROR(validate_field("column", GetQType<int32_t>()));
  RETURN_IF_ERROR(validate_field("line_text", GetQType<Text>()));
  return absl::OkStatus();
}

std::optional<SourceLocationView> ReadSourceLocationAnnotation(
    const ExprNodePtr absl_nonnull& node) {
  if (node->node_deps().size() != 2) {
    return std::nullopt;
  }
  const auto& loc_qvalue = node->node_deps()[1]->qvalue();
  if (!loc_qvalue.has_value()) {
    return std::nullopt;
  }
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  if (!IsInstanceOf<SourceLocationAnnotation>(op.get())) {
    return std::nullopt;
  }

  const auto* named_tuple_interface =
      dynamic_cast<const NamedFieldQTypeInterface*>(loc_qvalue->GetType());
  if (named_tuple_interface == nullptr) {
    return std::nullopt;
  }
  auto get_text =
      [&](absl::string_view field_name) -> std::optional<absl::string_view> {
    std::optional<int64_t> field_index =
        named_tuple_interface->GetFieldIndexByName(field_name);
    if (field_index.has_value()) {
      auto field_qvalue = loc_qvalue->GetField(*field_index);
      if (field_qvalue.GetType() == GetQType<Text>()) {
        return field_qvalue.UnsafeAs<Text>().view();
      }
    }
    return std::nullopt;
  };
  auto get_int32 = [&](absl::string_view field_name) -> std::optional<int32_t> {
    std::optional<int64_t> field_index =
        named_tuple_interface->GetFieldIndexByName(field_name);
    if (field_index.has_value()) {
      auto field_qvalue = loc_qvalue->GetField(*field_index);
      if (field_qvalue.GetType() == GetQType<int32_t>()) {
        return field_qvalue.UnsafeAs<int32_t>();
      }
    }
    return std::nullopt;
  };

  auto function_name = get_text("function_name");
  auto file_name = get_text("file_name");
  auto line = get_int32("line");
  auto column = get_int32("column");
  auto line_text = get_text("line_text");

  if (!function_name.has_value() || !file_name.has_value() ||
      !line.has_value() || !column.has_value() || !line_text.has_value()) {
    return std::nullopt;
  }

  return SourceLocationView{.function_name = *function_name,
                            .file_name = *file_name,
                            .line = *line,
                            .column = *column,
                            .line_text = *line_text};
}

}  // namespace arolla::expr
