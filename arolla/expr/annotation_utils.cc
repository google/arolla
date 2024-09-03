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
#include "arolla/expr/annotation_utils.h"

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<bool> IsAnnotation(const ExprNodePtr& node) {
  ASSIGN_OR_RETURN(auto op, DecayRegisteredOperator(node->op()));
  return !node->node_deps().empty() &&
         dynamic_cast<const AnnotationExprOperatorTag*>(op.get()) != nullptr;
}

absl::StatusOr<ExprNodePtr> StripTopmostAnnotations(ExprNodePtr expr) {
  ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(expr));
  while (is_annotation) {
    expr = expr->node_deps()[0];
    ASSIGN_OR_RETURN(is_annotation, IsAnnotation(expr));
  }
  return expr;
}

absl::StatusOr<ExprNodePtr> StripAnnotations(const ExprNodePtr& expr) {
  return Transform(
      expr, [](const ExprNodePtr& node) -> absl::StatusOr<ExprNodePtr> {
        ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(node));
        DCHECK(!is_annotation ||  // IsAnnotation() checks node_deps size.
               !node->node_deps().empty());
        return is_annotation ? node->node_deps()[0] : node;
      });
}

bool IsQTypeAnnotation(const ExprNodePtr& node) {
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  return op != nullptr && typeid(*op) == typeid(QTypeAnnotation) &&
         node->node_deps().size() == 2;
}

bool IsNameAnnotation(const ExprNodePtr& node) {
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  if (op == nullptr || typeid(*op) != typeid(NameAnnotation) ||
      node->node_deps().size() != 2) {
    return false;
  }
  const auto& qvalue = node->node_deps()[1]->qvalue();
  return qvalue.has_value() && qvalue->GetType() == GetQType<Text>();
}

bool IsExportAnnotation(const ExprNodePtr& node) {
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  if (op == nullptr || ((typeid(*op) != typeid(ExportAnnotation) ||
                         node->node_deps().size() != 2) &&
                        (typeid(*op) != typeid(ExportValueAnnotation) ||
                         node->node_deps().size() != 3))) {
    return false;
  }
  const auto& qvalue = node->node_deps()[1]->qvalue();
  return qvalue.has_value() && qvalue->GetType() == GetQType<Text>();
}

const QType* /*nullable*/ ReadQTypeAnnotation(const ExprNodePtr& node) {
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

absl::string_view ReadNameAnnotation(const ExprNodePtr& node) {
  if (IsNameAnnotation(node)) {
    // Note: This chain of access is safe because
    // everything was verified in IsNameAnnotation().
    return node->node_deps()[1]->qvalue()->UnsafeAs<Text>().view();
  }
  return {};
}

absl::string_view ReadExportAnnotationTag(const ExprNodePtr& node) {
  if (IsExportAnnotation(node)) {
    // Note: This chain of access is safe because
    // everything was verified in IsExportAnnotation().
    return node->node_deps()[1]->qvalue()->UnsafeAs<Text>().view();
  }
  return {};
}

ExprNodePtr /*nullable*/ ReadExportAnnotationValue(const ExprNodePtr& node) {
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

}  // namespace arolla::expr
