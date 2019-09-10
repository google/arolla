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

#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
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

absl::StatusOr<bool> IsDetachedAnnotation(const ExprNodePtr& node) {
  ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(node));
  DCHECK(!is_annotation ||
         !node->node_deps().empty());  // IsAnnotation() checks node_deps size.
  return is_annotation && node->node_deps()[0]->is_placeholder();
}

absl::StatusOr<ExprNodePtr> GetDetachedAnnotation(ExprNodePtr node) {
  ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(node));
  if (!is_annotation) {
    return absl::InvalidArgumentError(
        absl::StrCat("can not detach annotation from ", GetDebugSnippet(node),
                     " that is not a valid annotation node"));
  }
  auto new_deps = node->node_deps();
  DCHECK(!new_deps.empty());  // IsAnnotation() checks node_deps size.
  new_deps[0] = Placeholder("_");
  return WithNewDependencies(node, std::move(new_deps));
}

absl::StatusOr<ExprNodePtr> AttachAnnotation(const ExprNodePtr& node,
                                             const ExprNodePtr& annotation) {
  ASSIGN_OR_RETURN(bool is_detached_annotation,
                   IsDetachedAnnotation(annotation));
  if (!is_detached_annotation) {
    return absl::InvalidArgumentError(absl::StrCat(
        "can not attach a node that is not a detached annotation: %s",
        GetDebugSnippet(node)));
  }
  auto new_deps = annotation->node_deps();
  DCHECK(!new_deps.empty());  // IsDetachedAnnotation() checks node_deps size.
  new_deps[0] = node;
  return WithNewDependencies(annotation, std::move(new_deps));
}

absl::StatusOr<ExprNodePtr> AttachAnnotations(
    const ExprNodePtr& node, absl::Span<const ExprNodePtr> annotations) {
  ExprNodePtr annotated_node = node;
  for (const auto& anno : annotations) {
    ASSIGN_OR_RETURN(annotated_node, AttachAnnotation(annotated_node, anno));
  }
  return annotated_node;
}

absl::StatusOr<ExprNodePtr> StripTopmostAnnotations(const ExprNodePtr& expr) {
  ExprNodePtr annotationless_expr = expr;
  ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(annotationless_expr));
  while (is_annotation) {
    if (annotationless_expr->node_deps().empty()) {
      return absl::FailedPreconditionError(
          absl::StrFormat("incorrect annotation node %s",
                          GetDebugSnippet(annotationless_expr)));
    }
    annotationless_expr = annotationless_expr->node_deps()[0];
    ASSIGN_OR_RETURN(is_annotation, IsAnnotation(annotationless_expr));
  }
  return annotationless_expr;
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
  return op != nullptr && typeid(*op) == typeid(NameAnnotation) &&
         node->node_deps().size() == 2;
}

bool IsExportAnnotation(const ExprNodePtr& node) {
  auto op = DecayRegisteredOperator(node->op()).value_or(nullptr);
  return op != nullptr && ((typeid(*op) == typeid(ExportAnnotation) &&
                            node->node_deps().size() == 2) ||
                           (typeid(*op) == typeid(ExportValueAnnotation) &&
                            node->node_deps().size() == 3));
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
    DCHECK_EQ(node->node_deps().size(), 2);
    if (const auto& qvalue = node->node_deps()[1]->qvalue()) {
      if (qvalue->GetType() == GetQType<Text>()) {
        return qvalue->UnsafeAs<Text>().view();
      }
    }
  }
  return "";
}

absl::string_view ReadExportAnnotationTag(const ExprNodePtr& node) {
  if (IsExportAnnotation(node)) {
    // Must be verified in InferAttr.
    DCHECK_GE(node->node_deps().size(), 2);
    if (node->node_deps()[1]->qvalue().has_value() &&
        node->node_deps()[1]->qvalue()->GetType() == GetQType<Text>()) {
      return node->node_deps()[1]->qvalue()->UnsafeAs<Text>().view();
    }
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
