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
#include "arolla/expr/qtype_utils.h"

#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> CollectLeafQTypes(
    ExprNodePtr expr) {
  return CollectLeafQTypesOnPostOrder(PostOrder(expr));
}

absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>>
CollectLeafQTypesOnPostOrder(const PostOrder& post_order) {
  absl::flat_hash_map<std::string, QTypePtr> result;
  std::vector<absl::string_view> leaf_keys(post_order.nodes_size());
  for (size_t i = 0; i < post_order.nodes_size(); ++i) {
    const auto node = post_order.node(i);
    if (node->is_leaf()) {
      leaf_keys[i] = node->leaf_key();
      continue;
    }
    // Find current leaf key (works for annotation).
    const auto node_dep_indices = post_order.dep_indices(i);
    if (node_dep_indices.empty() || leaf_keys[node_dep_indices[0]].empty()) {
      continue;
    }
    const absl::string_view leaf_key = leaf_keys[node_dep_indices[0]];
    // Propagate leaf key if it's an annotation.
    ASSIGN_OR_RETURN(bool is_annotation, IsAnnotation(node));
    if (is_annotation) {
      leaf_keys[i] = leaf_key;
    }
    // Collect qtype if it's a qtype annotation.
    auto* qtype = ReadQTypeAnnotation(node);
    if (qtype == nullptr) {
      continue;
    }
    if (auto it = result.try_emplace(leaf_key, qtype).first;
        it->second != qtype) {
      return absl::InvalidArgumentError(
          absl::StrFormat("inconsistent qtype annotations for L.%s: %s != %s",
                          leaf_key, qtype->name(), it->second->name()));
    }
  }
  return result;
}

absl::StatusOr<ExprNodePtr> PopulateQTypes(
    ExprNodePtr expr,
    absl::FunctionRef<absl::Nullable<const QType*>(absl::string_view)>
        get_qtype,
    bool allow_incomplete_type_information) {
  const auto post_order = PostOrder(expr);

  // Collect qtypes from the expression.
  ASSIGN_OR_RETURN(auto expr_leaf_qtypes,
                   CollectLeafQTypesOnPostOrder(post_order));

  // Update expression.
  std::set<absl::string_view> untyped_leaves;
  ASSIGN_OR_RETURN(
      ExprNodePtr result,
      TransformOnPostOrder(
          post_order, [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
            if (node->is_leaf()) {
              if (auto qtype = get_qtype(node->leaf_key()); qtype != nullptr) {
                return CallOp(QTypeAnnotation::Make(),
                              {std::move(node), Literal(qtype)});
              }
              if (auto it = expr_leaf_qtypes.find(node->leaf_key());
                  it != expr_leaf_qtypes.end()) {
                return CallOp(QTypeAnnotation::Make(),
                              {std::move(node), Literal(it->second)});
              }
              if (!allow_incomplete_type_information) {
                untyped_leaves.insert(node->leaf_key());
              }
              return node;
            }
            if (IsQTypeAnnotation(node) && !node->node_deps().empty()) {
              const auto& arg = node->node_deps().front();
              if (arg->qtype() != nullptr) {
                return arg;
              }
            }
            return node;
          }));
  if (!untyped_leaves.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("QType for the leaves {%s} are missing, which may be "
                        "caused by missing input features",
                        absl::StrJoin(untyped_leaves, ", ")));
  }
  return result;
}

absl::StatusOr<ExprNodePtr> PopulateQTypes(
    ExprNodePtr expr,
    const absl::flat_hash_map<std::string, QTypePtr>& leaf_qtypes,
    bool allow_incomplete_type_information) {
  return PopulateQTypes(
      expr,
      [&](absl::string_view leaf_key) {
        auto it = leaf_qtypes.find(leaf_key);
        return it == leaf_qtypes.end() ? nullptr : it->second;
      },
      allow_incomplete_type_information);
}

const QType* GetExprQType(ExprNodePtr node) { return node->qtype(); }

std::vector<const QType*> GetExprQTypes(absl::Span<const ExprNodePtr> nodes) {
  std::vector<const QType*> qtypes;
  qtypes.reserve(nodes.size());
  for (const auto& dep : nodes) {
    qtypes.push_back(dep->qtype());
  }
  return qtypes;
}

std::vector<std::optional<TypedValue>> GetExprQValues(
    absl::Span<const ExprNodePtr> nodes) {
  std::vector<std::optional<TypedValue>> result;
  result.reserve(nodes.size());
  for (const auto& dep : nodes) {
    result.emplace_back(dep->qvalue());
  }
  return result;
}

namespace {

bool IsDefaultEdgeQType(const QTypePtr& arg_qtype) {
  return arg_qtype == GetQType<Unit>();
}

}  // namespace

bool IsDefaultEdgeArg(const ExprNodePtr& arg) {
  return IsDefaultEdgeQType(arg->qtype());
}

absl::StatusOr<bool> IsGroupScalarEdge(const ExprNodePtr& edge) {
  if (edge == nullptr) {
    return absl::FailedPreconditionError(
        "Null node pointer passed to IsGroupScalarEdge.");
  }
  auto edge_type = edge->qtype();
  ASSIGN_OR_RETURN(auto identified_edge_type, ToEdgeQType(edge_type));
  ASSIGN_OR_RETURN(auto shape_type,
                   ToShapeQType(identified_edge_type->parent_shape_qtype()));
  return shape_type == GetQType<OptionalScalarShape>();
}

std::vector<ExprAttributes> GetExprAttrs(absl::Span<const ExprNodePtr> nodes) {
  std::vector<ExprAttributes> result;
  result.reserve(nodes.size());
  for (const auto& node : nodes) {
    result.push_back(node->attr());
  }
  return result;
}

std::vector<const QType*> GetAttrQTypes(
    absl::Span<const ExprAttributes> attrs) {
  std::vector<const QType*> result;
  result.reserve(attrs.size());
  for (const auto& attr : attrs) {
    result.push_back(attr.qtype());
  }
  return result;
}

std::vector<const QType*> GetValueQTypes(absl::Span<const QTypePtr> qtypes) {
  std::vector<const QType*> result;
  result.reserve(qtypes.size());
  for (const auto qtype : qtypes) {
    result.push_back(qtype->value_qtype());
  }
  return result;
}

bool HasAllAttrQTypes(absl::Span<const ExprAttributes> attrs) {
  for (const auto& attr : attrs) {
    if (!attr.qtype()) {
      return false;
    }
  }
  return true;
}

}  // namespace arolla::expr
