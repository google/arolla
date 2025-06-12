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
#ifndef AROLLA_EXPR_QTYPE_UTILS_H_
#define AROLLA_EXPR_QTYPE_UTILS_H_

#include <optional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr {

// Collects QTypes of the expression leaves from annotations.
absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> CollectLeafQTypes(
    ExprNodePtr expr);

// Collects QType of the expression leaves from annotations.
//
// `post_order` must contain nodes of the expression in post-order.
absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>>
CollectLeafQTypesOnPostOrder(const PostOrder& post_order);

// Assigns QType information to leaves and populates qtypes for the entire
// expression.
//
// `get_qtype` accepts a leaf name and returns a QType for this leaf, or nullptr
// if there is no information.
//
absl::StatusOr<ExprNodePtr> PopulateQTypes(
    ExprNodePtr expr,
    absl::FunctionRef<const QType* absl_nullable(absl::string_view)> get_qtype,
    bool allow_incomplete_type_information = false);

// Assigns QType information to leaves and populates qtypes for the entire
// expression.
absl::StatusOr<ExprNodePtr> PopulateQTypes(
    ExprNodePtr expr,
    const absl::flat_hash_map<std::string, QTypePtr>& leaf_qtypes,
    bool allow_incomplete_type_information = false);

// Reads QType attributes assigned to the nodes.
std::vector<const QType*> GetExprQTypes(absl::Span<const ExprNodePtr> nodes);

// Extracts QValues attributes assigned to the nodes.
std::vector<std::optional<TypedValue>> GetExprQValues(
    absl::Span<const ExprNodePtr> nodes);

// For operators like math.sum(x), some arguments might not be specified by
// the user. At ExprOperatorSignature creation time we do not have access to the
// concrete arguments to the node, which we need for the "actual default value".
// Because of this, we assign it a special value of kUnit which signifies that
// the true value needs to be determined in toLower.
bool IsDefaultEdgeArg(const ExprNodePtr& arg);

// Returns true if node is a group scalar edge.
// Return false if node is a normal edge.
// Returns Error Status if node is not of type edge.
absl::StatusOr<bool> IsGroupScalarEdge(const ExprNodePtr& edge);

// Returns attributes stored in the given expression nodes.
std::vector<ExprAttributes> GetExprAttrs(absl::Span<const ExprNodePtr> nodes);

// Returns qtypes stored in the given attributes.
std::vector<const QType* /*nullable*/> GetAttrQTypes(
    absl::Span<const ExprAttributes> attrs);

// Returns value qtypes for the given qtypes.
std::vector<const QType* /*nullable*/> GetValueQTypes(
    absl::Span<const QTypePtr> qtypes);

// Returns true, if attr.qtype() != nullptr for all attr in attrs.
bool HasAllAttrQTypes(absl::Span<const ExprAttributes> attrs);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_QTYPE_UTILS_H_
