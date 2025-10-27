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
#include "arolla/expr/operators/aggregation.h"

#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::IsDefaultEdgeArg;
using ::arolla::expr::IsGroupScalarEdge;

TakeOperator::TakeOperator()
    : BasicExprOperator(
          "array.take",
          ExprOperatorSignature(
              {{"x"},
               {"ids"},
               {.name = "over", .default_value = TypedValue::FromValue(kUnit)},
               {.name = "ids_over",
                .default_value = TypedValue::FromValue(kUnit)}}),
          "Takes elements from `x` based on group-wise indices specified in "
          "`ids`.\n"
          "\n"
          "The groups are defined by the edge `over` for `x` and by the edge "
          "`ids_over` for `ids`.\n"
          "\n"
          "Example 1 (`x` and `ids` have the same size):\n"
          " array.take(x=[10, 20, 30, 40], ids=[0, 1 , 1, 0], "
          "edge.from_sizes([2, 2])) -> [10, 20, 40, 30]\n"
          "\n"
          "Example 2 (`x` and `ids` have different sizes):\n"
          " array.take(x=[10, 20, 30, 40], ids=[0, 0, 1, 1, 0, 1], "
          "edge.from_sizes([2, 2], edge.from_sizes([3, 3])), "
          "ids_edge.from_sizes([4])) -> [10, 10, 20, 40, 30, 40]\n"
          "\n"
          "Args:\n"
          "  x: An array of values. Return values will be taken from here.\n"
          "  ids: An array of integer values. Represents the ids from which to "
          "take values from `x`. The ids are 0-based w.r.t. the groups. Their "
          "values should be in the range [0, group_size) for each group.\n"
          "  over: (optional) An edge defining the mapping from `x` to groups. "
          "Child size should match the size of `x`. If not specified, treats "
          "everything as part of the same group.\n"
          "  ids_over: (optional) An edge defining the mapping from `ids` to "
          "groups. Child size should match the size of `ids`. Parent size "
          "should match the parent size of `over`(the number of groups). If "
          "not specified, the same edge as `over` is used.\n"
          "\n"
          "Returns:\n"
          "  An array matching the size of `ids` with the elements taken from "
          "`x`.\n",
          FingerprintHasher("arolla::expr_operators::TakeOperator").Finish()) {}

absl::StatusOr<ExprNodePtr> TakeOperator::ToLowerLevel(
    const ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  const auto& node_deps = node->node_deps();
  DCHECK_GE(node_deps.size(), 4);  // Validated by `ValidateNodeDepsCount`.
  const ExprNodePtr& values = node_deps[0];
  const ExprNodePtr& offsets = node_deps[1];
  ExprNodePtr values_edge = node_deps[2];
  ExprNodePtr offsets_edge = node_deps[3];
  bool is_scalar_values_edge = IsDefaultEdgeArg(values_edge);
  if (!is_scalar_values_edge) {
    ASSIGN_OR_RETURN(is_scalar_values_edge, IsGroupScalarEdge(values_edge));
  }
  bool is_scalar_offsets_edge = IsDefaultEdgeArg(offsets_edge);
  if (!is_scalar_offsets_edge) {
    ASSIGN_OR_RETURN(is_scalar_offsets_edge, IsGroupScalarEdge(offsets_edge));
  }
  if (is_scalar_values_edge != is_scalar_offsets_edge) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Two edges must share the parent side but only one of them is an edge "
        "to scalar. is_scalar_values_edge(=%d) != is_scalar_offsets_edge(=%d)",
        is_scalar_values_edge, is_scalar_offsets_edge));
  }
  if (is_scalar_values_edge) {
    // Optimization for to-scalar edges.
    return CallOp("array.at", {values, offsets});
  }
  if (values_edge->fingerprint() == offsets_edge->fingerprint()) {
    // Optimization for identical edges.
    return CallOp("array._take_over", {values, offsets, values_edge});
  }
  return CallOp("array._take_over_over",
                {values, offsets, values_edge, offsets_edge});
}

absl::StatusOr<QTypePtr> TakeOperator::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  return input_qtypes[0];
}

}  // namespace arolla::expr_operators
