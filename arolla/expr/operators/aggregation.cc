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
#include "arolla/expr/operators/aggregation.h"

#include <vector>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "absl//types/span.h"
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
          "",  // TODO: doc-string
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
