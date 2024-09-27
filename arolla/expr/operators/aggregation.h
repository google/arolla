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
#ifndef AROLLA_EXPR_OPERATORS_AGGREGATION_H_
#define AROLLA_EXPR_OPERATORS_AGGREGATION_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr_operators {

// Takes an array `x`, an array of offsets and two edges (for `x` and for
// the offsets respectively). The size of each array must be equal to the detail
// size of the corresponding edge. The second edge is optional: by default it
// is the same as the first one.
// Returns an array by taking values from `x` in the order of the offsets. The
// offsets are specified w.r.t. to the groups defined by the edges. e.g. an
// offset of 2 means taking the third element of the group this offset is in.
class TakeOperator : public expr::BasicExprOperator {
 public:
  TakeOperator();
  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override;
  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const override;
};

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_AGGREGATION_H_
