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
#include "arolla/expr/operators/factory_operators.h"

#include <cstdint>
#include <memory>

#include "absl//status/statusor.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;

using NarrowestNumericType = int32_t;

// core.empty_like operator implementation.
class EmptyLikeOp final : public expr::BasicExprOperator {
 public:
  EmptyLikeOp()
      : BasicExprOperator(
            "core.empty_like", ExprOperatorSignature{{"target"}},
            "Creates an empty value with shape and (optional) type like "
            "target.",
            FingerprintHasher("arolla::expr_operators::EmptyLikeOp").Finish()) {
  }
  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    auto target_qtype = node->node_deps()[0]->qtype();
    ASSIGN_OR_RETURN(auto scalar_qtype, GetScalarQType(target_qtype));
    ASSIGN_OR_RETURN(auto optional_scalar_qtype, ToOptionalQType(scalar_qtype));
    ASSIGN_OR_RETURN(auto missing, CreateMissingValue(optional_scalar_qtype));
    return CallOp("core.const_like", {node->node_deps()[0], Literal(missing)});
  }
  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    return ToOptionalLikeQType(input_qtypes[0]);
  }
};

}  // namespace

absl::StatusOr<ExprOperatorPtr> MakeEmptyLikeOp() {
  return std::make_shared<EmptyLikeOp>();
}

}  // namespace arolla::expr_operators
