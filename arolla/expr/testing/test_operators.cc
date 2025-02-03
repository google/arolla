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
#include "arolla/expr/testing/test_operators.h"

#include <cstdint>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/casting_registry.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::testing {

absl::StatusOr<QTypePtr> DummyOp::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  return GetQType<int32_t>();
}

TernaryAddOp::TernaryAddOp()
    : BasicExprOperator(
          "TernaryAddOp", ExprOperatorSignature::MakeArgsN(3),
          "A testing operator: test.add3",
          FingerprintHasher("arolla::expr::testing::TernaryAddOp").Finish()) {}

absl::StatusOr<ExprNodePtr> TernaryAddOp::ToLowerLevel(
    const ExprNodePtr& node) const {
  const auto& deps = node->node_deps();
  CHECK_EQ(3, deps.size());
  ASSIGN_OR_RETURN(ExprNodePtr add12, CallOp("math.add", {deps[0], deps[1]}));
  return CallOp("math.add", {add12, deps[2]});
}

absl::StatusOr<QTypePtr> TernaryAddOp::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  return expr_operators::CastingRegistry::GetInstance()->CommonType(
      input_qtypes);
}

AddFourOp::AddFourOp()
    : BasicExprOperator(
          "AddFourOp", ExprOperatorSignature::MakeArgsN(4),
          "A testing operator: test.add4",
          FingerprintHasher("arolla::expr::testing::AddFourOp").Finish()) {}

absl::StatusOr<ExprNodePtr> AddFourOp::ToLowerLevel(
    const ExprNodePtr& node) const {
  const auto& deps = node->node_deps();
  CHECK_EQ(4, deps.size());
  ASSIGN_OR_RETURN(ExprNodePtr add123,
                   CallOp("test.add3", {deps[0], deps[1], deps[2]}));
  return CallOp("math.add", {add123, deps[3]});
}

absl::StatusOr<QTypePtr> AddFourOp::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  return expr_operators::CastingRegistry::GetInstance()->CommonType(
      input_qtypes);
}

PowerOp::PowerOp()
    : BasicExprOperator(
          "PowerOp", ExprOperatorSignature{{"x"}, {"power"}},
          "A testing operator: test.power",
          FingerprintHasher("arolla::expr::testing::PowerOp").Finish()) {}

absl::StatusOr<QTypePtr> PowerOp::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  if (input_qtypes[0] != GetQType<int>() &&
      input_qtypes[0] != GetQType<float>() &&
      input_qtypes[1] != GetQType<int>() &&
      input_qtypes[1] != GetQType<float>()) {
    return absl::FailedPreconditionError("Power() expects scalar input types.");
  }
  return GetQType<float>();
}

}  // namespace arolla::expr::testing
