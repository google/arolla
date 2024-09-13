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
#include "arolla/expr/operator_loader/backend_operator.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::testing::HasSubstr;

class BackendOperatorTest : public ::testing::Test {
 protected:
  absl::StatusOr<std::shared_ptr<const BackendOperator>> MakeOp() {
    ASSIGN_OR_RETURN(auto qtype_constraint_predicate_expr_1,
                     CallOp("core.not_equal", {CallOp("qtype.get_scalar_qtype",
                                                      {Placeholder("x")}),
                                               Literal(GetNothingQType())}));
    ASSIGN_OR_RETURN(auto qtype_constraint_predicate_expr_2,
                     CallOp("core.not_equal", {CallOp("qtype.get_scalar_qtype",
                                                      {Placeholder("y")}),
                                               Literal(GetNothingQType())}));
    ASSIGN_OR_RETURN(
        auto qtype_constraint_predicate_expr_3,
        CallOp("core.not_equal", {CallOp("qtype.broadcast_qtype_like",
                                         {Placeholder("y"), Placeholder("x")}),
                                  Literal(GetNothingQType())}));
    std::vector<QTypeConstraint> qtype_constraints = {
        {qtype_constraint_predicate_expr_1,
         "expected `x` to be a scalar based type, got {x}"},
        {qtype_constraint_predicate_expr_2,
         "expected `y` to be a UNIT based type, got {y}"},
        {qtype_constraint_predicate_expr_3,
         "incompatible types x:{x} and y:{y}"},
    };
    ASSIGN_OR_RETURN(auto qtype_inference_expr,
                     CallOp("qtype.broadcast_qtype_like",
                            {Placeholder("y"), Placeholder("x")}));
    ASSIGN_OR_RETURN(
        auto op, BackendOperator::Make(
                     "core.presence_and", ExprOperatorSignature{{"x"}, {"y"}},
                     "presence-and-doc-string", std::move(qtype_constraints),
                     std::move(qtype_inference_expr)));
    return std::dynamic_pointer_cast<const BackendOperator>(op);
  }
};

TEST_F(BackendOperatorTest, GetDoc) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  ASSERT_THAT(op.get()->doc(), "presence-and-doc-string");
  ASSERT_THAT(op->GetDoc(), IsOkAndHolds("presence-and-doc-string"));
}

TEST_F(BackendOperatorTest, QTypeInference) {
  {
    ASSERT_OK_AND_ASSIGN(auto expr,
                         CallOp(MakeOp(), {Literal(1.5f), Literal(kUnit)}));
    EXPECT_EQ(expr->qtype(), GetQType<float>());
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto expr,
        CallOp(MakeOp(), {Literal(1.5f), Literal(OptionalValue<Unit>())}));
    EXPECT_EQ(expr->qtype(), GetQType<OptionalValue<float>>());
  }
}

TEST_F(BackendOperatorTest, QTypeConstraint) {
  EXPECT_THAT(
      CallOp(MakeOp(), {Literal(MakeTupleFromFields()), Literal(kUnit)}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected `x` to be a scalar based type, got tuple<>")));
  EXPECT_THAT(
      CallOp(MakeOp(), {Literal(1.5f), Literal(MakeTupleFromFields())}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected `y` to be a UNIT based type, got tuple<>")));
  EXPECT_THAT(
      CallOp(MakeOp(), {Literal(Array<float>()), Literal(DenseArray<Unit>())}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "incompatible types x:ARRAY_FLOAT32 and y:DENSE_ARRAY_UNIT")));
}

TEST_F(BackendOperatorTest, Eval) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp(MakeOp(), {Literal(1.5f), Literal(OptionalValue<Unit>())}));
  ASSERT_OK_AND_ASSIGN(auto result_tv, Invoke(expr, {}));
  ASSERT_OK_AND_ASSIGN(auto result, result_tv.As<OptionalValue<float>>());
  EXPECT_EQ(result.get(), std::nullopt);
}

TEST_F(BackendOperatorTest, UnexpectedParameters) {
  ASSERT_OK_AND_ASSIGN(auto op, MakeOp());
  auto& backend_op = dynamic_cast<const BackendOperator&>(*op);

  EXPECT_THAT(BackendOperator::Make("core.presence_and",
                                    ExprOperatorSignature{{"a"}, {"b"}},
                                    "docstring", backend_op.qtype_constraints(),
                                    backend_op.qtype_inference_expr()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unexpected parameters: P.x, P.y")));
}

}  // namespace
}  // namespace arolla::operator_loader
