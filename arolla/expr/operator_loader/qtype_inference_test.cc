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
#include "arolla/expr/operator_loader/qtype_inference.h"

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/bytes.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;

using Attr = ::arolla::expr::ExprAttributes;

absl::StatusOr<ExprNodePtr> is_scalar_qtype_expr(ExprNodePtr x) {
  return CallOp("core.equal", {x, CallOp("qtype.get_scalar_qtype", {x})});
}

template <typename... QTypePtrs>
Sequence MakeInputQTypeSequence(QTypePtrs... input_qtypes) {
  std::initializer_list<QTypePtr> tmp = {input_qtypes...};
  auto buffer = std::make_shared<QTypePtr[]>(tmp.size());
  std::replace_copy(tmp.begin(), tmp.end(), buffer.get(), QTypePtr{nullptr},
                    GetNothingQType());
  return Sequence(GetQTypeQType(), tmp.size(), std::move(buffer));
}

class QTypeInferenceTest : public ::testing::Test {
 protected:
  static absl::StatusOr<QTypeInferenceFn> SampleInferenceFn() {
    ASSIGN_OR_RETURN(auto x_is_scalar_qtype_expr,
                     is_scalar_qtype_expr(Placeholder("x")));
    ASSIGN_OR_RETURN(auto y_is_scalar_qtype_expr,
                     is_scalar_qtype_expr(Placeholder("y")));
    ASSIGN_OR_RETURN(
        auto x_y_common_qtype_expr,
        CallOp("qtype.common_qtype", {Placeholder("x"), Placeholder("y")}));
    return MakeQTypeInferenceFn(
        ExprOperatorSignature{{"x"}, {"y"}},
        {
            {x_is_scalar_qtype_expr, "expected `x` to be scalar, got {x}"},
            {y_is_scalar_qtype_expr, "expected `y` to be scalar, got {y}"},
        },
        x_y_common_qtype_expr);
  }
};

TEST_F(QTypeInferenceTest, Ok) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleInferenceFn());
  EXPECT_THAT(
      fn(MakeInputQTypeSequence(GetQType<int64_t>(), GetQType<int32_t>())),
      IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(fn(MakeInputQTypeSequence(GetQType<int32_t>())),
              IsOkAndHolds(nullptr));
  EXPECT_THAT(fn(MakeInputQTypeSequence(GetQType<int64_t>(), nullptr)),
              IsOkAndHolds(nullptr));
  EXPECT_THAT(fn(MakeInputQTypeSequence()), IsOkAndHolds(nullptr));
}

TEST_F(QTypeInferenceTest, ErrorMessage) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleInferenceFn());
  EXPECT_THAT(
      fn(MakeInputQTypeSequence(GetQType<int32_t>(), GetQType<ScalarShape>())),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected `y` to be scalar, got SCALAR_SHAPE")));
  EXPECT_THAT(
      fn(MakeInputQTypeSequence(GetQType<int32_t>(), GetQType<Bytes>())),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "operator attribute inference failure: qtype inference "
               "expression produced no qtype for complete operator "
               "inputs"));
}

TEST_F(QTypeInferenceTest, NoOutputQType) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("core.get_nth", {Placeholder("x"), Placeholder("y")}));
  EXPECT_THAT(
      MakeQTypeInferenceFn(ExprOperatorSignature{{"x"}, {"y"}}, {}, expr),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("problem with a qtype inference expression: "
                         "M.core.get_nth(P.x, P.y)")));
}

TEST_F(QTypeInferenceTest, BadOutputQType) {
  auto x = Literal(1.5f);
  EXPECT_THAT(
      MakeQTypeInferenceFn({}, {}, x),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("problem with a qtype inference expression: 1.5; "
                         "expected output qtype QTYPE, but got FLOAT32")));
}

}  // namespace
}  // namespace arolla::operator_loader
