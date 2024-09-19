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
#include "arolla/expr/operator_loader/qtype_inference.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using expr::CallOp;
using expr::Literal;
using expr::Placeholder;

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

class QTypeInferenceTest : public ::testing::Test {
 protected:
  static absl::StatusOr<QTypeInferenceFn> SampleInferenceFn() {
    ASSIGN_OR_RETURN(auto x_is_scalar_qtype_expr,
                     CallOp("qtype.is_scalar_qtype", {Placeholder("x")}));
    ASSIGN_OR_RETURN(auto y_is_scalar_qtype_expr,
                     CallOp("qtype.is_scalar_qtype", {Placeholder("y")}));
    ASSIGN_OR_RETURN(
        auto x_y_common_qtype_expr,
        CallOp("qtype.common_qtype", {Placeholder("x"), Placeholder("y")}));
    return MakeQTypeInferenceFn(
        {
            {x_is_scalar_qtype_expr, "expected `x` to be scalar, got {x}"},
            {y_is_scalar_qtype_expr, "expected `y` to be scalar, got {y}"},
        },
        x_y_common_qtype_expr);
  }
};

TEST_F(QTypeInferenceTest, Ok) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleInferenceFn());
  EXPECT_THAT(fn({
                  {"x", GetQType<int64_t>()},
                  {"y", GetQType<int32_t>()},
              }),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(fn({
                  {"y", GetQType<int32_t>()},
              }),
              IsOkAndHolds(nullptr));
  EXPECT_THAT(fn({
                  {"x", GetQType<int64_t>()},
                  {"y", GetNothingQType()},
              }),
              IsOkAndHolds(nullptr));
  EXPECT_THAT(fn({}), IsOkAndHolds(nullptr));
}

TEST_F(QTypeInferenceTest, ErrorMessage) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleInferenceFn());
  EXPECT_THAT(
      fn({
          {"x", GetQType<int32_t>()},
          {"y", GetQType<ScalarShape>()},
      }),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected `y` to be scalar, got SCALAR_SHAPE")));
  EXPECT_THAT(
      fn({
          {"x", GetQType<int32_t>()},
          {"y", GetQType<Bytes>()},
      }),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(
                   "qtype inference expression produced no "
                   "qtype: M.qtype.common_qtype(P.x, P.y), x:INT32, y:BYTES")));
}

TEST_F(QTypeInferenceTest, NoOutputQType) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("core.get_nth", {Placeholder("x"), Placeholder("y")}));
  EXPECT_THAT(
      MakeQTypeInferenceFn({}, expr),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Error while computing output QType of a QType inference "
                    "expression: M.core.get_nth(P.x, P.y)")));
}

TEST_F(QTypeInferenceTest, BadOutputQType) {
  auto x = Literal(1.f);
  EXPECT_THAT(MakeQTypeInferenceFn({}, x),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected a qtype inference expression to "
                                 "return QTYPE, got FLOAT32: 1.")));
}

}  // namespace
}  // namespace arolla::operator_loader
