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
#include "arolla/expr/operator_loader/qtype_constraint.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::testing::HasSubstr;

class QTypeConstraintTest : public ::testing::Test {
 protected:
  static absl::StatusOr<QTypeConstraintFn> SampleConstraintFn() {
    ASSIGN_OR_RETURN(auto x_is_scalar_qtype_expr,
                     CallOp("qtype.is_scalar_qtype", {Placeholder("x")}));
    ASSIGN_OR_RETURN(auto y_is_scalar_qtype_expr,
                     CallOp("qtype.is_scalar_qtype", {Placeholder("y")}));
    ASSIGN_OR_RETURN(
        auto x_y_has_common_qtype_expr,
        CallOp("core.not_equal", {CallOp("qtype.common_qtype",
                                         {Placeholder("x"), Placeholder("y")}),
                                  Literal(GetNothingQType())}));
    return MakeQTypeConstraintFn({
        {x_is_scalar_qtype_expr, "expected `x` to be scalar, got {x}"},
        {y_is_scalar_qtype_expr, "expected `y` to be scalar, got {y}"},
        {x_y_has_common_qtype_expr, "no common qtype for x:{x} and y:{y}"},
    });
  }

  static absl::StatusOr<QTypeConstraintFn> SampleConstraintWithVariadicFn() {
    auto false_expr = Literal(OptionalUnit{});
    return MakeQTypeConstraintFn({
        {false_expr, "*x: {*x}"},
    });
  }
};

TEST_F(QTypeConstraintTest, Trivial) {
  ASSERT_OK_AND_ASSIGN(auto fn, MakeQTypeConstraintFn({}));
  EXPECT_THAT(fn({}), IsOkAndHolds(true));
}

TEST_F(QTypeConstraintTest, Ok) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleConstraintFn());
  EXPECT_THAT(fn({
                  {"x", GetQType<int64_t>()},
                  {"y", GetQType<int32_t>()},
              }),
              IsOkAndHolds(true));
  EXPECT_THAT(fn({
                  {"x", GetQType<int64_t>()},
                  {"y", GetNothingQType()},
              }),
              IsOkAndHolds(false));
  EXPECT_THAT(fn({
                  {"x", GetNothingQType()},
                  {"y", GetQType<int32_t>()},
              }),
              IsOkAndHolds(false));
}

TEST_F(QTypeConstraintTest, ErrorMessage) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleConstraintFn());
  EXPECT_THAT(
      fn({
          {"x", GetQType<int64_t>()},
          {"y", GetQType<ScalarShape>()},
      }),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected `y` to be scalar, got SCALAR_SHAPE")));
  EXPECT_THAT(
      fn({
          {"x", GetNothingQType()},
          {"y", GetQType<ScalarShape>()},
      }),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected `y` to be scalar, got SCALAR_SHAPE")));
  EXPECT_THAT(fn({
                  {"x", GetQType<int32_t>()},
                  {"y", GetQType<Bytes>()},
              }),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common qtype for x:INT32 and y:BYTES")));
}

TEST_F(QTypeConstraintTest, NoOutputQType) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("core.get_nth", {Placeholder("x"), Placeholder("y")}));
  EXPECT_THAT(
      MakeQTypeConstraintFn({{expr, ""}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("error while computing output QType of a QType "
                         "constraint predicate: "
                         "M.core.get_nth(P.x, P.y)")));
}

TEST_F(QTypeConstraintTest, BadOutputQType) {
  auto x = Placeholder("x");
  EXPECT_THAT(MakeQTypeConstraintFn({{x, ""}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected a constraint predicate to return "
                                 "OPTIONAL_UNIT, got QTYPE: P.x")));
}

TEST_F(QTypeConstraintTest, VariadicConstraint) {
  ASSERT_OK_AND_ASSIGN(auto fn, SampleConstraintWithVariadicFn());
  EXPECT_THAT(
      fn({{"x", MakeTupleQType({})}}),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("*x: ()")));
  EXPECT_THAT(fn({
                  {"x", MakeTupleQType({GetQType<int32_t>(), GetQType<float>(),
                                        GetQType<bool>()})},
              }),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("*x: (INT32, FLOAT32, BOOLEAN)")));
  EXPECT_THAT(  // non-tuple
      fn({
          {"x", GetQType<int64_t>()},
      }),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("*x: {*x}")));
}

}  // namespace
}  // namespace arolla::operator_loader
