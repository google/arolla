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
#include "arolla/expr/operator_loader/parameter_qtypes.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::operator_loader {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using Attr = ::arolla::expr::ExprAttributes;

TEST(ParameterQTypesTest, ExtractParameterQTypes_NonVariadicSignature) {
  ASSERT_OK_AND_ASSIGN(auto sig,
                       ExprOperatorSignature::Make("first, second=", kUnit));
  EXPECT_THAT(
      ExtractParameterQTypes(
          sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()}}),
      IsOkAndHolds(UnorderedElementsAre(Pair("first", GetQType<int32_t>()),
                                        Pair("second", GetQType<float>()))));
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{GetNothingQType()}, Attr{}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "inputs of type NOTHING are unsupported"));
  EXPECT_THAT(ExtractParameterQTypes(sig, {}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "unexpected number of inputs"));
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "unexpected number of inputs"));
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{}, Attr{}, Attr{}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "unexpected number of inputs"));
}

TEST(ParameterQTypesTest, ExtractParameterQTypes_VariadicSignature) {
  ASSERT_OK_AND_ASSIGN(
      auto sig, ExprOperatorSignature::Make("first, second=, *args", kUnit));
  EXPECT_THAT(
      ExtractParameterQTypes(
          sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()}}),
      IsOkAndHolds(UnorderedElementsAre(Pair("first", GetQType<int32_t>()),
                                        Pair("second", GetQType<float>()),
                                        Pair("args", MakeTupleQType({})))));
  EXPECT_THAT(
      ExtractParameterQTypes(
          sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()},
                Attr{GetQType<Bytes>()}}),
      IsOkAndHolds(UnorderedElementsAre(
          Pair("first", GetQType<int32_t>()), Pair("second", GetQType<float>()),
          Pair("args", MakeTupleQType({GetQType<Bytes>()})))));
  EXPECT_THAT(
      ExtractParameterQTypes(
          sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()},
                Attr{GetQType<Bytes>()}, Attr{GetQType<Text>()}}),
      IsOkAndHolds(UnorderedElementsAre(
          Pair("first", GetQType<int32_t>()), Pair("second", GetQType<float>()),
          Pair("args",
               MakeTupleQType({GetQType<Bytes>(), GetQType<Text>()})))));
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{GetNothingQType()}, Attr{}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "inputs of type NOTHING are unsupported"));
  EXPECT_THAT(ExtractParameterQTypes(sig, {}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "unexpected number of inputs"));
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{}}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "unexpected number of inputs"));
}

TEST(ParameterQTypesTest, MakeParameterQTypeModelExecutor) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("core.make_tuple", {Leaf("first"), Leaf("second"), Leaf("args")}));
  ASSERT_OK_AND_ASSIGN(auto executor, MakeParameterQTypeModelExecutor(expr));

  {
    ASSERT_OK_AND_ASSIGN(auto result, executor({}));
    EXPECT_THAT(result.GetFingerprint(),
                MakeTupleFromFields(GetNothingQType(), GetNothingQType(),
                                    GetNothingQType())
                    .GetFingerprint());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto result,
                         executor({
                             {"first", GetQType<int32_t>()},
                             {"second", GetQType<float>()},
                             {"args", MakeTupleQType({GetQType<Bytes>()})},
                         }));
    EXPECT_THAT(result.GetFingerprint(),
                MakeTupleFromFields(GetQType<int32_t>(), GetQType<float>(),
                                    MakeTupleQType({GetQType<Bytes>()}))
                    .GetFingerprint());
  }
}

TEST(ParameterQTypesTest, FormatParameterQTypes) {
  EXPECT_EQ(FormatParameterQTypes({
                {"i32", GetQType<int32_t>()},
                {"i64", GetQType<int64_t>()},
                {"f32", GetQType<float>()},
                {"f64", GetQType<double>()},
                {"unit", GetQType<Unit>()},
                {"qtype", GetQType<QTypePtr>()},
            }),
            ("f32:FLOAT32, f64:FLOAT64, "
             "i32:INT32, i64:INT64, "
             "qtype:QTYPE, unit:UNIT"));
}

TEST(ParameterQTypesTest, FormatParameterQTypes_WithMessage_Simple) {
  EXPECT_EQ(FormatParameterQTypes("x is {x}, y is {y}",
                                  {
                                      {"x", GetQType<int32_t>()},
                                      {"y", GetQType<float>()},
                                  }),
            "x is INT32, y is FLOAT32");
}

TEST(ParameterQTypesTest, FormatParameterQTypes_WithMessage_Tuple) {
  EXPECT_EQ(
      FormatParameterQTypes(
          "args: {args}, *args: ({*args})",
          {{"args", MakeTupleQType({GetQType<int32_t>(), GetQType<float>()})}}),
      "args: tuple<INT32,FLOAT32>, *args: (INT32, FLOAT32)");
}

TEST(ParameterQTypesTest, FormatParameterQTypes_WithMessage_NamedTuple) {
  ASSERT_OK_AND_ASSIGN(
      auto named_tuple_qtype,
      MakeNamedTupleQType({"a", "b"}, MakeTupleQType({GetQType<int32_t>(),
                                                      GetQType<float>()})));
  ParameterQTypes param_qtypes;
  param_qtypes["kwargs"] = named_tuple_qtype;
  EXPECT_EQ(FormatParameterQTypes("kwargs: {kwargs}, **kwargs: {{**kwargs}}",
                                  param_qtypes),
            "kwargs: namedtuple<a=INT32,b=FLOAT32>, **kwargs: "
            "{a: INT32, b: FLOAT32}");
}
}  // namespace
}  // namespace arolla::operator_loader
