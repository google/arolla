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
#include "arolla/expr/operator_loader/parameter_qtypes.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::operator_loader {
namespace {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using Attr = ::arolla::expr::ExprAttributes;

class ParameterQTypesTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(ParameterQTypesTest, ExtractParameterQTypes) {
  ASSERT_OK_AND_ASSIGN(
      auto sig, ExprOperatorSignature::Make("first, second=, *args", kUnit));
  EXPECT_THAT(ExtractParameterQTypes(sig, {}), IsEmpty());
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{GetQType<int32_t>()}}),
              UnorderedElementsAre(Pair("first", GetQType<int32_t>())));
  EXPECT_THAT(ExtractParameterQTypes(
                  sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()}}),
              UnorderedElementsAre(Pair("first", GetQType<int32_t>()),
                                   Pair("second", GetQType<float>()),
                                   Pair("args", MakeTupleQType({}))));
  EXPECT_THAT(
      ExtractParameterQTypes(
          sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()},
                Attr{GetQType<Bytes>()}}),
      UnorderedElementsAre(Pair("first", GetQType<int32_t>()),
                           Pair("second", GetQType<float>()),
                           Pair("args", MakeTupleQType({GetQType<Bytes>()}))));
  EXPECT_THAT(
      ExtractParameterQTypes(
          sig, {Attr{GetQType<int32_t>()}, Attr{GetQType<float>()},
                Attr{GetQType<Bytes>()}, Attr{GetQType<Text>()}}),
      UnorderedElementsAre(
          Pair("first", GetQType<int32_t>()), Pair("second", GetQType<float>()),
          Pair("args", MakeTupleQType({GetQType<Bytes>(), GetQType<Text>()}))));
  EXPECT_THAT(ExtractParameterQTypes(sig, {Attr{}, Attr{}, Attr{}, Attr{}}),
              IsEmpty());
}

TEST_F(ParameterQTypesTest, MakeParameterQTypeModelExecutor) {
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

TEST_F(ParameterQTypesTest, FormatParameterQTypes) {
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

}  // namespace
}  // namespace arolla::operator_loader
