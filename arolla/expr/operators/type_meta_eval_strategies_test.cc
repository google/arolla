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
#include "arolla/expr/operators/type_meta_eval_strategies.h"

#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla::expr_operators {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;

using ::arolla::expr_operators::type_meta::AllSame;
using ::arolla::expr_operators::type_meta::AllSameScalarType;
using ::arolla::expr_operators::type_meta::ArgCount;
using ::arolla::expr_operators::type_meta::Broadcast;
using ::arolla::expr_operators::type_meta::CallableStrategy;
using ::arolla::expr_operators::type_meta::FirstMatchingTypeStrategy;
using ::arolla::expr_operators::type_meta::Is;
using ::arolla::expr_operators::type_meta::IsArrayShape;
using ::arolla::expr_operators::type_meta::IsDenseArray;
using ::arolla::expr_operators::type_meta::IsEdge;
using ::arolla::expr_operators::type_meta::IsNot;
using ::arolla::expr_operators::type_meta::IsShape;
using ::arolla::expr_operators::type_meta::LiftResultType;
using ::arolla::expr_operators::type_meta::Nth;
using ::arolla::expr_operators::type_meta::NthApply;
using ::arolla::expr_operators::type_meta::NthMatch;
using ::arolla::expr_operators::type_meta::Optional;
using ::arolla::expr_operators::type_meta::OptionalLike;
using ::arolla::expr_operators::type_meta::Scalar;
using ::arolla::expr_operators::type_meta::ScalarOrOptional;
using ::arolla::expr_operators::type_meta::ScalarTypeIs;
using ::arolla::expr_operators::type_meta::ToOptional;
using ::arolla::expr_operators::type_meta::ToShape;
using ::arolla::expr_operators::type_meta::Unary;

TEST(TypeMetaEvalStrategiesTest, ArgCount) {
  std::vector<QTypePtr> i32_types = {GetQType<int32_t>(), GetQType<int32_t>(),
                                     GetQType<int32_t>()};
  std::vector<QTypePtr> empty = {};
  EXPECT_THAT(ArgCount(3)(i32_types),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(ArgCount(1)(empty),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected to have 1 arguments, got 0")));
  EXPECT_THAT(ArgCount(0)(i32_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected to have 0 arguments, got 3")));
}

TEST(TypeMetaEvalStrategiesTest, NthSingleArg) {
  auto second_type = CallableStrategy(Nth(1));
  EXPECT_THAT(second_type({GetQType<int32_t>(), GetQType<int64_t>()}),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(
      second_type({}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected to have at least 2 argument(s), got 0")));
}

TEST(TypeMetaEvalStrategiesTest, NthMultipleArgs) {
  auto i32 = GetQType<int32_t>();
  auto oi32 = GetQType<OptionalValue<int32_t>>();
  auto f32 = GetQType<float>();
  auto of32 = GetQType<OptionalValue<float>>();

  std::vector<QTypePtr> types = {i32, oi32, f32, of32};
  EXPECT_THAT((Nth({0, 2})(types)), IsOkAndHolds(ElementsAre(i32, f32)));
  EXPECT_THAT((Nth({1, 3})(types)), IsOkAndHolds(ElementsAre(oi32, of32)));
  EXPECT_THAT((Nth({0, 2, 4})(types)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected to have at least 5 argument(s), got 4"));
}

TEST(TypeMetaEvalStrategiesTest, Scalar) {
  EXPECT_THAT(
      Scalar({GetQType<int32_t>(), GetQType<float>()}),
      IsOkAndHolds(ElementsAre(GetQType<int32_t>(), GetQType<float>())));
  EXPECT_THAT(
      Scalar({GetQType<int32_t>(), GetOptionalQType<int32_t>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "expected all arguments to be scalar, but got OPTIONAL_INT32")));
  EXPECT_THAT(Scalar({GetQType<int32_t>(), GetDenseArrayQType<int32_t>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected all arguments to be scalar, but got "
                                 "DENSE_ARRAY_INT32")));
}

TEST(TypeMetaEvalStrategiesTest, Optional) {
  EXPECT_THAT(
      Optional({GetOptionalQType<int32_t>(), GetOptionalQType<float>()}),
      IsOkAndHolds(
          ElementsAre(GetOptionalQType<int32_t>(), GetOptionalQType<float>())));
  EXPECT_THAT(
      Optional({GetOptionalQType<int32_t>(), GetQType<int32_t>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected all arguments to be optional, but got INT32")));
  EXPECT_THAT(
      Optional({GetOptionalQType<int32_t>(), GetDenseArrayQType<int32_t>()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected all arguments to be optional, but got "
                         "DENSE_ARRAY_INT32")));
}

TEST(TypeMetaEvalStrategiesTest, ScalarOrOptional) {
  EXPECT_THAT(
      ScalarOrOptional({GetOptionalQType<int32_t>(), GetQType<float>()}),
      IsOkAndHolds(
          ElementsAre(GetOptionalQType<int32_t>(), GetQType<float>())));
  EXPECT_THAT(
      ScalarOrOptional(
          {GetOptionalQType<int32_t>(), GetDenseArrayQType<int32_t>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "expected all arguments to be scalar or optional scalar, but got "
              "DENSE_ARRAY_INT32")));
}

TEST(TypeMetaEvalStrategiesTest, OptionalLike) {
  EXPECT_THAT(OptionalLike(
                  {GetOptionalQType<int32_t>(), GetDenseArrayQType<int32_t>()}),
              IsOkAndHolds(ElementsAre(GetOptionalQType<int32_t>(),
                                       GetDenseArrayQType<int32_t>())));
  EXPECT_THAT(
      OptionalLike({GetOptionalQType<int32_t>(), GetQType<int32_t>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected all arguments to be optional, but got INT32")));
  EXPECT_THAT(
      OptionalLike({GetOptionalQType<int32_t>(), GetQType<DenseArrayEdge>()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected all arguments to be optional, but got "
                         "DENSE_ARRAY_EDGE")));
}

TEST(TypeMetaEvalStrategiesTest, FirstMatchingTypeStrategy) {
  auto first_numeric =
      CallableStrategy(FirstMatchingTypeStrategy(IsNumeric, Nth(0)));
  EXPECT_THAT(first_numeric({GetQType<int32_t>(), GetQType<int64_t>()}),
              IsOkAndHolds(GetQType<int32_t>()));
  EXPECT_THAT(first_numeric({GetQType<Text>(), GetQType<int64_t>()}),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(first_numeric({GetQType<int32_t>(), GetQType<Text>()}),
              IsOkAndHolds(GetQType<int32_t>()));

  // test that default_fn is called
  EXPECT_THAT(first_numeric({GetQType<Text>(), GetQType<Text>()}),
              IsOkAndHolds(GetQType<Text>()));
  EXPECT_THAT(
      first_numeric({}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected to have at least 1 argument(s), got 0")));
}

TEST(TypeMetaEvalStrategiesTest, IsNumeric) {
  EXPECT_TRUE(IsNumeric(GetQType<int32_t>()));
  EXPECT_TRUE(IsNumeric(GetQType<float>()));
  EXPECT_TRUE(IsNumeric(GetOptionalQType<float>()));
  EXPECT_TRUE(IsNumeric(GetArrayQType<float>()));
  EXPECT_TRUE(IsNumeric(GetDenseArrayQType<float>()));

  EXPECT_FALSE(IsNumeric(GetQType<bool>()));
  EXPECT_FALSE(IsNumeric(GetQType<Bytes>()));
  EXPECT_FALSE(IsNumeric(GetArrayQType<bool>()));
  EXPECT_FALSE(IsNumeric(GetDenseArrayQType<bool>()));
  EXPECT_FALSE(IsNumeric(GetOptionalQType<bool>()));
}

TEST(TypeMetaEvalStrategiesTest, NthMatch) {
  std::vector<QTypePtr> i32_types = {GetQType<int32_t>(), GetQType<int32_t>(),
                                     GetQType<int32_t>()};
  std::vector<QTypePtr> i32_type = {GetQType<int32_t>()};
  std::vector<QTypePtr> i32_types_len_2 = {GetQType<int32_t>(),
                                           GetQType<int32_t>()};

  EXPECT_THAT((NthMatch(1, Is<int32_t>)(i32_types)),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(
      (NthMatch(1, Is<int64_t>)(i32_types)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected type to be INT64, got INT32; for arguments (1)"));
  EXPECT_THAT((NthMatch(1, Is<int64_t>)(i32_type)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected to have at least 2 argument(s), got 1"));
  EXPECT_THAT((NthMatch(2, Is<int32_t>)(i32_types)),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(
      (NthMatch(2, Is<int64_t>)(i32_types)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected type to be INT64, got INT32; for arguments (2)"));
  EXPECT_THAT((NthMatch(2, Is<int64_t>)(i32_types_len_2)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected to have at least 3 argument(s), got 2"));

  std::vector<QTypePtr> types1 = {GetQType<int32_t>(), GetQType<int32_t>(),
                                  GetQType<OptionalValue<int32_t>>(),
                                  GetQType<OptionalValue<int32_t>>(),
                                  GetQType<float>()};
  EXPECT_THAT((NthMatch({0, 1}, AllSame)(types1)),
              IsOkAndHolds(ElementsAreArray(types1)));
  EXPECT_THAT((NthMatch({2, 3}, AllSame)(types1)),
              IsOkAndHolds(ElementsAreArray(types1)));
  EXPECT_THAT((NthMatch({0, 1, 2, 3}, AllSameScalarType)(types1)),
              IsOkAndHolds(ElementsAreArray(types1)));
  EXPECT_THAT((NthMatch({0, 2}, AllSame)(types1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all types to be equal, got INT32 and "
                       "OPTIONAL_INT32; for arguments (0, 2)"));
  EXPECT_THAT((NthMatch({0, 2}, AllSame)({GetQType<int32_t>()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected to have at least 3 argument(s), got 1"));
}

TEST(TypeMetaEvalStrategiesTest, NthApply) {
  std::vector<QTypePtr> types = {GetQType<int32_t>(),
                                 GetDenseArrayQType<int32_t>(),
                                 GetArrayQType<int32_t>()};
  {
    // Apply to two args.
    std::vector<QTypePtr> res_types = {GetDenseArrayQType<int32_t>(),
                                       GetDenseArrayQType<int32_t>(),
                                       GetArrayQType<int32_t>()};
    EXPECT_THAT(NthApply({0, 1}, Broadcast)(types),
                IsOkAndHolds(ElementsAreArray(res_types)));
  }
  {
    // Apply to two non-subsequent args.
    std::vector<QTypePtr> res_types = {GetArrayQType<int32_t>(),
                                       GetDenseArrayQType<int32_t>(),
                                       GetArrayQType<int32_t>()};

    EXPECT_THAT(NthApply({0, 2}, Broadcast)(types),
                IsOkAndHolds(ElementsAreArray(res_types)));
  }
  {
    // Apply to a single argument.
    std::vector<QTypePtr> res_types = {GetOptionalQType<int32_t>(),
                                       GetDenseArrayQType<int32_t>(),
                                       GetArrayQType<int32_t>()};
    EXPECT_THAT(NthApply(0, ToOptional)(types),
                IsOkAndHolds(ElementsAreArray(res_types)));
  }

  // Error reporting.
  EXPECT_THAT(NthApply({1, 2}, Broadcast)(types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unable to broadcast arguments; "
                       "DENSE_ARRAY_INT32,ARRAY_INT32; for arguments (1, 2)"));
  EXPECT_THAT(NthApply({2, 3}, Broadcast)(types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected to have at least 4 argument(s), got 3"));
}

TEST(TypeMetaEvalStrategiesTest, LiftResultType) {
  auto i32 = GetQType<int32_t>();
  auto f32 = GetQType<float>();
  auto oi32 = GetOptionalQType<int32_t>();
  auto of32 = GetOptionalQType<float>();
  auto ai32 = GetArrayQType<int32_t>();
  auto af32 = GetArrayQType<float>();

  auto lift_f32 = CallableStrategy(LiftResultType(f32));

  // No arguments.
  EXPECT_THAT(lift_f32({}), IsOkAndHolds(f32));

  // Only scalar arguments.
  EXPECT_THAT(lift_f32({i32}), IsOkAndHolds(f32));
  EXPECT_THAT(lift_f32({i32, f32}), IsOkAndHolds(f32));

  // Some optional arguments.
  EXPECT_THAT(lift_f32({oi32}), IsOkAndHolds(of32));
  EXPECT_THAT(lift_f32({i32, oi32}), IsOkAndHolds(of32));

  // Some array arguments.
  EXPECT_THAT(lift_f32({ai32}), IsOkAndHolds(af32));
  EXPECT_THAT(lift_f32({oi32, ai32}), IsOkAndHolds(af32));
  EXPECT_THAT(lift_f32({i32, oi32, ai32}), IsOkAndHolds(af32));
}

TEST(TypeMetaEvalStrategiesTest, Broadcast) {
  auto i32 = GetQType<int32_t>();
  auto f32 = GetQType<float>();
  auto oi32 = GetOptionalQType<int32_t>();
  auto ai32 = GetArrayQType<int32_t>();
  auto af32 = GetArrayQType<float>();
  auto di32 = GetDenseArrayQType<int32_t>();
  auto df32 = GetDenseArrayQType<float>();

  // No arguments.
  EXPECT_THAT(Broadcast({}), IsOkAndHolds(ElementsAre()));

  // Only scalar-like arguments.
  EXPECT_THAT(Broadcast({i32}), IsOkAndHolds(ElementsAre(i32)));
  EXPECT_THAT(Broadcast({i32, f32}), IsOkAndHolds(ElementsAre(i32, f32)));
  EXPECT_THAT(Broadcast({i32, oi32}), IsOkAndHolds(ElementsAre(i32, oi32)));

  // Some array arguments.
  EXPECT_THAT(Broadcast({ai32}), IsOkAndHolds(ElementsAre(ai32)));
  EXPECT_THAT(Broadcast({ai32, f32}), IsOkAndHolds(ElementsAre(ai32, af32)));
  EXPECT_THAT(Broadcast({i32, oi32, af32}),
              IsOkAndHolds(ElementsAre(ai32, ai32, af32)));
  EXPECT_THAT(Broadcast({i32, oi32, af32, ai32}),
              IsOkAndHolds(ElementsAre(ai32, ai32, af32, ai32)));

  // Dense array
  EXPECT_THAT(
      Broadcast({df32, GetQType<int32_t>(), GetOptionalQType<int32_t>()}),
      IsOkAndHolds(ElementsAre(df32, di32, di32)));

  // Mix of arrays
  EXPECT_THAT(Broadcast({af32, df32}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(TypeMetaEvalStrategiesTest, Is) {
  std::vector<QTypePtr> i32_types = {GetQType<int32_t>(), GetQType<int32_t>()};
  EXPECT_THAT(Is<int32_t>(i32_types),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(Is(GetQType<int32_t>())(i32_types),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(Is<int64_t>(i32_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected type of argument 0 to be INT64, got INT32"));
  EXPECT_THAT(Is(GetQType<int64_t>())(i32_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected type of argument 0 to be INT64, got INT32"));
}

TEST(TypeMetaEvalStrategiesTest, IsNot) {
  std::vector<QTypePtr> i32_types = {GetQType<int32_t>(), GetQType<int32_t>()};
  EXPECT_THAT(IsNot<int64_t>(i32_types),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(IsNot(GetQType<int64_t>())(i32_types),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(IsNot<int32_t>(i32_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected type of argument 0 to be not INT32"));
  EXPECT_THAT(IsNot(GetQType<int32_t>())(i32_types),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected type of argument 0 to be not INT32"));
}

TEST(TypeMetaEvalStrategiesTest, ScalarTypeIs) {
  std::vector<QTypePtr> i32_types = {
      GetQType<int32_t>(), GetOptionalQType<int32_t>(),
      GetDenseArrayQType<int32_t>(), GetDenseArrayQType<int32_t>(),
      GetArrayQType<int32_t>()};
  EXPECT_THAT(ScalarTypeIs<int32_t>(i32_types),
              IsOkAndHolds(ElementsAreArray(i32_types)));
  EXPECT_THAT(
      ScalarTypeIs<int64_t>(i32_types),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected scalar type of argument 0 to be INT64, got INT32"));
}

TEST(TypeMetaEvalStrategiesTest, Unary) {
  auto single_arg_type = CallableStrategy(Unary);
  EXPECT_THAT(single_arg_type({GetQType<int32_t>()}),
              IsOkAndHolds(GetQType<int32_t>()));
  EXPECT_THAT(single_arg_type({GetQType<int32_t>(), GetQType<int32_t>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected to have one argument")));
}

TEST(TypeMetaEvalStrategiesTest, ToShape) {
  auto shape_type = CallableStrategy(ToShape);

  EXPECT_THAT(shape_type({GetQType<int32_t>()}),
              IsOkAndHolds(GetQType<ScalarShape>()));

  EXPECT_THAT(shape_type({GetArrayQType<bool>()}),
              IsOkAndHolds(GetQType<ArrayShape>()));

  EXPECT_THAT(shape_type({GetDenseArrayQType<bool>()}),
              IsOkAndHolds(GetQType<DenseArrayShape>()));

  EXPECT_THAT(shape_type({GetQType<OptionalValue<bool>>()}),
              IsOkAndHolds(GetQType<OptionalScalarShape>()));

  EXPECT_THAT(shape_type({GetQType<OptionalScalarShape>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no shape type for")));
}

TEST(TypeMetaEvalStrategiesTest, ToOptional) {
  auto to_optional = CallableStrategy(ToOptional);
  EXPECT_THAT(to_optional({GetArrayQType<int32_t>()}),
              IsOkAndHolds(GetArrayQType<int32_t>()));
  EXPECT_THAT(
      to_optional({GetQType<ArrayEdge>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("no optional-like qtype for ARRAY_EDGE; in argument 0")));
}

TEST(TypeMetaEvalStrategiesTest, AllSame) {
  EXPECT_THAT(AllSame({GetArrayQType<int32_t>(), GetArrayQType<int32_t>()}),
              IsOkAndHolds(ElementsAreArray(
                  {GetArrayQType<int32_t>(), GetArrayQType<int32_t>()})));
  EXPECT_THAT(AllSame({}), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(AllSame({GetArrayQType<int32_t>(), GetArrayQType<int64_t>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected all types to be equal, got "
                                 "ARRAY_INT32 and ARRAY_INT64")));
}

TEST(TypeMetaEvalStrategiesTest, AllSameScalarType) {
  EXPECT_THAT(AllSameScalarType(
                  {GetQType<int32_t>(), GetQType<OptionalValue<int32_t>>()}),
              IsOkAndHolds(ElementsAre(GetQType<int32_t>(),
                                       GetQType<OptionalValue<int32_t>>())));
  EXPECT_THAT(AllSameScalarType({}), IsOkAndHolds(ElementsAre()));
  EXPECT_THAT(AllSameScalarType({GetQType<int32_t>(), GetQType<float>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all scalar types to be equal, got INT32 and "
                       "FLOAT32"));
}

TEST(TypeMetaEvalStrategiesTest, IsShape) {
  auto shape_qtypes = {GetQType<ScalarShape>(), GetQType<ArrayShape>()};
  auto non_shape_qtypes = {GetQType<OptionalScalarShape>(),
                           GetQType<int32_t>()};
  EXPECT_THAT(IsShape(shape_qtypes),
              IsOkAndHolds(ElementsAreArray(shape_qtypes)));
  EXPECT_THAT(
      IsShape(non_shape_qtypes),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected all arguments to be shapes, got INT32")));
}

TEST(TypeMetaEvalStrategiesTest, IsArrayShape) {
  auto shape_qtypes = {GetQType<ArrayShape>(), GetQType<DenseArrayShape>()};
  auto non_shape_qtypes = {GetQType<ArrayShape>(), GetQType<ScalarShape>()};
  EXPECT_THAT(IsArrayShape(shape_qtypes),
              IsOkAndHolds(ElementsAreArray(shape_qtypes)));
  EXPECT_THAT(
      IsArrayShape(non_shape_qtypes),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "expected all arguments to be array shapes, got SCALAR_SHAPE")));
}

TEST(TypeMetaEvalStrategiesTest, IsEdge) {
  auto edge_qtypes = {GetQType<ArrayEdge>(), GetQType<DenseArrayEdge>()};
  EXPECT_THAT(IsEdge(edge_qtypes), IsOkAndHolds(ElementsAreArray(edge_qtypes)));
  EXPECT_THAT(
      IsEdge({GetQType<ArrayEdge>(), GetQType<int32_t>()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected all arguments to be edges, got INT32")));
}

TEST(TypeMetaEvalStrategiesTest, IsDenseArray) {
  auto da_qtypes = {GetDenseArrayQType<int64_t>(), GetDenseArrayQType<float>()};
  EXPECT_THAT(IsDenseArray(da_qtypes),
              IsOkAndHolds(ElementsAreArray(da_qtypes)));
  EXPECT_THAT(
      IsDenseArray({GetArrayQType<int64_t>(), GetDenseArrayQType<int64_t>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "expected all arguments to be DenseArrays, got ARRAY_INT64")));
}

TEST(TypeMetaEvalStrategiesTest, EdgeParentShapeQType) {
  auto edge_qtypes = {GetQType<ArrayEdge>(), GetQType<DenseArrayEdge>(),
                      GetQType<ArrayGroupScalarEdge>(),
                      GetQType<DenseArrayGroupScalarEdge>()};
  auto shape_qtypes = {GetQType<ArrayShape>(), GetQType<DenseArrayShape>(),
                       GetQType<OptionalScalarShape>(),
                       GetQType<OptionalScalarShape>()};
  EXPECT_THAT(type_meta::EdgeParentShapeQType(edge_qtypes),
              IsOkAndHolds(ElementsAreArray(shape_qtypes)));
  EXPECT_THAT(
      type_meta::EdgeParentShapeQType({GetArrayQType<int64_t>()}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("invalid argument 0: expected an edge, got ARRAY_INT64")));
}

}  // namespace
}  // namespace arolla::expr_operators
