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
#include "arolla/qtype/standard_type_properties/properties.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::MatchesRegex;

TEST(TypeProperties, GetScalarQType) {
  EXPECT_THAT(GetScalarQType(GetQType<int64_t>()),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(GetScalarQType(GetOptionalQType<int64_t>()),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(GetScalarQType(GetDenseArrayQType<int64_t>()),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(GetScalarQType(GetDenseArrayWeakFloatQType()),
              IsOkAndHolds(GetWeakFloatQType()));
  EXPECT_THAT(GetScalarQType(GetArrayWeakFloatQType()),
              IsOkAndHolds(GetWeakFloatQType()));
  EXPECT_THAT(
      GetScalarQType(MakeTupleQType({GetQType<int64_t>()})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex("there is no corresponding scalar type for .*")));
}

TEST(TypeProperties, GetShapeQType) {
  EXPECT_THAT(GetShapeQType(GetQType<int64_t>()),
              IsOkAndHolds(GetQType<ScalarShape>()));
  EXPECT_THAT(GetShapeQType(GetOptionalQType<int64_t>()),
              IsOkAndHolds(GetQType<OptionalScalarShape>()));
  EXPECT_THAT(GetShapeQType(GetDenseArrayQType<int64_t>()),
              IsOkAndHolds(GetQType<DenseArrayShape>()));
  EXPECT_THAT(GetShapeQType(GetDenseArrayWeakFloatQType()),
              IsOkAndHolds(GetQType<DenseArrayShape>()));
  EXPECT_THAT(GetShapeQType(GetArrayWeakFloatQType()),
              IsOkAndHolds(GetQType<ArrayShape>()));
  EXPECT_THAT(GetShapeQType(MakeTupleQType({GetQType<int64_t>()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex("no shape type for .*")));
}

TEST(TypeProperties, WithScalarQType) {
  EXPECT_THAT(WithScalarQType(GetQType<int64_t>(), GetQType<float>()),
              IsOkAndHolds(GetQType<float>()));
  EXPECT_THAT(WithScalarQType(GetOptionalQType<int64_t>(), GetQType<float>()),
              IsOkAndHolds(GetOptionalQType<float>()));
  EXPECT_THAT(WithScalarQType(GetDenseArrayQType<int64_t>(), GetQType<float>()),
              IsOkAndHolds(GetDenseArrayQType<float>()));
  EXPECT_THAT(
      WithScalarQType(GetDenseArrayQType<int64_t>(), GetWeakFloatQType()),
      IsOkAndHolds(GetDenseArrayWeakFloatQType()));
  EXPECT_THAT(WithScalarQType(GetArrayQType<int64_t>(), GetWeakFloatQType()),
              IsOkAndHolds(GetArrayWeakFloatQType()));
  EXPECT_THAT(WithScalarQType(MakeTupleQType({GetQType<int64_t>()}),
                              GetOptionalQType<float>()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex("unable to replace scalar type in .* with "
                                    "a non-scalar type .*")));
  EXPECT_THAT(
      WithScalarQType(MakeTupleQType({GetQType<int64_t>()}), GetQType<float>()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex("unable to replace scalar type in .*")));
}

TEST(TypeProperties, GetPresenceQType) {
  EXPECT_THAT(GetPresenceQType(GetQType<int64_t>()),
              IsOkAndHolds(GetQType<Unit>()));
  EXPECT_THAT(GetPresenceQType(GetOptionalQType<int64_t>()),
              IsOkAndHolds(GetQType<OptionalUnit>()));
  EXPECT_THAT(GetPresenceQType(GetDenseArrayQType<int64_t>()),
              IsOkAndHolds(GetDenseArrayQType<Unit>()));
  EXPECT_THAT(GetPresenceQType(GetDenseArrayWeakFloatQType()),
              IsOkAndHolds(GetDenseArrayQType<Unit>()));
  EXPECT_THAT(GetPresenceQType(GetArrayWeakFloatQType()),
              IsOkAndHolds(GetArrayQType<Unit>()));
  EXPECT_THAT(GetPresenceQType(MakeTupleQType({GetQType<int64_t>()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex("no type to represent presence in .*")));
}

TEST(TypeProperties, IsOptionalLikeQType) {
  EXPECT_FALSE(IsOptionalLikeQType(GetQType<int64_t>()));
  EXPECT_TRUE(IsOptionalLikeQType(GetOptionalQType<int64_t>()));
  EXPECT_TRUE(IsOptionalLikeQType(GetDenseArrayQType<int64_t>()));
  EXPECT_TRUE(IsOptionalLikeQType(GetDenseArrayWeakFloatQType()));
  EXPECT_TRUE(IsOptionalLikeQType(GetArrayWeakFloatQType()));
}

TEST(TypeProperties, ToOptionalLikeQType) {
  EXPECT_THAT(ToOptionalLikeQType(GetQType<int64_t>()),
              IsOkAndHolds(GetOptionalQType<int64_t>()));
  EXPECT_THAT(ToOptionalLikeQType(GetOptionalQType<int64_t>()),
              IsOkAndHolds(GetOptionalQType<int64_t>()));
  EXPECT_THAT(ToOptionalLikeQType(GetDenseArrayQType<int64_t>()),
              IsOkAndHolds(GetDenseArrayQType<int64_t>()));
  EXPECT_THAT(ToOptionalLikeQType(GetDenseArrayWeakFloatQType()),
              IsOkAndHolds(GetDenseArrayWeakFloatQType()));
  EXPECT_THAT(ToOptionalLikeQType(GetArrayWeakFloatQType()),
              IsOkAndHolds(GetArrayWeakFloatQType()));
}

}  // namespace
}  // namespace arolla
