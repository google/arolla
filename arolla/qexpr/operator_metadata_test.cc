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
#include "arolla/qexpr/operator_metadata.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/operator_name.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::Field;
using ::testing::MatchesRegex;

TEST(OperatorMetadataTest, OperatorMetadata) {
  auto i32 = GetQType<int32_t>();
  auto f32 = GetQType<float>();

  QExprOperatorMetadata add_ints_meta;
  add_ints_meta.name = AROLLA_OPERATOR_NAME("test.add");
  add_ints_meta.input_qtypes = {i32, i32};
  add_ints_meta.build_details.op_class = "Add<int>";
  QExprOperatorMetadata add_floats_meta;
  add_floats_meta.name = AROLLA_OPERATOR_NAME("test.add");
  add_floats_meta.input_qtypes = {f32, f32};
  add_floats_meta.build_details.op_class = "Add<float>";

  QExprOperatorMetadataRegistry registry;

  ASSERT_OK(registry.AddOperatorMetadata(add_ints_meta));
  ASSERT_OK(registry.AddOperatorMetadata(add_floats_meta));

  EXPECT_THAT(
      registry.AddOperatorMetadata(add_ints_meta),
      StatusIs(absl::StatusCode::kAlreadyExists,
               MatchesRegex("trying to register operator metadata twice for "
                            "operator test.add with input types .*")));
  EXPECT_THAT(
      registry.AddOperatorFamilyMetadata(QExprOperatorFamilyMetadata{
          .name = add_ints_meta.name, .family_build_details = {}}),
      StatusIs(absl::StatusCode::kAlreadyExists,
               Eq("trying to register individual operator or operator family "
                  "metadata twice under the same name test.add")));

  EXPECT_THAT(registry.LookupOperatorMetadata(add_ints_meta.name, {i32, i32}),
              IsOkAndHolds(Field(&QExprOperatorMetadata::build_details,
                                 Field(&BuildDetails::op_class, "Add<int>"))));
}

TEST(OperatorMetadataTest, OperatorFamilyMetadata) {
  auto i32 = GetQType<int32_t>();

  ::arolla::BuildDetails family_build_details;
  family_build_details.op_family_class = "AddFamily";
  QExprOperatorFamilyMetadata add_meta{
      .name = "test.add", .family_build_details = family_build_details};
  QExprOperatorMetadataRegistry registry;

  ASSERT_OK(registry.AddOperatorFamilyMetadata(add_meta));
  EXPECT_THAT(
      registry.AddOperatorMetadata(QExprOperatorMetadata{
          .name = "test.add", .input_qtypes = {i32, i32}}),
      StatusIs(absl::StatusCode::kAlreadyExists,
               Eq("trying to register individual operator or operator family "
                  "metadata twice under the same name test.add")));
  EXPECT_THAT(
      registry.AddOperatorFamilyMetadata(add_meta),
      StatusIs(absl::StatusCode::kAlreadyExists,
               Eq("trying to register individual operator or operator family "
                  "metadata twice under the same name test.add")));
  EXPECT_THAT(
      registry.LookupOperatorMetadata(add_meta.name, {i32, i32}),
      IsOkAndHolds(Field(&QExprOperatorMetadata::build_details,
                         Field(&BuildDetails::op_family_class, "AddFamily"))));
}

}  // namespace
}  // namespace arolla
