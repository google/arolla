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
#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/codegen/qexpr/testing/test_operators.h"
#include "arolla/qexpr/operator_metadata.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Field;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Optional;

class RegisterOperatorTest : public ::testing::Test {
 protected:
  const OperatorRegistry* registry_ = OperatorRegistry::GetInstance();
  QTypePtr b_ = GetQType<bool>();
  QTypePtr i32_ = GetQType<int32_t>();
  QTypePtr i64_ = GetQType<int64_t>();
  QTypePtr f32_ = GetQType<float>();
  QTypePtr f64_ = GetQType<double>();
  QTypePtr v3f32_ = GetQType<testing::Vector3<float>>();
  QTypePtr v3f64_ = GetQType<testing::Vector3<double>>();
};

TEST_F(RegisterOperatorTest, AddRegistered) {
  EXPECT_THAT(registry_->LookupOperator("test.add", {i32_, i32_}, i32_),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.add", {i64_, i64_}, i64_),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.add", {f32_, f32_}, f32_),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.add", {f64_, f64_}, f64_),
              IsOk());
  EXPECT_THAT(
      registry_->LookupOperator("test.add", {b_, b_}, b_),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr(
              "QExpr operator test.add(BOOLEAN,BOOLEAN)->BOOLEAN not found")));
  EXPECT_THAT(
      registry_->LookupOperator("test.add", {i64_, f64_}, i64_),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr(
                   "QExpr operator test.add(INT64,FLOAT64)->INT64 not found")));
}

TEST_F(RegisterOperatorTest, NestedAddRegistered) {
  EXPECT_THAT(registry_->LookupOperator("test.nested_namespace.Add",
                                        {i32_, i32_}, i32_),
              IsOk());
}

TEST_F(RegisterOperatorTest, AddDontLiftRegistered) {
  EXPECT_THAT(
      registry_->LookupOperator("test.add_dont_lift", {i32_, i32_}, i32_),
      IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.add_dont_lift",
                                        {GetOptionalQType<int>(), i32_},
                                        GetOptionalQType<int>()),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator(
                  "test.add_dont_lift",
                  {GetOptionalQType<int>(), GetOptionalQType<int>()},
                  GetOptionalQType<int>()),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("QExpr operator "
                                 "test.add_dont_lift(OPTIONAL_INT32,OPTIONAL_"
                                 "INT32)->OPTIONAL_INT32 not found")));
  EXPECT_THAT(
      registry_->LookupOperator("test.add_dont_lift",
                                {i32_, GetOptionalQType<int>()},
                                GetOptionalQType<int>()),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr("QExpr operator "
                    "test.add_dont_lift(INT32,OPTIONAL_INT32)->OPTIONAL_INT32 "
                    "not found")));
}

TEST_F(RegisterOperatorTest, MulNotRegistered) {
  // Mul operator is defined within the same BUILD file, but should not be
  // linked to this test.
  EXPECT_THAT(registry_->LookupOperator("test.mul", {i32_, i32_}, i32_),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("QExpr operator test.mul not found")));
}

TEST_F(RegisterOperatorTest, IdFamilyRegistered) {
  EXPECT_THAT(registry_->LookupOperator("test.id", {i64_}, i64_), IsOk());
}

TEST_F(RegisterOperatorTest, AddMetadata) {
  EXPECT_THAT(
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          "test.add", {i32_, i32_}),
      IsOkAndHolds(AllOf(
          Field(&QExprOperatorMetadata::name, Eq("test.add")),
          Field(
              &QExprOperatorMetadata::build_details,
              AllOf(
                  Field(&BuildDetails::build_target,
                        Eq("//arolla/codegen/qexpr/"
                           "testing:operator_add_i32_and_i32")),
                  Field(&BuildDetails::hdrs,
                        ElementsAre("arolla/codegen/qexpr/testing/"
                                    "test_operators.h")),
                  Field(&BuildDetails::deps,
                        ElementsAre("//arolla/codegen/qexpr/"
                                    "testing:test_operators_lib")),
                  Field(&BuildDetails::op_class,
                        Eq("::arolla::testing::AddOp")),
                  Field(&BuildDetails::op_class_details,
                        Optional(AllOf(
                            Field(&OpClassDetails::accepts_context, Eq(false)),
                            Field(&OpClassDetails::returns_status_or,
                                  Eq(false))))),

                  Field(&BuildDetails::op_family_class, IsEmpty()))))));
}

TEST_F(RegisterOperatorTest, AddWithContextMetadata) {
  EXPECT_THAT(
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          "test.add_with_context", {i32_, i32_}),
      IsOkAndHolds(AllOf(
          Field(&QExprOperatorMetadata::name, Eq("test.add_with_context")),
          Field(
              &QExprOperatorMetadata::build_details,
              AllOf(Field(&BuildDetails::deps,
                          ElementsAre("//arolla/codegen/qexpr/"
                                      "testing:test_operators_lib")),
                    Field(&BuildDetails::op_class,
                          Eq("::arolla::testing::AddWithContextOp")),
                    Field(&BuildDetails::op_class_details,
                          Optional(AllOf(
                              Field(&OpClassDetails::accepts_context, Eq(true)),
                              Field(&OpClassDetails::returns_status_or,
                                    Eq(false))))),
                    Field(&BuildDetails::op_family_class, IsEmpty()))))));
}

TEST_F(RegisterOperatorTest, AddWithStatusOrMetadata) {
  EXPECT_THAT(
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          "test.add_with_status_or", {i32_, i32_}),
      IsOkAndHolds(AllOf(
          Field(&QExprOperatorMetadata::name, Eq("test.add_with_status_or")),
          Field(&QExprOperatorMetadata::build_details,
                AllOf(Field(&BuildDetails::deps,
                            ElementsAre("//arolla/codegen/qexpr/"
                                        "testing:test_operators_lib")),
                      Field(&BuildDetails::op_class,
                            Eq("::arolla::testing::AddWithStatusOrOp")),
                      Field(&BuildDetails::op_class_details,
                            Optional(
                                AllOf(Field(&OpClassDetails::accepts_context,
                                            Eq(false)),
                                      Field(&OpClassDetails::returns_status_or,
                                            Eq(true))))),
                      Field(&BuildDetails::op_family_class, IsEmpty()))))));
}

TEST_F(RegisterOperatorTest, AddWithArgAsFunctionMetadata) {
  EXPECT_THAT(
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          "test.add_with_arg_as_function", {i32_, i32_}),
      IsOkAndHolds(AllOf(
          Field(&QExprOperatorMetadata::name,
                Eq("test.add_with_arg_as_function")),
          Field(
              &QExprOperatorMetadata::build_details,
              AllOf(
                  Field(&BuildDetails::deps,
                        ElementsAre("//arolla/codegen/qexpr/"
                                    "testing:test_operators_lib")),
                  Field(&BuildDetails::op_class,
                        Eq("::arolla::testing::AddWithArgAsFunction")),
                  Field(
                      &BuildDetails::op_class_details,
                      Optional(AllOf(
                          Field(&OpClassDetails::accepts_context, Eq(false)),
                          Field(&OpClassDetails::returns_status_or, Eq(false)),
                          Field(&OpClassDetails::arg_as_function_ids,
                                ElementsAre(1))))),
                  Field(&BuildDetails::op_family_class, IsEmpty()))))));
}

TEST_F(RegisterOperatorTest, AddWithAllArgAsFunctionMetadata) {
  EXPECT_THAT(
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          "test.add_with_all_args_as_function", {i32_, i32_}),
      IsOkAndHolds(AllOf(
          Field(&QExprOperatorMetadata::name,
                Eq("test.add_with_all_args_as_function")),
          Field(
              &QExprOperatorMetadata::build_details,
              AllOf(
                  Field(&BuildDetails::deps,
                        ElementsAre("//arolla/codegen/qexpr/"
                                    "testing:test_operators_lib")),
                  Field(&BuildDetails::op_class,
                        Eq("::arolla::testing::AddWithArgAsFunction")),
                  Field(
                      &BuildDetails::op_class_details,
                      Optional(AllOf(
                          Field(&OpClassDetails::accepts_context, Eq(false)),
                          Field(&OpClassDetails::returns_status_or, Eq(false)),
                          Field(&OpClassDetails::arg_as_function_ids,
                                ElementsAre(0, 1))))),
                  Field(&BuildDetails::op_family_class, IsEmpty()))))));
}

TEST_F(RegisterOperatorTest, NegateRegistered) {
  EXPECT_THAT(registry_->LookupOperator("test.negate", {i32_}, i32_), IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.negate", {i64_}, i64_), IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.negate", {f32_}, f32_), IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.negate", {f64_}, f64_), IsOk());
  EXPECT_THAT(
      registry_->LookupOperator("test.negate", {b_}, b_),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr("QExpr operator test.negate(BOOLEAN)->BOOLEAN not found")));
}

TEST_F(RegisterOperatorTest, Vector3Ops) {
  EXPECT_THAT(registry_->LookupOperator("test.add", {v3f32_, v3f32_}, v3f32_),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.add", {v3f64_, v3f64_}, v3f64_),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.vector_components", {v3f32_},
                                        MakeTupleQType({f32_, f32_, f32_})),
              IsOk());
  EXPECT_THAT(registry_->LookupOperator("test.vector_components", {v3f64_},
                                        MakeTupleQType({f64_, f64_, f64_})),
              IsOk());
  EXPECT_THAT(
      registry_->LookupOperator("test.vector_components", {i32_}, i32_),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("QExpr operator test.vector_components(INT32)->INT32 "
                         "not found")));

  EXPECT_THAT(
      registry_->LookupOperator("test.dot_prod", {v3f32_, v3f32_}, f32_),
      IsOk());
  EXPECT_THAT(
      registry_->LookupOperator("test.dot_prod", {v3f64_, v3f64_}, f64_),
      IsOk());
  EXPECT_THAT(
      registry_->LookupOperator("test.dot_prod", {v3f32_, v3f64_}, v3f64_),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("QExpr operator "
                         "test.dot_prod(Vector3<FLOAT32>,Vector3<FLOAT64>)->"
                         "Vector3<FLOAT64> not found")));
}

TEST_F(RegisterOperatorTest, DotProdMetadata) {
  EXPECT_THAT(
      QExprOperatorMetadataRegistry::GetInstance().LookupOperatorMetadata(
          "test.dot_prod", {v3f32_, v3f32_}),
      IsOkAndHolds(AllOf(
          Field(&QExprOperatorMetadata::name, "test.dot_prod"),
          Field(&QExprOperatorMetadata::build_details,
                AllOf(Field(&BuildDetails::build_target,
                            Eq("//arolla/codegen/qexpr/"
                               "testing:operator_dotprod_testing_vector3_of_"
                               "f32_and_testing_vector3_of_f32")),
                      Field(&BuildDetails::hdrs,
                            ElementsAre(
                                "arolla/codegen/qexpr/testing/"
                                "test_operators.h")),
                      Field(&BuildDetails::deps,
                            ElementsAre("//arolla/codegen/qexpr/"
                                        "testing:test_operators_lib")),
                      Field(&BuildDetails::op_class,
                            Eq("::arolla::testing::DotProdOp")),
                      Field(&BuildDetails::op_family_class, IsEmpty()))))));
}

}  // namespace
}  // namespace arolla::testing
