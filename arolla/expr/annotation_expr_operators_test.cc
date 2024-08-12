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
#include "arolla/expr/annotation_expr_operators.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsAttr;

class AnnotationExprOperatorsTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(AnnotationExprOperatorsTest, QTypeAnnotation) {
  auto annotation_qtype = QTypeAnnotation::Make();

  EXPECT_THAT(annotation_qtype->InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an operator "
                       "node: expected 2 but got 0"));
  EXPECT_THAT(
      annotation_qtype->InferAttributes({ExprAttributes{GetQType<int64_t>()},
                                         ExprAttributes{GetQType<int64_t>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected QTYPE, got qtype: INT64"));
  EXPECT_THAT(
      annotation_qtype->InferAttributes({ExprAttributes{GetQType<int64_t>()},
                                         ExprAttributes{GetQType<QTypePtr>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "`qtype` must be a literal"));

  EXPECT_THAT(annotation_qtype->InferAttributes(
                  {ExprAttributes{},
                   ExprAttributes{TypedValue::FromValue(GetQType<int64_t>())}}),
              IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
  EXPECT_THAT(annotation_qtype->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{TypedValue::FromValue(GetQType<int64_t>())}}),
              IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
  EXPECT_THAT(
      annotation_qtype->InferAttributes(
          {ExprAttributes{GetQType<int64_t>()},
           ExprAttributes{TypedValue::FromValue(GetQType<Text>())}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "inconsistent annotation.qtype(expr: INT64, qtype=TEXT)"));
}

TEST_F(AnnotationExprOperatorsTest, NameAnnotation) {
  auto annotation_name = NameAnnotation::Make();

  EXPECT_THAT(annotation_name->InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an operator "
                       "node: expected 2 but got 0"));
  EXPECT_THAT(
      annotation_name->InferAttributes({ExprAttributes{GetQType<int64_t>()},
                                        ExprAttributes{GetQType<int64_t>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected a TEXT literal, got name: INT64"));
  EXPECT_THAT(annotation_name->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()}, ExprAttributes{}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`name` must be a TEXT literal"));
  EXPECT_THAT(
      annotation_name->InferAttributes({ExprAttributes{GetQType<int64_t>()},
                                        ExprAttributes{GetQType<Text>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "`name` must be a TEXT literal"));
  EXPECT_THAT(annotation_name->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{TypedValue::FromValue(Text("foo"))}}),
              IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
}

TEST_F(AnnotationExprOperatorsTest, ExportAnnotation) {
  auto annotation_export = ExportAnnotation::Make();

  EXPECT_THAT(annotation_export->InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an operator "
                       "node: expected 2 but got 0"));
  EXPECT_THAT(
      annotation_export->InferAttributes({ExprAttributes{GetQType<int64_t>()},
                                          ExprAttributes{GetQType<int64_t>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected TEXT, got export_tag: INT64"));
  EXPECT_THAT(
      annotation_export->InferAttributes({ExprAttributes{GetQType<int64_t>()},
                                          ExprAttributes{GetQType<Text>()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "`export_tag` must be a TEXT literal"));
  EXPECT_THAT(annotation_export->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()}, ExprAttributes{}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`export_tag` must be a TEXT literal"));
  EXPECT_THAT(annotation_export->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{TypedValue::FromValue(Text(""))}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`export_tag` must be non-empty"));
  EXPECT_THAT(annotation_export->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{TypedValue::FromValue(Text("foo"))}}),
              IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
}

TEST_F(AnnotationExprOperatorsTest, ExportValueAnnotation) {
  auto annotation_export_value = ExportValueAnnotation::Make();

  EXPECT_THAT(annotation_export_value->InferAttributes({}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "incorrect number of dependencies passed to an operator "
                       "node: expected 3 but got 0"));
  EXPECT_THAT(annotation_export_value->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{GetQType<int64_t>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected TEXT, got export_tag: INT64"));
  EXPECT_THAT(annotation_export_value->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()}, ExprAttributes{},
                   ExprAttributes{GetQType<int64_t>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`export_tag` must be a TEXT literal"));
  EXPECT_THAT(annotation_export_value->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{GetQType<Text>()},
                   ExprAttributes{GetQType<int64_t>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`export_tag` must be a TEXT literal"));
  EXPECT_THAT(annotation_export_value->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{TypedValue::FromValue(Text(""))},
                   ExprAttributes{GetQType<int64_t>()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`export_tag` must be non-empty"));
  EXPECT_THAT(annotation_export_value->InferAttributes(
                  {ExprAttributes{GetQType<int64_t>()},
                   ExprAttributes{TypedValue::FromValue(Text("foo"))},
                   ExprAttributes{GetQType<int64_t>()}}),
              IsOkAndHolds(EqualsAttr(GetQType<int64_t>())));
}

}  // namespace
}  // namespace arolla::expr
