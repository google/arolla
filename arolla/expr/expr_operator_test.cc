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
#include "arolla/expr/expr_operator.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/repr.h"

namespace arolla::expr {
namespace {

using ::arolla::testing::MockExprOperator;
using ::testing::MatchesRegex;
using ::testing::Return;

TEST(ExprOperatorTest, HasExprOperatorTag) {
  auto regular_op = MockExprOperator::MakeStrict({
      .tags = ExprOperatorTags::kNone,
  });
  auto backend_op = MockExprOperator::MakeStrict({
      .tags = ExprOperatorTags::kBackend,
  });
  auto builtin_op = MockExprOperator::MakeStrict({
      .tags = ExprOperatorTags::kBuiltin,
  });
  auto annotation_op = MockExprOperator::MakeStrict({
      .tags = ExprOperatorTags::kAnnotation,
  });

  EXPECT_FALSE(HasBackendExprOperatorTag(nullptr));
  EXPECT_FALSE(HasBackendExprOperatorTag(regular_op));
  EXPECT_TRUE(HasBackendExprOperatorTag(backend_op));
  EXPECT_FALSE(HasBackendExprOperatorTag(builtin_op));
  EXPECT_FALSE(HasBackendExprOperatorTag(annotation_op));

  EXPECT_FALSE(HasBuiltinExprOperatorTag(nullptr));
  EXPECT_FALSE(HasBuiltinExprOperatorTag(regular_op));
  EXPECT_FALSE(HasBuiltinExprOperatorTag(backend_op));
  EXPECT_TRUE(HasBuiltinExprOperatorTag(builtin_op));
  EXPECT_TRUE(HasBuiltinExprOperatorTag(annotation_op));

  EXPECT_FALSE(HasAnnotationExprOperatorTag(nullptr));
  EXPECT_FALSE(HasAnnotationExprOperatorTag(regular_op));
  EXPECT_FALSE(HasAnnotationExprOperatorTag(backend_op));
  EXPECT_FALSE(HasAnnotationExprOperatorTag(builtin_op));
  EXPECT_TRUE(HasAnnotationExprOperatorTag(annotation_op));
}

TEST(ExprOperatorTest, IsBackendOperator) {
  auto regular_op = MockExprOperator::MakeStrict({
      .name = "op.name",
      .tags = ExprOperatorTags::kNone,
  });
  auto backend_op = MockExprOperator::MakeStrict({
      .name = "op.name",
      .tags = ExprOperatorTags::kBackend,
  });

  EXPECT_FALSE(IsBackendOperator(nullptr, "op.name"));
  EXPECT_FALSE(IsBackendOperator(regular_op, "op.name"));
  EXPECT_TRUE(IsBackendOperator(backend_op, "op.name"));
  EXPECT_FALSE(IsBackendOperator(backend_op, "op.other_name"));
}

TEST(ExprOperatorTest, ReprWithoutPyQValueSpecializationKey) {
  auto op = MockExprOperator::Make({.name = "op'name"});
  EXPECT_THAT(
      Repr(ExprOperatorPtr(op)),
      MatchesRegex("<Operator with name='op\\\\'name', hash=0x[0-9a-f]+, "
                   "cxx_type='MockExprOperator'>"));
}

TEST(ExprOperatorTest, ReprWithPyQValueSpecializationKey) {
  auto op = MockExprOperator::Make({.name = "op'name"});
  EXPECT_CALL(*op, py_qvalue_specialization_key())
      .WillRepeatedly(Return("foo'bar"));
  EXPECT_THAT(
      Repr(ExprOperatorPtr(op)),
      MatchesRegex("<Operator with name='op\\\\'name', hash=0x[0-9a-f]+, "
                   "cxx_type='MockExprOperator', key='foo\\\\'bar'>"));
}

}  // namespace
}  // namespace arolla::expr
