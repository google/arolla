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
#include "arolla/expr/basic_expr_operator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::NotNull;
using ::testing::Return;

using ::arolla::testing::EqualsAttr;
using ::arolla::testing::EqualsExpr;

using Attr = ::arolla::expr::ExprAttributes;

class MockBasicOperator : public BasicExprOperator {
 public:
  MockBasicOperator(absl::string_view name, ExprOperatorSignature signature,
                    absl::string_view doc, Fingerprint fingerprint,
                    ExprOperatorTags tags = ExprOperatorTags::kNone)
      : BasicExprOperator(name, signature, doc, fingerprint, tags) {}

  MOCK_METHOD(absl::StatusOr<QTypePtr>, GetOutputQType,
              (absl::Span<const QTypePtr> input_qtypes), (const, override));
};

TEST(BasicExprOperatorTest, Basics) {
  ExprOperatorSignature signature{{"x"}};
  auto op = std::make_shared<MockBasicOperator>(
      "test_op", signature, "doc string", Fingerprint{0x1234},
      ExprOperatorTags::kAnnotation);
  EXPECT_EQ(op->display_name(), "test_op");
  EXPECT_EQ(op->fingerprint(), Fingerprint{0x1234});
  EXPECT_EQ(op->tags(), ExprOperatorTags::kAnnotation);
  ASSERT_OK_AND_ASSIGN(auto retrieved_sig, op->GetSignature());
  ASSERT_THAT(retrieved_sig, NotNull());
  EXPECT_EQ(retrieved_sig->parameters.size(), 1);
  EXPECT_EQ(retrieved_sig->parameters[0].name, "x");
  EXPECT_EQ(op->signature().parameters.size(), 1);
  EXPECT_THAT(op->GetDoc(), IsOkAndHolds("doc string"));
  EXPECT_EQ(op->doc(), "doc string");
}

TEST(BasicExprOperatorTest, ValidateNodeDepsCount) {
  auto op = std::make_shared<MockBasicOperator>(
      "test_op", ExprOperatorSignature::MakeArgsN(2), "doc",
      Fingerprint{0x1234});
  auto expr = ExprNode::UnsafeMakeOperatorNode(op, {Leaf("x"), Leaf("y")}, {});
  EXPECT_THAT(op->ToLowerLevel(expr), IsOkAndHolds(EqualsExpr(expr)));
  EXPECT_THAT(
      op->ToLowerLevel(ExprNode::UnsafeMakeOperatorNode(op, {Leaf("x")}, {})),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("incorrect number of dependencies passed to an "
                         "operator node: expected 2 but got 1")));
}

TEST(BasicExprOperatorTest, ValidateOpInputsCount) {
  auto op = std::make_shared<MockBasicOperator>(
      "test_op", ExprOperatorSignature::MakeArgsN(2), "doc",
      Fingerprint{0x1234});
  EXPECT_CALL(*op, GetOutputQType(
                       ElementsAre(GetQType<int32_t>(), GetQType<int32_t>())))
      .WillOnce(Return(GetQType<int32_t>()));
  EXPECT_THAT(op->InferAttributes(
                  {Attr(GetQType<int32_t>()), Attr(GetQType<int32_t>())}),
              IsOkAndHolds(EqualsAttr(GetQType<int32_t>())));
  EXPECT_THAT(op->InferAttributes({Attr{}, Attr(GetQType<int32_t>())}),
              IsOkAndHolds(EqualsAttr(Attr())));
  EXPECT_THAT(op->InferAttributes({Attr{}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("incorrect number of dependencies passed to "
                                 "an operator node: expected 2 but got 1")));
}

TEST(BasicExprOperatorTest, GetOutputQTypeReturnsNullptr) {
  auto op = std::make_shared<MockBasicOperator>(
      "test_op", ExprOperatorSignature{}, "doc", Fingerprint{0x1234});
  EXPECT_CALL(*op, GetOutputQType(ElementsAre())).WillOnce(Return(nullptr));
  EXPECT_THAT(op->InferAttributes({}),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("GetOutputQType() for operator test_op "
                                 "returned nullptr")));
}

}  // namespace
}  // namespace arolla::expr
