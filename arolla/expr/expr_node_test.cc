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
#include "arolla/expr/expr_node.h"

#include <memory>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"

namespace arolla::expr {
namespace {

using ::arolla::expr::testing::DummyOp;

TEST(ExprNodeTest, ExprNodeTypeIsConvertibleToString) {
  std::stringstream ss;
  ss << ExprNodeType::kLiteral;
  EXPECT_EQ(ss.str(), "kLiteral");

  ss.str("");
  ss << ExprNodeType::kLeaf;
  EXPECT_EQ(ss.str(), "kLeaf");

  ss.str("");
  ss << ExprNodeType::kOperator;
  EXPECT_EQ(ss.str(), "kOperator");

  ss.str("");
  ss << ExprNodeType::kPlaceholder;
  EXPECT_EQ(ss.str(), "kPlaceholder");

  ss.str("");
  ss << static_cast<ExprNodeType>(255);
  EXPECT_EQ(ss.str(), "ExprNodeType(255)");
}

TEST(ExprNodeTest, DeepTreeNoStackOverflow) {
#ifndef NDEBUG
  constexpr int depth = 50000;
#else
  constexpr int depth = 1000000;
#endif
  ExprOperatorPtr op = std::make_shared<DummyOp>(
      "op.name", ExprOperatorSignature::MakeVariadicArgs());
  auto a = ExprNode::MakeLeafNode("a");
  auto deep = a;
  for (int i = depth; i != 0; --i) {
    deep = ExprNode::UnsafeMakeOperatorNode(ExprOperatorPtr(op), {deep, a}, {});
  }
}

// NOTE: msan regression test to ensure that the ExprNode
// destructor does not cause use-of-uninitialized-value issues.
using ExprNodeMsanTest = ::testing::TestWithParam<ExprNodePtr>;

TEST_P(ExprNodeMsanTest, Msan) {
  const auto& expr = GetParam();
  ASSERT_NE(expr, nullptr);
}

INSTANTIATE_TEST_SUITE_P(ExprNodeMsanTestSuite, ExprNodeMsanTest,
                         ::testing::ValuesIn([]() -> std::vector<ExprNodePtr> {
                           constexpr int depth = 64;
                           ExprOperatorPtr op = std::make_shared<DummyOp>(
                               "op.name",
                               ExprOperatorSignature::MakeVariadicArgs());
                           auto expr = ExprNode::MakeLeafNode("a");
                           for (int i = depth; i != 0; --i) {
                             expr = ExprNode::UnsafeMakeOperatorNode(
                                 ExprOperatorPtr(op), {expr}, {});
                           }
                           return {{expr}};
                         }()));

}  // namespace
}  // namespace arolla::expr
