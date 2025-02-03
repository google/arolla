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
#include "arolla/expr/operator_repr_functions.h"

#include <memory>
#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla::expr {
namespace {

using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::ReprTokenEq;
using ::testing::Optional;

std::optional<ReprToken> AddRepr(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  const auto& x_token = node_tokens.at(node->node_deps()[0]->fingerprint());
  const auto& y_token = node_tokens.at(node->node_deps()[1]->fingerprint());
  return ReprToken{.str = absl::StrFormat("%s + %s", x_token.str, y_token.str),
                   .precedence = ReprToken::kSafeForSubscription};
}

std::optional<ReprToken> SubtractRepr(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  const auto& x_token = node_tokens.at(node->node_deps()[0]->fingerprint());
  const auto& y_token = node_tokens.at(node->node_deps()[1]->fingerprint());
  return ReprToken{.str = absl::StrFormat("%s - %s", x_token.str, y_token.str),
                   .precedence = ReprToken::kSafeForArithmetic};
}

TEST(OperatorReprFunctionsTest, OpClass) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto expr = ExprNode::UnsafeMakeOperatorNode(
      std::make_shared<DummyOp>("custom.add",
                                ExprOperatorSignature({{"x"}, {"y"}})),
      {x, y}, ExprAttributes());
  absl::flat_hash_map<Fingerprint, ReprToken> node_tokens = {
      {x->fingerprint(), ReprToken{.str = "L.x"}},
      {y->fingerprint(), ReprToken{.str = "L.y"}},
  };
  absl::string_view specialization_key =
      expr->op()->py_qvalue_specialization_key();

  {
    // No registration.
    EXPECT_EQ(FormatOperatorNodePretty(expr, node_tokens), std::nullopt);
  }

  {
    // Initial registration.
    RegisterOpReprFnByQValueSpecializationKey(std::string(specialization_key),
                                              AddRepr);
    EXPECT_THAT(
        FormatOperatorNodePretty(expr, node_tokens),
        Optional(ReprTokenEq("L.x + L.y", ReprToken::kSafeForSubscription)));
  }

  {
    // Re-registration.
    RegisterOpReprFnByQValueSpecializationKey(std::string(specialization_key),
                                              SubtractRepr);
    EXPECT_THAT(
        FormatOperatorNodePretty(expr, node_tokens),
        Optional(ReprTokenEq("L.x - L.y", ReprToken::kSafeForArithmetic)));
  }
}

TEST(OperatorReprFunctionsTest, RegisteredOp) {
  auto x = Leaf("x");
  auto y = Leaf("y");
  auto expr = ExprNode::UnsafeMakeOperatorNode(
      std::make_shared<RegisteredOperator>("test.add"), {x, y},
      ExprAttributes());
  absl::flat_hash_map<Fingerprint, ReprToken> node_tokens = {
      {x->fingerprint(), ReprToken{.str = "L.x"}},
      {y->fingerprint(), ReprToken{.str = "L.y"}},
  };

  {
    // No registration.
    EXPECT_EQ(FormatOperatorNodePretty(expr, node_tokens), std::nullopt);
  }

  {
    // Initial registration.
    RegisterOpReprFnByByRegistrationName("test.add", AddRepr);
    EXPECT_THAT(
        FormatOperatorNodePretty(expr, node_tokens),
        Optional(ReprTokenEq("L.x + L.y", ReprToken::kSafeForSubscription)));
  }

  {
    // Re-registration.
    RegisterOpReprFnByByRegistrationName("test.add", SubtractRepr);
    EXPECT_THAT(
        FormatOperatorNodePretty(expr, node_tokens),
        Optional(ReprTokenEq("L.x - L.y", ReprToken::kSafeForArithmetic)));
  }
}

}  // namespace
}  // namespace arolla::expr
