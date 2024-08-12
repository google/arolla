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
#include "arolla/expr/quote.h"

#include <memory>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::arolla::expr::testing::DummyOp;
using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Ne;

class ExprQuoteTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }

 protected:
  ExprOperatorPtr op_ = std::make_shared<DummyOp>(
      "op", ExprOperatorSignature::MakeVariadicArgs());
};

TEST_F(ExprQuoteTest, Empty) {
  ExprQuote quote;
  EXPECT_THAT(quote.has_expr(), IsFalse());
  EXPECT_THAT(quote.expr(), StatusIs(absl::StatusCode::kInvalidArgument,
                                     "uninitialized ExprQuote"));
}

TEST_F(ExprQuoteTest, NotEmpty) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op_, {Leaf("x")}));
  ExprQuote quote(expr);
  EXPECT_THAT(quote.has_expr(), IsTrue());
  ASSERT_THAT(quote.expr(), IsOk());
  EXPECT_THAT(quote.expr()->get(), Eq(expr.get()));
  EXPECT_THAT(quote->get(), Eq(expr.get()));
}

TEST_F(ExprQuoteTest, DenseArray) {
  ASSERT_OK_AND_ASSIGN(auto expr_1, CallOp(op_, {Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto expr_2, CallOp(op_, {Leaf("y")}));
  auto array = CreateDenseArray<expr::ExprQuote>(
      {ExprQuote(expr_1), std::nullopt, ExprQuote(expr_2)});

  EXPECT_TRUE(array[0].present);
  EXPECT_FALSE(array[1].present);
  EXPECT_TRUE(array[2].present);

  EXPECT_EQ(array[0].value, ExprQuote(expr_1));
  EXPECT_EQ(array[2].value, ExprQuote(expr_2));
}

TEST_F(ExprQuoteTest, AbslHash) {
  ASSERT_OK_AND_ASSIGN(auto expr_1, CallOp(op_, {Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto expr_2, CallOp(op_, {Leaf("y")}));
  ASSERT_OK_AND_ASSIGN(auto expr_3, CallOp(op_, {Leaf("z")}));
  // Equal to make sure they have the same hash within the same equivalence
  // class.
  ASSERT_OK_AND_ASSIGN(auto expr_4, CallOp(op_, {Leaf("x")}));
  std::vector cases{
      ExprQuote(expr_1),
      ExprQuote(expr_2),
      ExprQuote(expr_3),
      ExprQuote(expr_4),
  };

  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
}

TEST_F(ExprQuoteTest, Fingerprint) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op_, {Leaf("x")}));
  EXPECT_THAT(ExprQuote(expr).expr_fingerprint(), Eq(expr->fingerprint()));
  EXPECT_THAT(ExprQuote().expr_fingerprint(), Ne(expr->fingerprint()));
}

TEST_F(ExprQuoteTest, FingerprintHasher) {
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op_, {Leaf("x")}));
  ExprQuote quote(expr);

  auto quote_fingerprint = FingerprintHasher("").Combine(quote).Finish();
  EXPECT_THAT(quote_fingerprint, Ne(expr->fingerprint()));
  EXPECT_THAT(FingerprintHasher("").Combine(quote).Finish(),
              Eq(quote_fingerprint));
}

TEST_F(ExprQuoteTest, Repr) {
  EXPECT_THAT(Repr(ExprQuote()), Eq("ExprQuote(nullptr)"));

  Text text_with_quote{"some\"\ntext"};
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp(op_, {Leaf("x"), Literal(text_with_quote)}));

  ExprQuote quote{expr};
  EXPECT_THAT(Repr(text_with_quote), Eq("'some\\\"\\ntext'"));
  EXPECT_THAT(Repr(quote),
              Eq("ExprQuote('op(L.x, \\'some\\\\\\\"\\\\ntext\\')')"));
}

}  // namespace
}  // namespace arolla::expr
