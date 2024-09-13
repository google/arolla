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
#include "arolla/expr/expr_visitor.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::EqualsExpr;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::Pointer;

size_t CountNodes(const ExprNodePtr& expr) {
  size_t result = 0;
  return PostOrderTraverse(
      expr,
      [&](const ExprNodePtr& /*node*/,
          absl::Span<const size_t* const> /*visits*/) { return ++result; });
}

class ExprVisitorTest : public ::testing::Test {
 public:
  template <typename... Args>
  ExprNodePtr Bar(Args&&... args) {
    return CallOp(bar_, {std::forward<Args>(args)...}).value();
  }

  template <typename... Args>
  ExprNodePtr Baz(Args&&... args) {
    return CallOp(baz_, {std::forward<Args>(args)...}).value();
  }

  template <typename... Args>
  ExprNodePtr Qux(Args&&... args) {
    return CallOp(qux_, {std::forward<Args>(args)...}).value();
  }

 protected:
  ExprOperatorPtr bar_ = std::make_shared<DummyOp>(
      "bar", ExprOperatorSignature::MakeVariadicArgs());
  ExprOperatorPtr baz_ = std::make_shared<DummyOp>(
      "baz", ExprOperatorSignature::MakeVariadicArgs());
  ExprOperatorPtr qux_ = std::make_shared<DummyOp>(
      "qux", ExprOperatorSignature::MakeVariadicArgs());
};

TEST_F(ExprVisitorTest, PostOrder_Trivial) {
  auto x0 = Leaf("x0");
  PostOrder post_order(x0);
  ASSERT_THAT(post_order.nodes(), ElementsAre(Pointer(x0.get())));
  ASSERT_THAT(post_order.dep_indices(0), ElementsAre());
}

TEST_F(ExprVisitorTest, PostOrder) {
  auto x0 = Leaf("x0");
  auto x1 = Leaf("x1");
  auto x2 = Leaf("x2");
  auto add01 = Bar(x0, x1);
  auto add012 = Bar(add01, x0, x1, x2);
  PostOrder post_order(add012);
  ASSERT_THAT(
      post_order.nodes(),
      ElementsAre(Pointer(x0.get()), Pointer(x1.get()), Pointer(add01.get()),
                  Pointer(x2.get()), Pointer(add012.get())));
  ASSERT_THAT(post_order.dep_indices(0), ElementsAre());
  ASSERT_THAT(post_order.dep_indices(1), ElementsAre());
  ASSERT_THAT(post_order.dep_indices(2), ElementsAre(0, 1));
  ASSERT_THAT(post_order.dep_indices(3), ElementsAre());
  ASSERT_THAT(post_order.dep_indices(4), ElementsAre(2, 0, 1, 3));
}

TEST_F(ExprVisitorTest, VisitOrder) {
  auto x0 = Leaf("x0");
  auto x1 = Leaf("x1");
  auto x2 = Leaf("x2");
  auto add01 = Bar(x0, x1);
  auto add012 = Bar(add01, x2);
  std::vector<ExprNodePtr> actual_order = VisitorOrder(add012);
  ASSERT_THAT(actual_order, ElementsAre(Pointer(x0.get()), Pointer(x1.get()),
                                        Pointer(add01.get()), Pointer(x2.get()),
                                        Pointer(add012.get())));
}

TEST_F(ExprVisitorTest, PreAndPostVisitorOrder) {
  auto x0 = Leaf("x0");
  auto x1 = Leaf("x1");
  auto x2 = Leaf("x2");
  auto add01 = Bar(x0, x1);
  auto add012 = Bar(add01, x2);
  std::vector<std::pair<bool, ExprNodePtr>> actual_order =
      PreAndPostVisitorOrder(add012);
  ASSERT_THAT(
      actual_order,
      ElementsAre(
          Pair(true, Pointer(add012.get())), Pair(true, Pointer(add01.get())),
          Pair(true, Pointer(x0.get())), Pair(false, Pointer(x0.get())),
          Pair(true, Pointer(x1.get())), Pair(false, Pointer(x1.get())),
          Pair(false, Pointer(add01.get())), Pair(true, Pointer(x2.get())),
          Pair(false, Pointer(x2.get())), Pair(false, Pointer(add012.get()))));
}

TEST_F(ExprVisitorTest, PostOrderTraverseBool) {
  ASSERT_TRUE(PostOrderTraverse(
      Leaf("x"),
      [](ExprNodePtr, absl::Span<bool const* const>) -> bool { return true; }));
}

TEST_F(ExprVisitorTest, PostOrderTraverseStatusOrBool) {
  ASSERT_THAT(PostOrderTraverse(Leaf("x"),
                                [](ExprNodePtr, absl::Span<bool const* const>) {
                                  return absl::StatusOr<bool>(true);
                                }),
              IsOkAndHolds(true));
}

TEST_F(ExprVisitorTest, VisitLeaf) { ASSERT_EQ(CountNodes(Leaf("x")), 1); }

TEST_F(ExprVisitorTest, VisitOperator) {
  ASSERT_EQ(CountNodes(Bar(Leaf("x"), Leaf("y"))), 3);
}

TEST_F(ExprVisitorTest, LargeAst) {
  ASSERT_EQ(CountNodes(Bar(Bar(Leaf("x"), Leaf("y")), Leaf("x"))), 4);
}

TEST_F(ExprVisitorTest, Transform_WithStatusOrFn) {
  auto expr = Bar(Bar(Baz(Leaf("a"), Leaf("b")), Leaf("c")), Leaf("d"));

  // Replace each "bar" with "qux".
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr expr_with_qux,
      Transform(expr, [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        if (node->op() == bar_) {
          return WithNewOperator(node, qux_);
        }
        return node;
      }));
  ASSERT_THAT(
      expr_with_qux,
      EqualsExpr(Qux(Qux(Baz(Leaf("a"), Leaf("b")), Leaf("c")), Leaf("d"))));
  // The inner "baz" node must not be recreated, so the pointer must be
  // identical to the original.
  EXPECT_THAT(expr_with_qux->node_deps()[0]->node_deps()[0].get(),
              Eq(expr->node_deps()[0]->node_deps()[0].get()));
}

TEST_F(ExprVisitorTest, Transform_WithNoStatusFn) {
  auto expr = Bar(Bar(Baz(Leaf("a"), Leaf("b")), Leaf("c")), Leaf("d"));

  // Replace each "bar" with its first argument.
  EXPECT_THAT(Transform(expr,
                        [&](ExprNodePtr node) -> ExprNodePtr {
                          if (node->op() == bar_) {
                            return node->node_deps()[0];
                          } else {
                            return node;
                          }
                        }),
              // The inner "math.add" node must not be recreated, so the pointer
              // must be identical to the original.
              IsOkAndHolds(EqualsExpr(expr->node_deps()[0]->node_deps()[0])));
}

TEST_F(ExprVisitorTest, Transform_NoChangeRequired) {
  auto expr = Baz(Bar(Baz(Leaf("a"), Leaf("b")), Leaf("c")), Leaf("d"));
  // No new nodes should be created in Transform, so the pointer must be
  // identical to the original.
  EXPECT_THAT(Transform(expr, [](ExprNodePtr node) { return node; }),
              IsOkAndHolds(EqualsExpr(expr)));
}

class DeepTransformTest : public ::testing::Test {
 public:
  template <typename... Args>
  ExprNodePtr A(Args&&... args) {
    return CallOp(a_, {std::forward<Args>(args)...}).value();
  }

  template <typename... Args>
  ExprNodePtr B(Args&&... args) {
    return CallOp(b_, {std::forward<Args>(args)...}).value();
  }

  template <typename... Args>
  ExprNodePtr S(Args&&... args) {
    return CallOp(s_, {std::forward<Args>(args)...}).value();
  }

  template <typename... Args>
  ExprNodePtr C(Args&&... args) {
    return CallOp(c_, {std::forward<Args>(args)...}).value();
  }

  // This function provides the following transformation:
  //
  //   s(x1(...), x2(...), ...) -> a(s(...), s(...), ...)
  //   a(x1(...), x2(...), ...) -> b(s(...), s(...), ...)
  //           b(...) -> b(...)
  //   c(x1, x2, ...) -> b(b(x1), b(x2), ...)
  //
  auto SabTransform()
      -> std::function<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> {
    return [this, visited = absl::flat_hash_set<Fingerprint>()](
               ExprNodePtr node) mutable -> absl::StatusOr<ExprNodePtr> {
      EXPECT_TRUE(visited.emplace(node->fingerprint()).second)
          << "duplicate call to transform_fn";
      if (node->op() == s_) {
        std::vector<absl::StatusOr<ExprNodePtr>> new_deps;
        for (auto& dep : node->node_deps()) {
          new_deps.push_back(WithNewOperator(dep, s_));
        }
        return CallOp(a_, new_deps);
      }
      if (node->op() == a_) {
        std::vector<absl::StatusOr<ExprNodePtr>> new_deps;
        for (auto& dep : node->node_deps()) {
          new_deps.push_back(WithNewOperator(dep, s_));
        }
        return CallOp(b_, new_deps);
      }
      if (node->op() == c_) {
        std::vector<absl::StatusOr<ExprNodePtr>> new_deps;
        for (auto& dep : node->node_deps()) {
          new_deps.push_back(CallOp(b_, {dep}));
        }
        return CallOp(b_, new_deps);
      }
      return node;
    };
  }

 private:
  ExprOperatorPtr a_ =
      std::make_shared<DummyOp>("a", ExprOperatorSignature::MakeVariadicArgs());

  ExprOperatorPtr b_ =
      std::make_shared<DummyOp>("b", ExprOperatorSignature::MakeVariadicArgs());

  ExprOperatorPtr c_ =
      std::make_shared<DummyOp>("c", ExprOperatorSignature::MakeVariadicArgs());

  ExprOperatorPtr s_ =
      std::make_shared<DummyOp>("s", ExprOperatorSignature::MakeVariadicArgs());
};

TEST_F(DeepTransformTest, Trivial) {
  ASSERT_THAT(DeepTransform(A(), SabTransform()),
              IsOkAndHolds(EqualsExpr(B())));
  ASSERT_THAT(DeepTransform(B(), SabTransform()),
              IsOkAndHolds(EqualsExpr(B())));
  ASSERT_THAT(DeepTransform(S(), SabTransform()),
              IsOkAndHolds(EqualsExpr(B())));
}

TEST_F(DeepTransformTest, CacheHitCoverage) {
  {
    auto expr = B(A(A()), A(S()));
    auto expected = B(B(B()), B(B()));
    ASSERT_THAT(DeepTransform(expr, SabTransform()),
                IsOkAndHolds(EqualsExpr(expected)));
  }
  {
    auto expr = B(B(S()), A(S()));
    auto expected = B(B(B()), B(B()));
    ASSERT_THAT(DeepTransform(expr, SabTransform()),
                IsOkAndHolds(EqualsExpr(expected)));
  }
}

TEST_F(DeepTransformTest, TooManyProcessedNodes) {
  ASSERT_THAT(DeepTransform(
                  Literal<int>(0),
                  [](ExprNodePtr node) {
                    return Literal<int>(node->qvalue()->UnsafeAs<int>() + 1);
                  },
                  /*log_transformation_fn=*/std::nullopt,
                  /*processed_node_limit=*/1000),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("too many processed nodes")));
}

TEST_F(DeepTransformTest, LogTransformationFn) {
  std::string trace;
  auto transformations_logger = [&trace](ExprNodePtr a, ExprNodePtr b,
                                         DeepTransformStage stage) {
    if (stage == DeepTransformStage::kWithNewDeps) {
      if (a->fingerprint() != b->fingerprint()) {
        trace += GetDebugSnippet(b) +
                 " got new dependencies: " + GetDebugSnippet(a) + "\n";
      }
    } else if (stage == DeepTransformStage::kNewChildAfterTransformation) {
      trace += GetDebugSnippet(b) + " contains " + GetDebugSnippet(a) + "\n";
    }
  };
  ASSERT_OK(DeepTransform(C(A()), SabTransform(),
                          /*log_transformation_fn=*/transformations_logger));
  EXPECT_EQ(
      "c(a():INT32):INT32 got new dependencies: c(b():INT32):INT32\n"
      "b(b(...):INT32):INT32 contains b(b():INT32):INT32\n",
      trace);
}

TEST_F(DeepTransformTest, InfiniteLoop) {
  ASSERT_THAT(DeepTransform(S(), [&](ExprNodePtr) { return S(S()); }),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("infinite loop of node transformations "
                                 "containing node s(s():INT32):INT32")));
}

TEST_F(DeepTransformTest, UnaryRecursion) {
  auto expr = S();
  auto expected = B();
  for (int i = 0; i < 10; ++i) {
    expr = S(expr);
    expected = B(expected);
  }
  ASSERT_THAT(DeepTransform(expr, SabTransform()),
              IsOkAndHolds(EqualsExpr(expected)));
}

TEST_F(DeepTransformTest, UnaryRecursionStress) {
  auto expr = S();
  auto expected = B();
  for (int i = 0; i < 1000; ++i) {
    expr = S(expr);
    expected = B(expected);
  }
  ASSERT_THAT(DeepTransform(expr, SabTransform()),
              IsOkAndHolds(EqualsExpr(expected)));
}

TEST_F(DeepTransformTest, BinaryRecursion) {
  auto expr = S();
  auto expected = B();
  for (int i = 0; i < 10; ++i) {
    expr = S(expr, expr);
    expected = B(expected, expected);
  }
  ASSERT_THAT(DeepTransform(expr, SabTransform()),
              IsOkAndHolds(EqualsExpr(expected)));
}

TEST_F(DeepTransformTest, BinaryRecursionStress) {
  auto expr = S();
  auto expected = B();
  for (int i = 0; i < 1000; ++i) {
    expr = S(expr, expr);
    expected = B(expected, expected);
  }
  ASSERT_THAT(DeepTransform(expr, SabTransform()),
              IsOkAndHolds(EqualsExpr(expected)));
}

TEST_F(DeepTransformTest, TernaryRecursionStress) {
  auto expr = S();
  auto expected = B();
  for (int i = 0; i < 1000; ++i) {
    expr = S(expr, expr, expr);
    expected = B(expected, expected, expected);
  }
  ASSERT_THAT(DeepTransform(expr, SabTransform()),
              IsOkAndHolds(EqualsExpr(expected)));
}

TEST_F(DeepTransformTest, ComplexRecursionStress) {
  auto expr = S();
  auto expected = B();
  for (int i = 0; i < 1000; ++i) {
    expr = S(A(expr), B(expr, expected), expr);
    expected = B(B(expected), B(expected, expected), expected);
  }
  ASSERT_THAT(DeepTransform(expr, SabTransform()),
              IsOkAndHolds(EqualsExpr(expected)));
}

}  // namespace
}  // namespace arolla::expr
