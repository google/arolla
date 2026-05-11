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
#include "arolla/expr/registered_expr_operator.h"

#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/unit.h"

namespace arolla::expr {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::testing::DummyOp;
using ::arolla::testing::MockExprOperator;
using ::arolla::testing::ReprTokenEq;
using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::Not;
using ::testing::NotNull;

TEST(RegisteredOperatorTest, Tags) {
  auto op = MockExprOperator::Make({
      .tags = ExprOperatorTags::kBackend,
  });
  ASSERT_OK_AND_ASSIGN(
      auto reg_op,
      RegisterOperator("registered_operator_test.test_tags.op", op));
  EXPECT_FALSE(HasBackendExprOperatorTag(reg_op));
  EXPECT_FALSE(HasBuiltinExprOperatorTag(reg_op));
  EXPECT_FALSE(HasAnnotationExprOperatorTag(reg_op));
}

TEST(RegisteredOperatorTest, RegistryBasics) {
  constexpr absl::string_view kRegOpName = "registered_operator_test.basics.op";
  ExprOperatorPtr op = MockExprOperator::MakeStrict();
  ExprOperatorRegistry registry;
  EXPECT_THAT(registry.LookupOperatorOrNull(kRegOpName), IsNull());
  ASSERT_THAT(registry.Register(kRegOpName, op), IsOk());
  EXPECT_THAT(registry.LookupOperatorOrNull(kRegOpName), NotNull());
  EXPECT_THAT(
      registry.Register(kRegOpName, MockExprOperator::MakeStrict()),
      StatusIs(absl::StatusCode::kAlreadyExists,
               "operator 'registered_operator_test.basics.op' already exists"));
}

TEST(RegisteredOperatorTest, RegisterOperator_GetSignature) {
  ASSERT_OK_AND_ASSIGN(
      auto reg_op,
      RegisterOperator("registered_operator_test.test_signature.op",
                       MockExprOperator::Make({
                           .signature = ExprOperatorSignature::MakeArgsN(3),
                       })));
  ASSERT_OK_AND_ASSIGN(auto signature, reg_op->GetSignature());
  EXPECT_EQ(signature->parameters.size(), 3);
  EXPECT_EQ(signature->parameters[0].name, "arg1");
  EXPECT_EQ(signature->parameters[1].name, "arg2");
  EXPECT_EQ(signature->parameters[2].name, "arg3");
}

TEST(RegisteredOperatorTest, RegisterOperator_GetDoc) {
  ASSERT_OK_AND_ASSIGN(auto reg_op,
                       RegisterOperator("registered_operator_test.test_doc.op",
                                        MockExprOperator::Make({
                                            .doc = "dummy_docstring",
                                        })));
  ASSERT_THAT(reg_op->GetDoc(), IsOkAndHolds("dummy_docstring"));
}

TEST(RegisteredOperatorTest, RegistrationOrder) {
  ExprOperatorRegistry registry;
  auto op = MockExprOperator::MakeStrict();
  ASSERT_THAT(registry.Register("op.name_1", op), IsOk());
  ASSERT_THAT(registry.Register("op.name_3", op), IsOk());
  ASSERT_THAT(registry.Register("op.name_2", op), IsOk());
  EXPECT_THAT(registry.ListRegisteredOperators(),
              ElementsAre("op.name_1", "op.name_3", "op.name_2"));
}

TEST(RegisteredOperatorTest, Repr) {
  auto op = std::make_shared<RegisteredOperator>("foo'bar");
  EXPECT_THAT(op->GenReprToken(),
              ReprTokenEq("<RegisteredOperator 'foo\\'bar'>"));
}

TEST(RegisteredOperatorTest, IsRegisteredOperator) {
  auto op = MockExprOperator::MakeStrict();
  ASSERT_OK_AND_ASSIGN(
      auto reg_op,
      RegisterOperator("registered_operator_test.is_registered_operator.op",
                       op));
  EXPECT_FALSE(IsRegisteredOperator(nullptr));
  EXPECT_FALSE(IsRegisteredOperator(op));
  EXPECT_TRUE(IsRegisteredOperator(reg_op));
}

TEST(RegisteredOperatorTest, DecayRegisteredOperator) {
  constexpr absl::string_view kRegOpName =
      "registered_operator_test.decay_registered_operator.op";
  constexpr absl::string_view kAliasOpName =
      "registered_operator_test.decay_registered_operator.alias_op";
  auto op = MockExprOperator::Make({
      .name = "dummy.op",
  });
  ASSERT_OK_AND_ASSIGN(auto reg_op, RegisterOperator(kRegOpName, op));
  ASSERT_OK_AND_ASSIGN(auto alias_op,
                       RegisterOperatorAlias(kAliasOpName, kRegOpName));
  ASSERT_THAT(DecayRegisteredOperator(reg_op), IsOkAndHolds(op));
  ASSERT_THAT(DecayRegisteredOperator(alias_op), IsOkAndHolds(op));
}

TEST(RegisteredOperatorTest, UnsafeUnregister) {
  constexpr absl::string_view kRegOpName =
      "registered_operator_test.unsafe_unregistration.op";
  auto& registry = *ExprOperatorRegistry::GetInstance();
  auto op = MockExprOperator::MakeStrict();
  ASSERT_THAT(registry.Register(kRegOpName, op), IsOk());
  ASSERT_THAT(registry.LookupOperatorOrNull(kRegOpName), NotNull());
  ASSERT_THAT(registry.ListRegisteredOperators(), Contains(kRegOpName));
  registry.UnsafeUnregister(kRegOpName);
  ASSERT_THAT(registry.LookupOperatorOrNull(kRegOpName), IsNull());
  ASSERT_THAT(registry.ListRegisteredOperators(), Not(Contains(kRegOpName)));
}

TEST(RegisteredOperatorTest, RevisionId) {
  auto& registry = *ExprOperatorRegistry::GetInstance();
  const auto rev_id_fn = registry.AcquireRevisionIdFn("");
  const auto a_rev_id_fn = registry.AcquireRevisionIdFn("a");
  const auto a_b_rev_id_fn = registry.AcquireRevisionIdFn("a.b");
  const auto a_b_op_rev_id_fn = registry.AcquireRevisionIdFn("a.b.op");
  auto op = MockExprOperator::MakeStrict();
  const auto a_b_op_rev_id_0 = a_b_op_rev_id_fn();
  const auto a_b_rev_id_0 = a_b_rev_id_fn();
  const auto a_rev_id_0 = a_rev_id_fn();
  const auto rev_id_0 = rev_id_fn();
  // Registration of 'a.b.op' affects 'a.b.op', 'a.b', 'a', and ''.
  ASSERT_THAT(registry.Register("a.b.op", op), IsOk());
  const auto a_b_op_rev_id_1 = a_b_op_rev_id_fn();
  const auto a_b_rev_id_1 = a_b_rev_id_fn();
  const auto a_rev_id_1 = a_rev_id_fn();
  const auto rev_id_1 = rev_id_fn();
  ASSERT_NE(a_b_op_rev_id_1, a_b_op_rev_id_0);
  ASSERT_NE(a_b_rev_id_1, a_b_rev_id_0);
  ASSERT_NE(a_rev_id_1, a_rev_id_0);
  ASSERT_NE(rev_id_1, rev_id_0);
  // A failed registration doesn't change the revision ids.
  ASSERT_THAT(registry.Register("!@#", op),
              StatusIs(absl::StatusCode::kInvalidArgument));
  ASSERT_THAT(registry.Register("a.b.op", op),
              StatusIs(absl::StatusCode::kAlreadyExists));
  ASSERT_EQ(a_b_op_rev_id_fn(), a_b_op_rev_id_1);
  ASSERT_EQ(a_b_rev_id_fn(), a_b_rev_id_1);
  ASSERT_EQ(a_rev_id_fn(), a_rev_id_1);
  ASSERT_EQ(rev_id_fn(), rev_id_1);
  // Registration of 'a.c.op' affects 'a' and '', but doesn't affect 'a.b.op'
  // and 'a.b'.
  ASSERT_THAT(registry.Register("a.c.op", op), IsOk());
  const auto a_b_op_rev_id_2 = a_b_op_rev_id_fn();
  const auto a_b_rev_id_2 = a_b_rev_id_fn();
  const auto a_rev_id_2 = a_rev_id_fn();
  const auto rev_id_2 = rev_id_fn();
  ASSERT_EQ(a_b_op_rev_id_2, a_b_op_rev_id_1);
  ASSERT_EQ(a_b_rev_id_2, a_b_rev_id_1);
  ASSERT_NE(a_rev_id_2, a_rev_id_1);
  ASSERT_NE(rev_id_2, rev_id_1);
  // Unregistration of non-existing operator doesn't affect anything.
  registry.UnsafeUnregister("a.b.no_op");
  ASSERT_EQ(a_b_op_rev_id_fn(), a_b_op_rev_id_2);
  ASSERT_EQ(a_b_rev_id_fn(), a_b_rev_id_2);
  ASSERT_EQ(a_rev_id_fn(), a_rev_id_2);
  ASSERT_EQ(rev_id_fn(), rev_id_2);
  // Unregistration of 'a.c.op' affects 'a' and '', but doesn't affect 'a.b.op'
  // and 'a.b'.
  registry.UnsafeUnregister("a.c.op");
  const auto a_b_op_rev_id_3 = a_b_op_rev_id_fn();
  const auto a_b_rev_id_3 = a_b_rev_id_fn();
  const auto a_rev_id_3 = a_rev_id_fn();
  const auto rev_id_3 = rev_id_fn();
  ASSERT_EQ(a_b_op_rev_id_3, a_b_op_rev_id_2);
  ASSERT_EQ(a_b_rev_id_3, a_b_rev_id_2);
  ASSERT_NE(a_rev_id_3, a_rev_id_2);
  ASSERT_NE(rev_id_3, rev_id_2);
  // Unregistration of 'a.b.op' affects 'a.b.op', 'a.b', 'a', and ''.
  registry.UnsafeUnregister("a.b.op");
  const auto a_b_op_rev_id_4 = a_b_op_rev_id_fn();
  const auto a_b_rev_id_4 = a_b_rev_id_fn();
  const auto a_rev_id_4 = a_rev_id_fn();
  const auto rev_id_4 = rev_id_fn();
  ASSERT_NE(a_b_op_rev_id_4, a_b_op_rev_id_3);
  ASSERT_NE(a_b_rev_id_4, a_b_rev_id_3);
  ASSERT_NE(a_rev_id_4, a_rev_id_3);
  ASSERT_NE(rev_id_4, rev_id_3);
}

TEST(RegisteredOperatorTest, CircularDepenndencyDetector) {
  auto op_a =
      std::make_shared<RegisteredOperator>("circular_dependency_detector.A");
  auto op_b =
      std::make_shared<RegisteredOperator>("circular_dependency_detector.B");
  ASSERT_OK(RegisterOperator("circular_dependency_detector.A", op_b));
  ASSERT_OK(RegisterOperator("circular_dependency_detector.B", op_a));
  // DecayRegisteredOperator
  EXPECT_THAT(DecayRegisteredOperator(op_a),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("arolla::expr::DecayRegisteredOperator: "
                                 "detected a circular dependency: "
                                 "op_name='circular_dependency_detector.A'")));
  // GetSignature
  EXPECT_THAT(
      op_a->GetSignature(),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("arolla::expr::RegisteredOperator::GetSignature: "
                         "detected a circular dependency: "
                         "op_name='circular_dependency_detector.A'")));
  // GetDoc
  EXPECT_THAT(op_a->GetDoc(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("arolla::expr::RegisteredOperator::GetDoc: "
                                 "detected a circular dependency: "
                                 "op_name='circular_dependency_detector.A'")));
  // InferAttributes
  EXPECT_THAT(
      op_a->InferAttributes({}),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          HasSubstr("arolla::expr::RegisteredOperator::InferAttributes: "
                    "detected a circular dependency: "
                    "op_name='circular_dependency_detector.A', inputs=[]")));
  EXPECT_THAT(
      op_a->InferAttributes({ExprAttributes(GetQType<QTypePtr>())}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("arolla::expr::RegisteredOperator::InferAttributes: "
                         "detected a circular dependency: "
                         "op_name='circular_dependency_detector.A', "
                         "inputs=[Attr(qtype=QTYPE)]")));
  EXPECT_THAT(
      op_a->InferAttributes({ExprAttributes(GetQType<QTypePtr>()),
                             ExprAttributes(TypedValue::FromValue(Unit{}))}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("arolla::expr::RegisteredOperator::InferAttributes: "
                         "detected a circular dependency: "
                         "op_name='circular_dependency_detector.A', "
                         "inputs=[Attr(qtype=QTYPE), Attr(qvalue=unit)]")));
  // ToLowerLevel
  EXPECT_THAT(
      ToLowerNode(ExprNode::UnsafeMakeOperatorNode(op_a, {}, {})),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("arolla::expr::RegisteredOperator::ToLowerLevel: "
                         "detected a circular dependency: "
                         "op_name='circular_dependency_detector.A', "
                         "inputs=[]")));
  EXPECT_THAT(
      ToLowerNode(ExprNode::UnsafeMakeOperatorNode(
          op_a, {Leaf("x"), Literal(Unit{})}, {})),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("arolla::expr::RegisteredOperator::ToLowerLevel: "
                         "detected a circular dependency: "
                         "op_name='circular_dependency_detector.A', "
                         "inputs=[Attr{}, Attr(qvalue=unit)]")));
}

TEST(RegisteredOperatorTest, LongDependencyChain) {
  auto op = MockExprOperator::MakeNice();
  ASSERT_OK_AND_ASSIGN(auto reg_op,
                       RegisterOperator("long_dependency_chain._0", op));
  for (int i = 1; i < 200; ++i) {
    ASSERT_OK_AND_ASSIGN(
        reg_op, RegisterOperator(
                    absl::StrFormat("long_dependency_chain._%d", i), reg_op));
  }
  EXPECT_THAT(DecayRegisteredOperator(reg_op), IsOkAndHolds(op));
  EXPECT_THAT(reg_op->GetDoc(), op->doc());
  EXPECT_THAT(reg_op->GetSignature(), IsOk());
  EXPECT_THAT(reg_op->InferAttributes({}), IsOk());
  EXPECT_THAT(
      ToLowerNode(ExprNode::UnsafeMakeOperatorNode(std::move(reg_op), {}, {})),
      IsOk());
}

absl::StatusOr<ExprOperatorPtr> GetChainOp(int n) {
  static const bool once = ([]{
  ExprOperatorPtr op = std::make_shared<DummyOp>(
      "dummy_op", ExprOperatorSignature::MakeVariadicArgs());
  for (int i = 1; i < 100; ++i) {
    ASSERT_OK_AND_ASSIGN(
        op, RegisterOperator(absl::StrFormat("benchmark.chain_op._%d", i), op));
  }
  }(), true);
  (void)once;
  return LookupOperator(absl::StrFormat("benchmark.chain_op._%d", n));
}

void BM_DecayRegisteredOperator(benchmark::State& state) {
  InitArolla();
  ASSERT_OK_AND_ASSIGN(auto op, GetChainOp(state.range(0)));
  for (auto _ : state) {
    auto tmp = DecayRegisteredOperator(op).ok();
    benchmark::DoNotOptimize(tmp);
  }
}

void BM_GetDoc(benchmark::State& state) {
  InitArolla();
  ASSERT_OK_AND_ASSIGN(auto op, GetChainOp(state.range(0)));
  for (auto _ : state) {
    auto tmp = op->GetDoc();
    benchmark::DoNotOptimize(tmp);
  }
}

void BM_InferAttr(benchmark::State& state) {
  InitArolla();
  ASSERT_OK_AND_ASSIGN(auto op, GetChainOp(state.range(0)));
  std::vector inputs = {ExprAttributes(), ExprAttributes()};
  for (auto _ : state) {
    auto tmp = op->InferAttributes(inputs);
    benchmark::DoNotOptimize(tmp);
  }
}

void BM_ToLowerLevel(benchmark::State& state) {
  InitArolla();
  ASSERT_OK_AND_ASSIGN(auto op, GetChainOp(state.range(0)));
  ASSERT_OK_AND_ASSIGN(auto expr, CallOp(op, {Leaf("x"), Leaf("y")}));
  std::vector inputs = {ExprAttributes(), ExprAttributes()};
  for (auto _ : state) {
    auto tmp = ToLowerNode(expr);
    benchmark::DoNotOptimize(tmp);
  }
}

BENCHMARK(BM_DecayRegisteredOperator)->Arg(1)->Arg(2)->Arg(20);
BENCHMARK(BM_GetDoc)->Arg(1)->Arg(2)->Arg(20);
BENCHMARK(BM_InferAttr)->Arg(1)->Arg(2)->Arg(20);
BENCHMARK(BM_ToLowerLevel)->Arg(1)->Arg(2)->Arg(20);

}  // namespace
}  // namespace arolla::expr
