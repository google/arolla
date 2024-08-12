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
#include <cstddef>
#include <cstdint>
#include <utility>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operators/while_loop/while_loop.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::TypedValueWith;
using ::testing::ElementsAre;
using ::testing::Eq;

class WhileOperatorTest
    : public ::testing::TestWithParam<DynamicEvaluationEngineOptions> {
 protected:
  void SetUp() override { InitArolla(); }

  DynamicEvaluationEngineOptions GetOptions() const { return GetParam(); }
};

INSTANTIATE_TEST_SUITE_P(
    GarbageCollection, WhileOperatorTest,
    ::testing::Values(
        DynamicEvaluationEngineOptions{.allow_overriding_input_slots = false},
        DynamicEvaluationEngineOptions{.allow_overriding_input_slots = true}));

TEST_P(WhileOperatorTest, SimpleWhile) {
  auto init_x = Leaf("x");
  auto init_y = Leaf("y");

  ASSERT_OK_AND_ASSIGN(
      auto loop_condition,
      CallOp("core.not_equal", {Placeholder("y"), Literal<int64_t>(0)}));

  auto new_x = Placeholder("y");
  ASSERT_OK_AND_ASSIGN(
      auto new_y, CallOp("math.mod", {Placeholder("x"), Placeholder("y")}));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr while_loop,
                       expr_operators::MakeWhileLoop(
                           {{"x", init_x}, {"y", init_y}}, loop_condition,
                           {{"x", new_x}, {"y", new_y}}));
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr gcd,
      CallOp("namedtuple.get_field", {while_loop, Literal(Text("x"))}));

  EXPECT_THAT(Invoke(gcd,
                     {{"x", TypedValue::FromValue<int64_t>(57)},
                      {"y", TypedValue::FromValue<int64_t>(58)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int64_t>(Eq(1))));
  EXPECT_THAT(Invoke(gcd,
                     {{"x", TypedValue::FromValue<int64_t>(171)},
                      {"y", TypedValue::FromValue<int64_t>(285)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int64_t>(Eq(57))));
}

// The function creates an expression with loop adding L.x to accumulator
// number_of_xs - 1 times.
absl::StatusOr<ExprNodePtr> SumOfXs(int64_t number_of_xs) {
  auto init_n = Literal<int64_t>(1);
  auto init_x = Leaf("x");
  auto init_accumulator = Leaf("x");

  ASSIGN_OR_RETURN(auto loop_condition,
                   CallOp("core.not_equal",
                          {Placeholder("n"), Literal<int64_t>(number_of_xs)}));
  ASSIGN_OR_RETURN(auto new_n,
                   CallOp("math.add", {Placeholder("n"), Literal<int64_t>(1)}));
  ASSIGN_OR_RETURN(
      auto new_accumulator,
      CallOp("math.add", {Placeholder("accumulator"), Placeholder("x")}));
  return CallOp(
      "namedtuple.get_field",
      {expr_operators::MakeWhileLoop(
           {{"n", init_n}, {"x", init_x}, {"accumulator", init_accumulator}},
           loop_condition, {{"n", new_n}, {"accumulator", new_accumulator}}),
       Literal(Text("accumulator"))});
}

TEST_P(WhileOperatorTest, LoopWithDifferentNumberOfIterations) {
  for (int64_t iterations = 0; iterations < 10; ++iterations) {
    ASSERT_OK_AND_ASSIGN(ExprNodePtr sum, SumOfXs(iterations + 1));
    EXPECT_THAT(
        Invoke(sum, {{"x", TypedValue::FromValue<int64_t>(57)}}, GetOptions()),
        IsOkAndHolds(TypedValueWith<int64_t>(57 * (iterations + 1))));
  }
}

TEST_P(WhileOperatorTest, LoopWithDenseArray) {
  ASSERT_OK_AND_ASSIGN(ExprNodePtr sum_of_1000, SumOfXs(1000));

  EXPECT_THAT(Invoke(sum_of_1000, {{"x", TypedValue::FromValue<int64_t>(1)}},
                     GetOptions()),
              IsOkAndHolds(TypedValueWith<int64_t>(1000)));
  EXPECT_THAT(
      Invoke(
          sum_of_1000,
          {{"x", TypedValue::FromValue(CreateDenseArray<int64_t>({0, 1, 2}))}},
          GetOptions()),
      IsOkAndHolds(
          TypedValueWith<DenseArray<int64_t>>(ElementsAre(0, 1000, 2000))));

  // Add an intermediate node that can be garbage collected.
  auto init_x = Leaf("x");
  ASSERT_OK_AND_ASSIGN(
      ExprNodePtr sum_of_1000_1000,
      SubstituteByFingerprint(sum_of_1000,
                              {{init_x->fingerprint(), sum_of_1000}}));
  EXPECT_THAT(
      Invoke(
          sum_of_1000_1000,
          {{"x", TypedValue::FromValue(CreateDenseArray<int64_t>({0, 1, 2}))}},
          GetOptions()),
      IsOkAndHolds(TypedValueWith<DenseArray<int64_t>>(
          ElementsAre(0, 1000000, 2000000))));
}

template <typename T>
void BM_WhileOperator(benchmark::State& state, T initial_value) {
  InitArolla();
  auto sum_of_1000_x = SumOfXs(1000).value();
  FrameLayout::Builder builder;
  auto x_slot = builder.AddSlot<T>();
  auto sum_of_1000_x_expr =
      CompileAndBindForDynamicEvaluation(DynamicEvaluationEngineOptions(),
                                         &builder, sum_of_1000_x,
                                         {{"x", TypedSlot::FromSlot(x_slot)}})
          .value();
  FrameLayout layout = std::move(builder).Build();
  RootEvaluationContext ctx(&layout);
  CHECK_OK(sum_of_1000_x_expr->InitializeLiterals(&ctx));
  for (auto _ : state) {
    CHECK_OK(sum_of_1000_x_expr->Execute(&ctx));
  }
}

void BM_WhileOperator_Scalar(benchmark::State& state) {
  BM_WhileOperator(state, int64_t{57});
}
BENCHMARK(BM_WhileOperator_Scalar);

void BM_WhileOperator_DenseArray(benchmark::State& state) {
  constexpr size_t kArraySize = 100;
  BM_WhileOperator(state, CreateConstDenseArray<int64_t>(kArraySize, 57));
}
BENCHMARK(BM_WhileOperator_DenseArray);

}  // namespace
}  // namespace arolla::expr::eval_internal
