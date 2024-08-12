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
#include "arolla/qexpr/simple_executable.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::testing::Eq;

std::unique_ptr<BoundExpr> CreateCountingBoundExpr(
    FrameLayout::Slot<int> init_counter, FrameLayout::Slot<int> exec_counter) {
  std::vector<std::unique_ptr<BoundOperator>> init_ops;
  init_ops.push_back(MakeBoundOperator([=](EvaluationContext*, FramePtr frame) {
    ++(*frame.GetMutable(init_counter));
  }));
  std::vector<std::unique_ptr<BoundOperator>> exec_ops;
  exec_ops.push_back(MakeBoundOperator([=](EvaluationContext*, FramePtr frame) {
    ++(*frame.GetMutable(exec_counter));
  }));
  return std::make_unique<SimpleBoundExpr>(
      /*input_slots=*/absl::flat_hash_map<std::string, TypedSlot>{},
      /*output_slot=*/TypedSlot::UnsafeFromOffset(GetQType<Unit>(), 0),
      std::move(init_ops), std::move(exec_ops));
}

TEST(SimpleExecutableTest, CombinedBoundExpr) {
  FrameLayout::Builder builder;
  std::vector<std::unique_ptr<BoundExpr>> subexprs;
  auto init_1_called = builder.AddSlot<int>();
  auto exec_1_called = builder.AddSlot<int>();
  subexprs.push_back(CreateCountingBoundExpr(init_1_called, exec_1_called));

  auto init_2_called = builder.AddSlot<int>();
  auto exec_2_called = builder.AddSlot<int>();
  subexprs.push_back(CreateCountingBoundExpr(init_2_called, exec_2_called));

  std::unique_ptr<BoundExpr> combined_expr =
      std::make_unique<CombinedBoundExpr>(
          /*input_slots=*/absl::flat_hash_map<std::string, TypedSlot>{},
          /*output_slot=*/TypedSlot::UnsafeFromOffset(GetQType<Unit>(), 0),
          /*named_output_slots=*/
          absl::flat_hash_map<std::string, TypedSlot>{}, std::move(subexprs));

  FrameLayout layout = std::move(builder).Build();
  RootEvaluationContext ctx(&layout);
  ctx.Set(init_1_called, 0);
  ctx.Set(init_2_called, 0);
  ctx.Set(exec_1_called, 0);
  ctx.Set(exec_2_called, 0);
  ASSERT_THAT(combined_expr->InitializeLiterals(&ctx), IsOk());
  EXPECT_THAT(ctx.Get(init_1_called), Eq(1));
  EXPECT_THAT(ctx.Get(init_2_called), Eq(1));
  EXPECT_THAT(ctx.Get(exec_1_called), Eq(0));
  EXPECT_THAT(ctx.Get(exec_2_called), Eq(0));

  ASSERT_THAT(combined_expr->Execute(&ctx), IsOk());
  EXPECT_THAT(ctx.Get(init_1_called), Eq(1));
  EXPECT_THAT(ctx.Get(init_2_called), Eq(1));
  EXPECT_THAT(ctx.Get(exec_1_called), Eq(1));
  EXPECT_THAT(ctx.Get(exec_2_called), Eq(1));
}

}  // namespace
}  // namespace arolla
