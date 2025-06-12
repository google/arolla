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
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/typed_slot.h"
#include "py/arolla/codegen/testing/aggregation_dot_product_times_5.h"
#include "py/arolla/codegen/testing/two_long_fibonacci_chains_batch.h"
#include "py/arolla/codegen/testing/x_plus_y_times_5_batch.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;

TEST(CodegenBatchTest, TestCompiledXPlusYTimes5) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto y_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto z_slot = layout_builder.AddSlot<DenseArray<float>>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledXPlusYTimes5Batch().Bind(
                           &layout_builder,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)}},
                           TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  alloc.frame().Set(x_slot, CreateDenseArray<float>({1.0f, 2.0f}));
  alloc.frame().Set(y_slot, CreateDenseArray<float>({5.0f, 3.0f}));
  ASSERT_OK(executable->Execute(alloc.frame()));
  EXPECT_THAT(alloc.frame().Get(z_slot), ElementsAre(30.0f, 25.0f));
}

TEST(CodegenBatchTest, TestCompiledXPlusYTimes5WithFactory) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto y_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto z_slot = layout_builder.AddSlot<DenseArray<float>>();
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledXPlusYTimes5Batch().Bind(
                           &layout_builder,
                           {{"x", TypedSlot::FromSlot(x_slot)},
                            {"y", TypedSlot::FromSlot(y_slot)}},
                           TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  UnsafeArenaBufferFactory factory(128);
  EvaluationContext ctx({.buffer_factory = &factory});
  executable->InitializeLiterals(&ctx, alloc.frame());
  ASSERT_OK(ctx.status());

  // Actual evaluation
  alloc.frame().Set(x_slot, CreateDenseArray<float>({1.0f, 2.0f}));
  alloc.frame().Set(y_slot, CreateDenseArray<float>({5.0f, 3.0f}));
  executable->Execute(&ctx, alloc.frame());
  ASSERT_OK(ctx.status());
  EXPECT_THAT(alloc.frame().Get(z_slot), ElementsAre(30.0f, 25.0f));
  EXPECT_FALSE(alloc.frame().Get(z_slot).is_owned());  // allocated in arena
}

TEST(CodegenBatchTest, TestCompiledTwoFibonacciChains) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto y_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto z_slot = layout_builder.AddSlot<DenseArray<float>>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledTwoFibonacciChainsBatch().Bind(
          &layout_builder,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)}},
          TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  alloc.frame().Set(x_slot, CreateDenseArray<float>({1.0f, 2.0f}));
  alloc.frame().Set(y_slot, CreateDenseArray<float>({5.0f, 3.0f}));
  ASSERT_OK(executable->Execute(alloc.frame()));
  EXPECT_THAT(alloc.frame().Get(z_slot), ElementsAre(0.0f, 0.0f));
}

TEST(CodegenBatchTest, TestCompiledAggregationDotProductTimes5) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto y_slot = layout_builder.AddSlot<DenseArray<float>>();
  auto z_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledAggregationDotProductTimes5().Bind(
          &layout_builder,
          {{"x", TypedSlot::FromSlot(x_slot)},
           {"y", TypedSlot::FromSlot(y_slot)}},
          TypedSlot::FromSlot(z_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  alloc.frame().Set(x_slot, CreateDenseArray<float>({3.0f, 2.0f}));
  alloc.frame().Set(y_slot, CreateDenseArray<float>({5.0f, 3.0f}));
  ASSERT_OK(executable->Execute(alloc.frame()));
  EXPECT_THAT(alloc.frame().Get(z_slot),
              Eq(static_cast<float>((3 * 5 + 2 * 3) * 5)));
}

}  // namespace

}  // namespace arolla
