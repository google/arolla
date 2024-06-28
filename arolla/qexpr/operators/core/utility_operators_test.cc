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
#include "arolla/qexpr/operators/core/utility_operators.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

using ::testing::Eq;

TEST(UtilityOperatorsTest, Identity) {
  auto i32 = GetQType<int>();
  auto copy_op = MakeCopyOp(i32);
  ASSERT_EQ(copy_op->signature(), QExprOperatorSignature::Get({i32}, i32));

  FrameLayout::Builder layout_builder;
  auto i0_slot = layout_builder.AddSlot<int>();
  auto i1_slot = layout_builder.AddSlot<int>();
  ASSERT_OK_AND_ASSIGN(
      auto copy_bound_op0,  // bind to new specified slots
      copy_op->Bind(ToTypedSlots(i0_slot), TypedSlot::FromSlot(i1_slot)));

  auto memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext root_ctx(&memory_layout);
  EvaluationContext ctx(root_ctx);
  root_ctx.Set(i0_slot, 7);
  copy_bound_op0->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  EXPECT_THAT(root_ctx.Get(i1_slot), Eq(7));
}

TEST(UtilityOperatorsTest, MakeTuple) {
  auto i32 = GetQType<int>();
  auto f64 = GetQType<double>();
  auto tuple_qtype = MakeTupleQType({i32, f64});
  ASSERT_OK_AND_ASSIGN(auto copy_op,
                       OperatorRegistry::GetInstance()->LookupOperator(
                           "core.make_tuple", {i32, f64}, tuple_qtype));
  ASSERT_EQ(copy_op->signature(),
            QExprOperatorSignature::Get({i32, f64}, tuple_qtype));

  FrameLayout::Builder layout_builder;
  auto tuple0_slot = AddSlot(tuple_qtype, &layout_builder);
  ASSERT_EQ(tuple0_slot.SubSlotCount(), 2);
  ASSERT_OK_AND_ASSIGN(auto i0_slot, tuple0_slot.SubSlot(0).ToSlot<int>());
  ASSERT_OK_AND_ASSIGN(auto d0_slot, tuple0_slot.SubSlot(1).ToSlot<double>());

  auto tuple1_slot = AddSlot(tuple_qtype, &layout_builder);
  ASSERT_EQ(tuple1_slot.SubSlotCount(), 2);
  ASSERT_OK_AND_ASSIGN(auto i1_slot, tuple1_slot.SubSlot(0).ToSlot<int>());
  ASSERT_OK_AND_ASSIGN(auto d1_slot, tuple1_slot.SubSlot(1).ToSlot<double>());

  ASSERT_OK_AND_ASSIGN(
      auto copy_bound_op,  // bind to new specified slots
      copy_op->Bind(ToTypedSlots(i0_slot, d0_slot), {tuple1_slot}));

  auto memory_layout = std::move(layout_builder).Build();
  RootEvaluationContext root_ctx(&memory_layout);
  EvaluationContext ctx(root_ctx);
  root_ctx.Set(i0_slot, 7);
  root_ctx.Set(d0_slot, 4.5);
  copy_bound_op->Run(&ctx, root_ctx.frame());
  EXPECT_OK(ctx.status());
  EXPECT_THAT(root_ctx.Get(i1_slot), Eq(7));
  EXPECT_THAT(root_ctx.Get(d1_slot), Eq(4.5));
}

}  // namespace
}  // namespace arolla
