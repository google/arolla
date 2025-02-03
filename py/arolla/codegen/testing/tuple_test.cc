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
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "py/arolla/codegen/testing/make_tuple.h"
#include "py/arolla/codegen/testing/reduce_tuple.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;

TEST(CodegenScalarTest, ReduceTupleDivision) {
  FrameLayout::Builder layout_builder;
  auto w_slot = layout_builder.AddSlot<float>();
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<float>();
  auto z_slot = layout_builder.AddSlot<float>();
  auto out_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<BoundExpr> executable,
      ::test_namespace::GetCompiledWXYZDivisionViaReduceTuple().Bind(
          &layout_builder,
          {
              {"w", TypedSlot::FromSlot(w_slot)},
              {"x", TypedSlot::FromSlot(x_slot)},
              {"y", TypedSlot::FromSlot(y_slot)},
              {"z", TypedSlot::FromSlot(z_slot)},
          },
          TypedSlot::FromSlot(out_slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  alloc.frame().Set(w_slot, 60.);
  alloc.frame().Set(x_slot, 2.);
  alloc.frame().Set(y_slot, 3.);
  alloc.frame().Set(z_slot, 5.);
  ASSERT_OK(executable->Execute(alloc.frame()));
  // 60 / 2 / 3 / 5 == 2
  EXPECT_EQ(alloc.frame().Get(out_slot), 2.);
}

TEST(CodegenScalarTest, MakeEmptyTuple) {
  FrameLayout::Builder layout_builder;
  QTypePtr tuple_qtype = MakeTupleQType({});
  auto out_slot = layout_builder.AddSubFrame(tuple_qtype->type_layout());
  auto out_typed_slot =
      TypedSlot::UnsafeFromOffset(tuple_qtype, out_slot.byte_offset());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledEmptyTuple().Bind(
                           &layout_builder, {}, out_typed_slot));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  ASSERT_OK(executable->Execute(alloc.frame()));
  auto typed_value = TypedValue::FromSlot(out_typed_slot, alloc.frame());
  EXPECT_EQ(typed_value.GetType(), tuple_qtype);
}

TEST(CodegenScalarTest, MakeLiteralTuple) {
  FrameLayout::Builder layout_builder;
  QTypePtr tuple_qtype =
      MakeTupleQType({GetQType<int>(), GetQType<float>(), GetQType<Bytes>()});
  auto out_slot = layout_builder.AddSubFrame(tuple_qtype->type_layout());
  auto out_typed_slot =
      TypedSlot::UnsafeFromOffset(tuple_qtype, out_slot.byte_offset());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledLiteralTuple().Bind(
                           &layout_builder, {}, out_typed_slot));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  ASSERT_OK(executable->Execute(alloc.frame()));
  auto typed_value = TypedValue::FromSlot(out_typed_slot, alloc.frame());
  EXPECT_EQ(typed_value.GetType(), tuple_qtype);
  EXPECT_EQ(alloc.frame().Get(out_typed_slot.SubSlot(0).UnsafeToSlot<int>()),
            1);
  EXPECT_EQ(alloc.frame().Get(out_typed_slot.SubSlot(1).UnsafeToSlot<float>()),
            2.0);
  EXPECT_EQ(alloc.frame().Get(out_typed_slot.SubSlot(2).UnsafeToSlot<Bytes>()),
            Bytes("3"));
}

TEST(CodegenScalarTest, MakeFlatTuple) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto z_slot = layout_builder.AddSlot<Bytes>();
  QTypePtr tuple_qtype = MakeTupleQType(
      {GetQType<float>(), GetOptionalQType<int>(), GetQType<Bytes>()});
  auto out_slot = layout_builder.AddSubFrame(tuple_qtype->type_layout());
  auto out_typed_slot =
      TypedSlot::UnsafeFromOffset(tuple_qtype, out_slot.byte_offset());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledMakeFlatTuple().Bind(
                           &layout_builder,
                           {
                               {"x", TypedSlot::FromSlot(x_slot)},
                               {"y", TypedSlot::FromSlot(y_slot)},
                               {"z", TypedSlot::FromSlot(z_slot)},
                           },
                           out_typed_slot));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  alloc.frame().Set(x_slot, 2.);
  alloc.frame().Set(y_slot, 3);
  alloc.frame().Set(z_slot, "5");
  ASSERT_OK(executable->Execute(alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(out_typed_slot.SubSlot(0).UnsafeToSlot<float>()),
            2.);
  EXPECT_EQ(alloc.frame().Get(
                out_typed_slot.SubSlot(1).UnsafeToSlot<OptionalValue<int>>()),
            3);
  EXPECT_EQ(alloc.frame().Get(out_typed_slot.SubSlot(2).UnsafeToSlot<Bytes>()),
            Bytes("5"));
}

TEST(CodegenScalarTest, MakeNestedTuple) {
  FrameLayout::Builder layout_builder;
  auto x_slot = layout_builder.AddSlot<float>();
  auto y_slot = layout_builder.AddSlot<int>();
  QTypePtr tuple_qtype = MakeTupleQType({GetQType<float>(), GetQType<int>()});
  constexpr int kDepth = 10;
  for (int i = 0; i < kDepth; ++i) {
    tuple_qtype =
        MakeTupleQType({GetQType<float>(), GetQType<int>(), tuple_qtype});
  }
  auto out_slot = layout_builder.AddSubFrame(tuple_qtype->type_layout());
  auto out_typed_slot =
      TypedSlot::UnsafeFromOffset(tuple_qtype, out_slot.byte_offset());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<BoundExpr> executable,
                       ::test_namespace::GetCompiledNestedTuple().Bind(
                           &layout_builder,
                           {
                               {"x", TypedSlot::FromSlot(x_slot)},
                               {"y", TypedSlot::FromSlot(y_slot)},
                           },
                           out_typed_slot));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  ASSERT_OK(executable->InitializeLiterals(alloc.frame()));

  // Actual evaluation
  alloc.frame().Set(x_slot, 200.f);
  alloc.frame().Set(y_slot, 300);
  ASSERT_OK(executable->Execute(alloc.frame()));
  auto typed_value = TypedValue::FromSlot(out_typed_slot, alloc.frame());
  auto value = typed_value.AsRef();
  ASSERT_EQ(typed_value.GetType(), tuple_qtype);
  for (int i = kDepth; i >= 0; --i) {
    EXPECT_THAT(value.GetField(0).As<float>(), IsOkAndHolds(200.f + i)) << i;
    EXPECT_THAT(value.GetField(1).As<int>(), IsOkAndHolds(300 + i)) << i;
    EXPECT_EQ(value.GetFieldCount(), 2 + (i == 0 ? 0 : 1)) << i;
    if (i != 0) {
      value = value.GetField(2);
    }
  }
}

}  // namespace
}  // namespace arolla
