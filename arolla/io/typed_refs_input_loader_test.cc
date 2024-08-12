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
#include "arolla/io/typed_refs_input_loader.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::arolla::testing::InputLoaderSupports;
using ::testing::Eq;

TEST(TupleInputLoaderTest, Scalars) {
  using Input = absl::Span<const TypedRef>;
  std::unique_ptr<InputLoader<Input>> input_loader = CreateTypedRefsInputLoader(
      {{"a", GetQType<float>()}, {"b", GetQType<int>()}});

  EXPECT_THAT(input_loader, InputLoaderSupports({{"a", GetQType<float>()},
                                                 {"b", GetQType<int>()}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<float>();
  auto b_slot = layout_builder.AddSlot<int>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  TypedValue tv_a = TypedValue::FromValue<float>(5);
  TypedValue tv_b = TypedValue::FromValue<int>(7);
  ASSERT_THAT(bound_input_loader({tv_a.AsRef(), tv_b.AsRef()}, alloc.frame()),
              IsOk());
  EXPECT_THAT(alloc.frame().Get(a_slot), Eq(5));
  EXPECT_THAT(alloc.frame().Get(b_slot), Eq(7));

  // Test with not all the slots bound.
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_b_input_loader,
                       input_loader->Bind({
                           {"b", TypedSlot::FromSlot(b_slot)},
                       }));
  alloc.frame().Set(a_slot, 42);
  alloc.frame().Set(b_slot, 57);
  ASSERT_THAT(bound_b_input_loader({tv_a.AsRef(), tv_b.AsRef()}, alloc.frame()),
              IsOk());
  EXPECT_THAT(alloc.frame().Get(a_slot), Eq(42));  // unchanged
  EXPECT_THAT(alloc.frame().Get(b_slot), Eq(7));
}

}  // namespace
}  // namespace arolla
