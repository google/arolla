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
#include "arolla/io/span_input_loader.h"

#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::arolla::testing::InputLoaderSupports;
using ::testing::Eq;

TEST(SpanInputLoaderTest, Scalars) {
  std::unique_ptr<InputLoader<absl::Span<const float>>> input_loader =
      SpanInputLoader<float>::Create({"a", "b"});

  EXPECT_THAT(input_loader, InputLoaderSupports({{"a", GetQType<float>()},
                                                 {"b", GetQType<float>()}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<float>();
  auto b_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(
      BoundInputLoader<absl::Span<const float>> bound_input_loader,
      input_loader->Bind({
          {"a", TypedSlot::FromSlot(a_slot)},
          {"b", TypedSlot::FromSlot(b_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_THAT(bound_input_loader({5, 7}, alloc.frame()), IsOk());
  EXPECT_THAT(alloc.frame().Get(a_slot), Eq(5));
  EXPECT_THAT(alloc.frame().Get(b_slot), Eq(7));

  EXPECT_THAT(bound_input_loader({5, 7, 9}, alloc.frame()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unexpected input count: expected 2, got 3"));

  // Test with not all the slots bound.
  ASSERT_OK_AND_ASSIGN(
      BoundInputLoader<absl::Span<const float>> bound_b_input_loader,
      input_loader->Bind({
          {"b", TypedSlot::FromSlot(b_slot)},
      }));
  ASSERT_THAT(bound_b_input_loader({2, 57}, alloc.frame()), IsOk());
  EXPECT_THAT(alloc.frame().Get(a_slot), Eq(5));  // unchanged
  EXPECT_THAT(alloc.frame().Get(b_slot), Eq(57));
}

TEST(SpanInputLoaderTest, Optionals) {
  std::unique_ptr<InputLoader<absl::Span<const std::optional<float>>>>
      input_loader = SpanInputLoader<std::optional<float>>::Create({"a", "b"});

  EXPECT_THAT(input_loader,
              InputLoaderSupports({{"a", GetOptionalQType<float>()},
                                   {"b", GetOptionalQType<float>()}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto b_slot = layout_builder.AddSlot<OptionalValue<float>>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<absl::Span<const std::optional<float>>>
                           bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_THAT(bound_input_loader({5, std::nullopt}, alloc.frame()), IsOk());
  EXPECT_THAT(alloc.frame().Get(a_slot), Eq(5.f));
  EXPECT_THAT(alloc.frame().Get(b_slot), Eq(std::nullopt));
}

}  // namespace
}  // namespace arolla
