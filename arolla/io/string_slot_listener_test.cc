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
#include "arolla/io/string_slot_listener.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;

TEST(StringSlotListenerTest, BytesSlotListener) {
  ASSERT_OK_AND_ASSIGN(auto slot_listener, BytesSlotListener("debug_html"));
  EXPECT_THAT(slot_listener->GetQTypeOf("debug_html"),
              Eq(GetOptionalQType<Bytes>()));

  FrameLayout::Builder layout_builder;
  auto bytes_slot = layout_builder.AddSlot<OptionalValue<Bytes>>();
  ASSERT_OK_AND_ASSIGN(BoundSlotListener<std::string> bound_slot_listener,
                       slot_listener->Bind({
                           {"debug_html", TypedSlot::FromSlot(bytes_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  std::string side_output;
  ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
  EXPECT_THAT(side_output, Eq(""));

  alloc.frame().Set(bytes_slot, Bytes{"fifty seven"});
  ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
  EXPECT_THAT(side_output, Eq("fifty seven"));
}

TEST(StringSlotListenerTest, BytesArraySlotListener) {
  ASSERT_OK_AND_ASSIGN(auto slot_listener,
                       BytesArraySlotListener("debug_htmls"));
  EXPECT_THAT(slot_listener->GetQTypeOf("debug_htmls"),
              Eq(GetDenseArrayQType<Bytes>()));

  FrameLayout::Builder layout_builder;
  auto bytes_array_slot = layout_builder.AddSlot<DenseArray<Bytes>>();
  ASSERT_OK_AND_ASSIGN(
      BoundSlotListener<std::vector<std::string>> bound_slot_listener,
      slot_listener->Bind({
          {"debug_htmls", TypedSlot::FromSlot(bytes_array_slot)},
      }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  std::vector<std::string> side_output;
  ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
  EXPECT_THAT(side_output, IsEmpty());

  alloc.frame().Set(bytes_array_slot,
                    CreateDenseArray<Bytes>({Bytes("fifty"), Bytes(""),
                                             Bytes("seven"), std::nullopt}));
  ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
  EXPECT_THAT(side_output, ElementsAre("fifty", "", "seven", ""));
}

}  // namespace
}  // namespace arolla
