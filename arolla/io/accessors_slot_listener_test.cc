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
#include "arolla/io/accessors_slot_listener.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;

struct TestStruct {
  int a;
  double b;
};

TEST(AccessorsSlotListenerTest, NormalAccessors) {
  ASSERT_OK_AND_ASSIGN(auto slot_listener,
                       CreateAccessorsSlotListener<TestStruct>(
                           "a", [](int a, TestStruct* s) { s->a = a; },  //
                           "b", [](double b, TestStruct* s) { s->b = b; }));
  EXPECT_THAT(slot_listener->GetQTypeOf("a"), Eq(GetQType<int32_t>()));
  EXPECT_THAT(slot_listener->GetQTypeOf("b"), Eq(GetQType<double>()));
  EXPECT_THAT(slot_listener->SuggestAvailableNames(), ElementsAre("a", "b"));

  {  // bind all
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<double>();
    auto a_slot = layout_builder.AddSlot<int>();
    layout_builder.AddSlot<char>();
    auto b_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<std::string>();
    ASSERT_OK_AND_ASSIGN(BoundSlotListener<TestStruct> bound_slot_listener,
                         slot_listener->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                             {"b", TypedSlot::FromSlot(b_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    alloc.frame().Set(a_slot, 5);
    alloc.frame().Set(b_slot, 3.5);

    TestStruct side_output{0, 0};
    ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
    EXPECT_THAT(side_output.a, Eq(5));
    EXPECT_THAT(side_output.b, Eq(3.5));
  }
  {  // bind only a
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<double>();
    auto a_slot = layout_builder.AddSlot<int>();
    layout_builder.AddSlot<char>();
    ASSERT_OK_AND_ASSIGN(BoundSlotListener<TestStruct> bound_slot_listener,
                         slot_listener->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    alloc.frame().Set(a_slot, 5);

    TestStruct side_output{0, 0};
    ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
    EXPECT_THAT(side_output.a, Eq(5));
    EXPECT_THAT(side_output.b, Eq(0));
  }
  {  // bind only b
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<double>();
    auto b_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<std::string>();
    ASSERT_OK_AND_ASSIGN(BoundSlotListener<TestStruct> bound_slot_listener,
                         slot_listener->Bind({
                             {"b", TypedSlot::FromSlot(b_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    alloc.frame().Set(b_slot, 3.5);
    TestStruct side_output{0, 0};
    ASSERT_OK(bound_slot_listener(alloc.frame(), &side_output));
    EXPECT_THAT(side_output.a, Eq(0));
    EXPECT_THAT(side_output.b, Eq(3.5));
  }
  {  // partial bind with an unused slot
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<double>();
    auto b_slot = layout_builder.AddSlot<double>();
    auto c_slot = layout_builder.AddSlot<int>();
    layout_builder.AddSlot<std::string>();
    ASSERT_OK_AND_ASSIGN(auto bound_slot_listener,
                         slot_listener->PartialBind({
                             {"b", TypedSlot::FromSlot(b_slot)},
                             {"c", TypedSlot::FromSlot(c_slot)},
                         }));
    ASSERT_TRUE(bound_slot_listener.has_value());

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    alloc.frame().Set(b_slot, 3.5);
    TestStruct side_output{0, 0};
    ASSERT_OK((*bound_slot_listener)(alloc.frame(), &side_output));
    EXPECT_THAT(side_output.a, Eq(0));
    EXPECT_THAT(side_output.b, Eq(3.5));
  }
  {  // empty partial bind
    FrameLayout::Builder layout_builder;
    auto c_slot = layout_builder.AddSlot<int>();
    layout_builder.AddSlot<std::string>();
    ASSERT_OK_AND_ASSIGN(auto bound_slot_listener,
                         slot_listener->PartialBind({
                             {"c", TypedSlot::FromSlot(c_slot)},
                         }));
    EXPECT_FALSE(bound_slot_listener.has_value());
  }
}

TEST(InputLoaderTest, NameDuplicates) {
  EXPECT_THAT(CreateAccessorsSlotListener<TestStruct>(
                  "a", [](int a, TestStruct* s) { s->a = a; },     //
                  "c", [](double b, TestStruct* s) { s->b = b; },  //
                  "b", [](double b, TestStruct* s) { s->b = b; },  //
                  "c", [](double b, TestStruct* s) { s->b = b; },  //
                  "a", [](int a, TestStruct* s) { s->a = a; }),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("accessors have duplicated names: a, c")));
}

TEST(InputLoaderTest, Errors) {
  ASSERT_OK_AND_ASSIGN(auto slot_listener,
                       CreateAccessorsSlotListener<TestStruct>(
                           "a", [](int a, TestStruct* s) { s->a = a; }));
  {  // wrong type
    FrameLayout::Builder layout_builder;
    auto dslot = layout_builder.AddSlot<double>();
    EXPECT_THAT(
        slot_listener->Bind({
            {"a", TypedSlot::FromSlot(dslot)},
        }),
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            HasSubstr("types mismatch: a{expected:INT32, actual:FLOAT64}")));
  }
}

using PairStringFunction =
    std::pair<std::string, std::function<void(int, int*)>>;
template <size_t>
using PairStringFunctionByInt = PairStringFunction;

template <size_t... Is>
absl::StatusOr<std::unique_ptr<SlotListener<int>>>
CreateAccessorsSlotListenerManyInputs(std::index_sequence<Is...>) {
  using T = std::tuple<PairStringFunctionByInt<Is>...>;
  return CreateAccessorsSlotListenerFromTuple<int>(T{PairStringFunction(
      absl::StrCat(Is), [](int, int* out) { *out = Is; })...});
}

TEST(AccessorsSlotListenerTest,
     CreateAccessorsSlotListenerCompilationStressTest) {
  // As at 06.04.2021 the test passes with N=500 in 24 seconds, but fails with
  // N=1000 due to "recursive template instantiation exceeded maximum depth of
  // 1024" error. Keeping N=50 to have reasonable compilation time (~3 seconds).
  constexpr size_t N = 50;
  ASSERT_OK_AND_ASSIGN(auto loader, CreateAccessorsSlotListenerManyInputs(
                                        std::make_index_sequence<N>()));
  EXPECT_THAT(loader->SuggestAvailableNames().size(), Eq(N));
}

}  // namespace
}  // namespace arolla
