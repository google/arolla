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
#include "arolla/memory/frame.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/dynamic_annotations.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/util/demangle.h"
#include "arolla/util/is_bzero_constructible.h"
#include "arolla/util/memory.h"
#include "arolla/util/status_macros_backport.h"  // IWYU pragma: keep

namespace arolla::testing {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;

struct SimpleStruct {
  int a;
  float b;
};

struct InitializedStruct {
  // Initializing any field of the struct creates an implicit constructor,
  // which makes this struct non-trivial.
  int a = 1;
  float b = 2.0;
};

TEST(FrameLayoutTest, SlotOutput) {
  FrameLayout::Builder builder;
  auto slot = builder.AddSlot<int>();
  std::ostringstream ss;
  ss << slot;
  EXPECT_EQ(ss.str(), std::string("Slot<") + TypeName<int>() + ">(0)");
}

TEST(FrameLayoutTest, SimpleFields) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<int>();
  auto slot2 = builder.AddSlot<float>();
  auto slot3 = builder.AddSlot<double>();
  auto layout = std::move(builder).Build();

  // Create and initialize memory.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(frame.Get(slot1), Eq(0));
  EXPECT_THAT(frame.Get(slot2), Eq(0.0f));
  EXPECT_THAT(frame.Get(slot3), Eq(0.0));

  frame.Set(slot1, 1);
  frame.Set(slot2, 2.0f);
  frame.Set(slot3, M_PI);

  EXPECT_THAT(frame.Get(slot1), Eq(1));
  EXPECT_THAT(frame.Get(slot2), Eq(2.0f));
  EXPECT_THAT(frame.Get(slot3), Eq(M_PI));
}

TEST(FrameLayoutTest, SimpleArrays) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<std::array<int, 4>>();
  auto slot2 = builder.AddSlot<std::array<float, 4>>();
  auto slot3 = builder.AddSlot<std::array<char, 4>>();
  auto layout = std::move(builder).Build();

  // Create a new evaluation context using the layout.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(frame.Get(slot1), ElementsAre(0, 0, 0, 0));
  EXPECT_THAT(frame.Get(slot2), ElementsAre(0.0f, 0.0f, 0.0f, 0.0f));
  EXPECT_THAT(frame.Get(slot3), ElementsAre(0, 0, 0, 0));

  frame.Set(slot1, std::array<int, 4>{1, 2, 3, 4});
  frame.Set(slot2, std::array<float, 4>{1.0f, 2.0f, 3.0f, 4.0f});
  frame.Set(slot3, std::array<char, 4>{'a', 'b', 'c', 'd'});

  EXPECT_THAT(frame.Get(slot1), ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(frame.Get(slot2), ElementsAre(1.0f, 2.0f, 3.0f, 4.0f));
  EXPECT_THAT(frame.Get(slot3), ElementsAre('a', 'b', 'c', 'd'));
}

TEST(FrameLayoutTest, SimplePointers) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<int*>();
  auto slot2 = builder.AddSlot<char*>();
  auto layout = std::move(builder).Build();

  // Create a new evaluation context using the layout.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(frame.Get(slot1), Eq(nullptr));
  EXPECT_THAT(frame.Get(slot2), Eq(nullptr));

  int int_values[] = {1, 2, 3, 4};
  char text[] = "It was a dark and stormy night.";

  frame.Set(slot1, int_values);
  frame.Set(slot2, text);

  EXPECT_THAT(frame.Get(slot1), Eq(int_values));
  EXPECT_THAT(frame.Get(slot2), Eq(text));
}

TEST(FrameLayoutTest, SmartPointers) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<std::unique_ptr<int>>();
  auto slot2 = builder.AddSlot<std::unique_ptr<std::string>>();
  auto layout = std::move(builder).Build();

  // Create a new evaluation context using the layout.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(frame.Get(slot1), Eq(nullptr));
  EXPECT_THAT(frame.Get(slot2), Eq(nullptr));

  frame.Set(slot1, std::make_unique<int>(12));
  frame.Set(slot2,
            std::make_unique<std::string>("It was a dark and stormy night."));

  EXPECT_THAT(*frame.Get(slot1), Eq(12));
  EXPECT_THAT(*frame.Get(slot2), Eq("It was a dark and stormy night."));
}

TEST(FrameLayoutTest, Vector) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<std::vector<int>>();
  auto slot2 = builder.AddSlot<std::vector<std::string>>();
  auto layout = std::move(builder).Build();

  // Create a new evaluation context using the layout.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(frame.Get(slot1), IsEmpty());
  EXPECT_THAT(frame.Get(slot2), IsEmpty());

  auto* int_vector = frame.GetMutable(slot1);
  int_vector->push_back(1);
  int_vector->push_back(2);
  int_vector->push_back(3);

  auto* string_vector = frame.GetMutable(slot2);
  string_vector->push_back("How");
  string_vector->push_back("now");
  string_vector->push_back("brown");
  string_vector->push_back("cow?");

  EXPECT_THAT(frame.Get(slot1), ElementsAre(1, 2, 3));
  EXPECT_THAT(frame.Get(slot2), ElementsAre("How", "now", "brown", "cow?"));
}

TEST(FrameLayoutTest, Structs) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<SimpleStruct>();
  auto slot2 = builder.AddSlot<InitializedStruct>();
  auto layout = std::move(builder).Build();

  // Create and initialize memory.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  // Verify s1 was zeroed.
  const SimpleStruct& s1 = frame.Get(slot1);
  EXPECT_THAT(s1.a, Eq(0));
  EXPECT_THAT(s1.b, Eq(0.0f));

  // Verify s2 was properly initialized.
  const InitializedStruct& s2 = frame.Get(slot2);
  EXPECT_THAT(s2.a, Eq(1));
  EXPECT_THAT(s2.b, Eq(2.0f));
}

TEST(FrameLayoutTest, AFewDifferentTypesWellInitialized) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<std::vector<int>>();
  auto slot2 = builder.AddSlot<std::vector<std::string>>();
  auto slot3 = builder.AddSlot<std::vector<int>>();
  auto slot4 = builder.AddSlot<SimpleStruct>();
  auto slot5 = builder.AddSlot<InitializedStruct>();
  auto slot6 = builder.AddSlot<std::vector<int>>();
  auto slot7 = builder.AddSlot<std::vector<std::string>>();
  auto slot8 = builder.AddSlot<std::vector<double>>();
  auto slot9 = builder.AddSlot<InitializedStruct>();
  auto layout = std::move(builder).Build();

  // Create a new evaluation context using the layout.
  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();

  EXPECT_THAT(frame.Get(slot1), IsEmpty());
  EXPECT_THAT(frame.Get(slot2), IsEmpty());
  EXPECT_THAT(frame.Get(slot3), IsEmpty());
  EXPECT_THAT(frame.Get(slot6), IsEmpty());
  EXPECT_THAT(frame.Get(slot7), IsEmpty());
  EXPECT_THAT(frame.Get(slot8), IsEmpty());

  // Verify SimpleStruct was zeroed.
  const SimpleStruct& simple = frame.Get(slot4);
  EXPECT_THAT(simple.a, Eq(0));
  EXPECT_THAT(simple.b, Eq(0.0f));

  // Verify InitializedStruct was properly initialized.
  for (const InitializedStruct& init : {frame.Get(slot5), frame.Get(slot9)}) {
    EXPECT_THAT(init.a, Eq(1));
    EXPECT_THAT(init.b, Eq(2.0f));
  }
}

TEST(FrameLayoutTest, HasField) {
  FrameLayout::Builder builder;
  auto slot1 = builder.AddSlot<int>();
  auto slot2 = builder.AddSlot<std::vector<int>>();
  auto slot3 = builder.AddSlot<SimpleStruct>();
  auto slot4 = builder.AddSlot<std::array<SimpleStruct, 4>>();
  auto slot5 = builder.AddSlot<InitializedStruct>();
  auto slot6 = builder.AddSlot<std::array<InitializedStruct, 4>>();
  auto layout = std::move(builder).Build();

  EXPECT_TRUE(layout.HasField(slot1.byte_offset(), typeid(int)));
  EXPECT_TRUE(layout.HasField(slot2.byte_offset(), typeid(std::vector<int>)));
  EXPECT_TRUE(layout.HasField(slot3.byte_offset(), typeid(SimpleStruct)));
  EXPECT_TRUE(layout.HasField(slot4.byte_offset(),
                              typeid(std::array<SimpleStruct, 4>)));
  EXPECT_TRUE(layout.HasField(slot5.byte_offset(), typeid(InitializedStruct)));
  EXPECT_TRUE(layout.HasField(slot6.byte_offset(),
                              typeid(std::array<InitializedStruct, 4>)));
}

TEST(FrameLayoutTest, RegisterUnsafeSlotWithEmptyField) {
  FrameLayout::Builder builder;
  ASSERT_TRUE(builder.RegisterUnsafeSlot(0, 0, typeid(std::monostate())).ok());
  auto layout = std::move(builder).Build();
  EXPECT_TRUE(layout.HasField(0, typeid(std::monostate())));
}

TEST(FrameLayoutTest, FieldDescriptorsRegisterUnsafe) {
  FrameLayout::Builder builder;
  auto slot = builder.AddSlot<int32_t>();
  auto slot_1part =
      FrameLayout::Slot<int16_t>::UnsafeSlotFromOffset(slot.byte_offset());
  auto slot_2part =
      FrameLayout::Slot<int16_t>::UnsafeSlotFromOffset(slot.byte_offset() + 2);
  ASSERT_THAT(builder.RegisterUnsafeSlot(slot_1part), IsOk());
  ASSERT_THAT(builder.RegisterUnsafeSlot(slot_2part), IsOk());
  ASSERT_THAT(builder.RegisterUnsafeSlot(slot.byte_offset() + 2, sizeof(int8_t),
                                         typeid(int8_t)),
              IsOk());
#ifndef NDEBUG
  EXPECT_THAT(builder.RegisterUnsafeSlot(slot_2part),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("slot is already registered")));
  EXPECT_THAT(builder.RegisterUnsafeSlot(slot_2part, /*allow_duplicates=*/true),
              IsOk());
#endif

  auto layout = std::move(builder).Build();

  EXPECT_TRUE(layout.HasField(slot.byte_offset(), typeid(int32_t)));
  EXPECT_TRUE(layout.HasField(slot.byte_offset(), typeid(int16_t)));
  EXPECT_TRUE(layout.HasField(slot.byte_offset() + 2, typeid(int16_t)));
  EXPECT_TRUE(layout.HasField(slot.byte_offset() + 2, typeid(int8_t)));
#ifndef NDEBUG
  EXPECT_FALSE(layout.HasField(slot.byte_offset() + 2, typeid(float)));
  EXPECT_FALSE(layout.HasField(slot.byte_offset() + 1, typeid(int8_t)));
#endif
}

TEST(FrameLayoutTest, FieldDescriptorsRegisterUnsafeErrors) {
  FrameLayout::Builder builder;
  auto slot = builder.AddSlot<int32_t>();
  auto slot_1part =
      FrameLayout::Slot<int16_t>::UnsafeSlotFromOffset(slot.byte_offset());
  auto slot_after_end =
      FrameLayout::Slot<int16_t>::UnsafeSlotFromOffset(slot.byte_offset() + 4);
  auto uninitialized_slot =
      FrameLayout::Slot<int16_t>::UnsafeUninitializedSlot();

  auto status = builder.RegisterUnsafeSlot(slot_1part);
  ASSERT_OK(status);

#ifndef NDEBUG
  status = builder.RegisterUnsafeSlot(slot);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_THAT(status.message(), HasSubstr("slot is already registered"));

  status = builder.RegisterUnsafeSlot(slot_1part);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_THAT(status.message(), HasSubstr("slot is already registered"));
#endif

  status = builder.RegisterUnsafeSlot(slot_after_end);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_THAT(status.message(),
              HasSubstr("unable to register slot after the end of alloc"));

  status = builder.RegisterUnsafeSlot(100, sizeof(int), typeid(int));
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_THAT(status.message(),
              HasSubstr("unable to register slot after the end of alloc, "
                        "offset: 100, size: 4, alloc size: 4"));

  status = builder.RegisterUnsafeSlot(uninitialized_slot);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(status.code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_THAT(status.message(),
              HasSubstr("unable to register uninitialized slot"));
}

struct SelfReference {
  const SelfReference* self;
  SelfReference() : self(this) {}
  SelfReference(const SelfReference&) = delete;
  SelfReference& operator=(const SelfReference&) = delete;
  ~SelfReference() { self = nullptr; }
};

TEST(FrameLayoutTest, AddSubFrame) {
  // Define sub-frame
  FrameLayout subframe_layout;
  std::vector<FrameLayout::Slot<SelfReference>> field_slots;
  {
    FrameLayout::Builder builder;
    for (int i = 0; i < 2; ++i) {
      field_slots.push_back(builder.AddSlot<SelfReference>());
    }
    subframe_layout = std::move(builder).Build();
  }
  // Define frame
  FrameLayout frame_layout;
  std::vector<FrameLayout::Slot<void>> subframe_slots;
  {
    FrameLayout::Builder builder;
    builder.AddSlot<float>();  // add shift to subframes
    for (int j = 0; j < 3; ++j) {
      // In the first iteration not yet seen type SelfReference will be added.
      // On the next iterations, more fields of this type should be added.
      subframe_slots.push_back(builder.AddSubFrame(subframe_layout));
      builder.AddSlot<double>();  // add shift to subframes
    }
    frame_layout = std::move(builder).Build();
  }
  // Check registration of sub-fields
  for (const auto& subframe_slot : subframe_slots) {
    for (const auto& field_slot : field_slots) {
      EXPECT_TRUE(frame_layout.HasField(
          subframe_slot.byte_offset() + field_slot.byte_offset(),
          typeid(SelfReference)));
    }
  }
  // Check construction of sub-fields
  const auto alloc =
      AlignedAlloc(frame_layout.AllocAlignment(), frame_layout.AllocSize());
  frame_layout.InitializeAlignedAlloc(alloc.get());
  FramePtr frame(alloc.get(), &frame_layout);
  for (const auto& subframe_slot : subframe_slots) {
    for (const auto& field_slot : field_slots) {
      const void* subframe_ptr =
          frame.GetRawPointer(subframe_slot.byte_offset());
      ConstFramePtr subframe(subframe_ptr, &subframe_layout);
      const SelfReference& field = subframe.Get(field_slot);
      EXPECT_TRUE(field.self == &field);
    }
  }
  // Check destruction of sub-fields
  frame_layout.DestroyAlloc(alloc.get());
  // At this point, we expect destructors of all sub-fields to get invoked, and
  // we want to test it. Apparently, MSAN catches the use of uninitialized
  // values because the values are technically destroyed. To suppress the error,
  // we mark the memory as still initialized.
  ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(alloc.get(), frame_layout.AllocSize());
  for (const auto& subframe_slot : subframe_slots) {
    for (const auto& field_slot : field_slots) {
      const void* subframe_ptr =
          frame.GetRawPointer(subframe_slot.byte_offset());
      ConstFramePtr subframe(subframe_ptr, &subframe_layout);
      const SelfReference& field = subframe.Get(field_slot);
      EXPECT_TRUE(field.self == nullptr);
    }
  }
}

TEST(FrameLayoutTest, AddSubFrameAllocAlignment) {
  FrameLayout::Builder builder;
  builder.AddSubFrame(MakeTypeLayout<std::aligned_storage_t<16, 16>>());
  builder.AddSubFrame(MakeTypeLayout<std::aligned_storage_t<16, 16>>());
  auto frame_layout = std::move(builder).Build();
  EXPECT_EQ(frame_layout.AllocSize(), 32);
  EXPECT_EQ(frame_layout.AllocAlignment().value, 16);
}

TEST(FrameLayoutTest, ArrayCompatibility) {
  FrameLayout::Builder builder;
  builder.AddSlot<std::aligned_storage_t<16, 16>>();
  builder.AddSlot<std::aligned_storage_t<1, 1>>();
  auto frame_layout = std::move(builder).Build();
  EXPECT_EQ(frame_layout.AllocSize(), 32);
  EXPECT_EQ(frame_layout.AllocAlignment().value, 16);
}

TEST(FrameLayoutTest, InitDestroyAllocN) {
  static int instance_counter = 0;
  struct InstanceCounted {
    InstanceCounted() { ++instance_counter; }
    ~InstanceCounted() { --instance_counter; }
  };
  struct SelfReferenced {
    SelfReferenced() : self(this) {}
    SelfReferenced* self;
  };

  FrameLayout::Builder builder;
  auto int_slot = builder.AddSlot<int>();
  auto self_ref_slot = builder.AddSlot<SelfReferenced>();
  builder.AddSlot<InstanceCounted>();
  auto layout = std::move(builder).Build();

  const int n = 10;
  const auto alloc =
      AlignedAlloc(layout.AllocAlignment(), layout.AllocSize() * n);

  // Check the initialization.
  layout.InitializeAlignedAllocN(alloc.get(), n);
  EXPECT_EQ(instance_counter, n);
  for (int i = 0; i < n; ++i) {
    ConstFramePtr ith_frame(
        static_cast<const std::byte*>(alloc.get()) + i * layout.AllocSize(),
        &layout);
    EXPECT_EQ(ith_frame.Get(int_slot), 0);
    EXPECT_EQ(ith_frame.Get(self_ref_slot).self, &ith_frame.Get(self_ref_slot));
  }
  // Check the destruction.
  layout.DestroyAllocN(alloc.get(), n);
  EXPECT_EQ(instance_counter, 0);
}

struct IsBZeroConstructible {
  static bool ctor_called;
  static bool dtor_called;
  IsBZeroConstructible() { ctor_called = true; }
  ~IsBZeroConstructible() { dtor_called = true; }
};

bool IsBZeroConstructible::ctor_called;
bool IsBZeroConstructible::dtor_called;

}  // namespace
}  // namespace arolla::testing

namespace arolla {

template <>
struct is_bzero_constructible<::arolla::testing::IsBZeroConstructible>
    : std::true_type {};

}  // namespace arolla

namespace arolla::testing {
namespace {

TEST(FrameLayoutTest, IsBZeroConstructibleHandling) {
  ASSERT_FALSE(IsBZeroConstructible::ctor_called);
  ASSERT_FALSE(IsBZeroConstructible::dtor_called);
  {
    auto layout = MakeTypeLayout<IsBZeroConstructible>();
    MemoryAllocation alloc(&layout);
  }
  EXPECT_FALSE(IsBZeroConstructible::ctor_called);
  EXPECT_TRUE(IsBZeroConstructible::dtor_called);
}

}  // namespace
}  // namespace arolla::testing
