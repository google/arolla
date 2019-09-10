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
#ifndef AROLLA_QTYPE_QTYPE_TEST_UTILS_H_
#define AROLLA_QTYPE_QTYPE_TEST_UTILS_H_

#include <array>
#include <utility>

#include "gtest/gtest.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {

// Template function for testing traits for primitive types.
template <typename CPP_TYPE>
void TestPrimitiveTraits(const char* type_name, CPP_TYPE value) {
  // Get corresponding QType object and verify its attributes.
  QTypePtr type = GetQType<CPP_TYPE>();
  EXPECT_EQ(type->name(), type_name);
  EXPECT_EQ(type->type_info(), typeid(CPP_TYPE));

  // Adding slots using QType::AddSlot.
  FrameLayout::Builder layout_builder;
  auto slot1 = AddSlot(type, &layout_builder);
  EXPECT_EQ(slot1.GetType(), type);
  auto slot2 = AddSlot(type, &layout_builder);
  EXPECT_EQ(slot2.GetType(), type);
  auto slot3 = AddSlot(type, &layout_builder);
  EXPECT_EQ(slot2.GetType(), type);

  // Testing TypedSlot::CopyTo
  FrameLayout layout = std::move(layout_builder).Build();
  std::array<CPP_TYPE, 3> alloc_holder;
  ASSERT_EQ(layout.AllocSize(), sizeof(alloc_holder));
  FramePtr frame(alloc_holder.data(), &layout);
  frame.Set(slot1.ToSlot<CPP_TYPE>().value(), value);
  slot1.CopyTo(frame, slot2, frame);
  EXPECT_EQ(frame.Get(slot2.ToSlot<CPP_TYPE>().value()), value);

  // Testing getting value from a TypedSlot.
  auto typed_value = TypedValue::FromSlot(slot2, frame);
  auto value_or = typed_value.As<CPP_TYPE>();
  EXPECT_OK(value_or.status());
  EXPECT_EQ(value_or.value().get(), value);

  // Testing setting slot from a TypedValue.
  EXPECT_OK(TypedValue::FromValue(value).CopyToSlot(slot3, frame));
  EXPECT_EQ(frame.Get(slot3.ToSlot<CPP_TYPE>().value()), value);
}

}  // namespace arolla

#endif  // AROLLA_QTYPE_QTYPE_TEST_UTILS_H_
