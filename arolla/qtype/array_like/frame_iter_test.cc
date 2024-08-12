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
#include "arolla/qtype/array_like/frame_iter.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/threading.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::Test;

TEST(FrameIterator, Iterate) {
  FrameLayout::Builder scalar_bldr;
  auto scalar_f_slot1 = scalar_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_i_slot1 = scalar_bldr.AddSlot<OptionalValue<int64_t>>();
  auto scalar_i_slot2 = scalar_bldr.AddSlot<OptionalValue<int64_t>>();
  auto scalar_f_slot2 = scalar_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_layout = std::move(scalar_bldr).Build();

  // We use the same scalar_slots both as input_slots and output_slots
  std::vector<TypedSlot> scalar_slots = {
      TypedSlot::FromSlot(scalar_f_slot1), TypedSlot::FromSlot(scalar_i_slot1),
      TypedSlot::FromSlot(scalar_i_slot2), TypedSlot::FromSlot(scalar_f_slot2)};

  DenseArray<float> arr_f1 =
      CreateDenseArray<float>({1.5, std::nullopt, 2.5, 3.5});
  DenseArray<int64_t> arr_i1 = CreateDenseArray<int64_t>({3, 4, 5, 6});
  DenseArray<int64_t> arr_i2 =
      CreateDenseArray<int64_t>({2, std::nullopt, 0, std::nullopt});
  DenseArray<float> arr_f2 =
      CreateDenseArray<float>({3.2, 2.2, std::nullopt, 1.2});

  FrameLayout::Builder vector_bldr;
  auto arr_output_f1 = vector_bldr.AddSlot<DenseArray<float>>();
  auto arr_output_i1 = vector_bldr.AddSlot<DenseArray<int64_t>>();
  auto arr_output_i2 = vector_bldr.AddSlot<DenseArray<int64_t>>();
  auto arr_output_f2 = vector_bldr.AddSlot<DenseArray<float>>();
  auto output_vector_layout = std::move(vector_bldr).Build();

  std::vector<TypedRef> input_refs = {
      TypedRef::FromValue(arr_f1), TypedRef::FromValue(arr_i1),
      TypedRef::FromValue(arr_i2), TypedRef::FromValue(arr_f2)};

  std::vector<TypedSlot> output_slots = {
      TypedSlot::FromSlot(arr_output_f1), TypedSlot::FromSlot(arr_output_i1),
      TypedSlot::FromSlot(arr_output_i2), TypedSlot::FromSlot(arr_output_f2)};

  auto scalar_processing_fn = [&](FramePtr frame) {
    OptionalValue<float> f1 = frame.Get(scalar_f_slot1);
    OptionalValue<float> f2 = frame.Get(scalar_f_slot2);
    if (f1.present) frame.Set(scalar_f_slot1, f1.value + 1.0);
    if (f2.present) frame.Set(scalar_f_slot2, f2.value + 2.0);

    OptionalValue<int64_t> i1 = frame.Get(scalar_i_slot1);
    OptionalValue<int64_t> i2 = frame.Get(scalar_i_slot2);
    if (i1.present) frame.Set(scalar_i_slot1, i1.value + 3);
    if (i2.present) frame.Set(scalar_i_slot2, i2.value + 4);
  };

  auto check_output_fn = [&](FrameIterator& frame_iterator) {
    MemoryAllocation alloc(&output_vector_layout);
    FramePtr output_frame = alloc.frame();
    EXPECT_OK(frame_iterator.StoreOutput(output_frame));

    EXPECT_THAT(output_frame.Get(arr_output_f1),
                ElementsAre(2.5, std::nullopt, 3.5, 4.5));
    EXPECT_THAT(output_frame.Get(arr_output_f2),
                ElementsAre(5.2, 4.2, std::nullopt, 3.2));
    EXPECT_THAT(output_frame.Get(arr_output_i1), ElementsAre(6, 7, 8, 9));
    EXPECT_THAT(output_frame.Get(arr_output_i2),
                ElementsAre(6, std::nullopt, 4, std::nullopt));
  };

  {  // without multithreading
    ASSERT_OK_AND_ASSIGN(
        auto frame_iterator,
        FrameIterator::Create(input_refs, scalar_slots, output_slots,
                              scalar_slots, &scalar_layout,
                              {.frame_buffer_count = 2}));
    frame_iterator.ForEachFrame(scalar_processing_fn);
    check_output_fn(frame_iterator);
  }

  // with multithreading
  StdThreading threading(4);
  for (int threads = 1; threads <= 4; ++threads) {
    ASSERT_OK_AND_ASSIGN(
        auto frame_iterator,
        FrameIterator::Create(input_refs, scalar_slots, output_slots,
                              scalar_slots, &scalar_layout,
                              {.frame_buffer_count = 3}));
    frame_iterator.ForEachFrame(scalar_processing_fn, threading, threads);
    check_output_fn(frame_iterator);
  }
}

TEST(FrameIterator, EmptyArrays) {
  FrameLayout::Builder scalar_bldr;
  auto scalar_slot = scalar_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_layout = std::move(scalar_bldr).Build();
  std::vector<TypedSlot> scalar_slots = {TypedSlot::FromSlot(scalar_slot)};

  FrameLayout::Builder arrays_layout_bldr;
  auto arr_output = arrays_layout_bldr.AddSlot<DenseArray<float>>();
  auto output_arrays_layout = std::move(arrays_layout_bldr).Build();

  DenseArray<float> arr;
  std::vector<TypedRef> input_refs = {TypedRef::FromValue(arr)};
  std::vector<TypedSlot> output_slots = {TypedSlot::FromSlot(arr_output)};

  auto scalar_processing_fn = [&](FramePtr frame) { ADD_FAILURE(); };

  ASSERT_OK_AND_ASSIGN(auto frame_iterator,
                       FrameIterator::Create(
                           input_refs, scalar_slots, output_slots, scalar_slots,
                           &scalar_layout, {.frame_buffer_count = 2}));
  frame_iterator.ForEachFrame(scalar_processing_fn);
  MemoryAllocation alloc(&output_arrays_layout);
  FramePtr output_frame = alloc.frame();
  EXPECT_OK(frame_iterator.StoreOutput(output_frame));
  EXPECT_EQ(output_frame.Get(arr_output).size(), 0);
}

TEST(FrameIterator, EmptyInputAndOutput) {
  FrameLayout::Builder scalar_bldr;
  auto scalar_layout = std::move(scalar_bldr).Build();

  {
    auto frame_iterator_or_status =
        FrameIterator::Create({}, {}, {}, {}, &scalar_layout);
    EXPECT_THAT(
        frame_iterator_or_status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("options.row_count can not be missed if there "
                           "is no input arrays")));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto frame_iterator,
                         FrameIterator::Create({}, {}, {}, {}, &scalar_layout,
                                               {.row_count = 4}));
    EXPECT_EQ(frame_iterator.row_count(), 4);
  }
}

TEST(FrameIterator, IncorrectInputType) {
  FrameLayout::Builder scalar_bldr;
  // Incorrect type, should be OptionalValue<float>
  auto scalar_slot = scalar_bldr.AddSlot<float>();
  auto scalar_layout = std::move(scalar_bldr).Build();

  std::vector<TypedSlot> scalar_slots = {TypedSlot::FromSlot(scalar_slot)};

  DenseArray<int64_t> arr = CreateDenseArray<int64_t>({1, std::nullopt, 2, 3});

  auto frame_iterator_or_status = FrameIterator::Create(
      {TypedRef::FromValue(arr)}, scalar_slots, {}, {}, &scalar_layout);

  EXPECT_THAT(frame_iterator_or_status,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("slot type does not match")));
}

TEST(FrameIterator, IncorrectOutputType) {
  FrameLayout::Builder vector_bldr;
  auto vector_slot = vector_bldr.AddSlot<DenseArray<float>>();
  auto vector_layout = std::move(vector_bldr).Build();

  FrameLayout::Builder scalar_bldr;
  // Incorrect type, should be float or OptionalValue<float>
  auto scalar_slot = scalar_bldr.AddSlot<int64_t>();
  auto scalar_layout = std::move(scalar_bldr).Build();

  auto frame_iterator_or_status =
      FrameIterator::Create({}, {}, {TypedSlot::FromSlot(vector_slot)},
                            {TypedSlot::FromSlot(scalar_slot)}, &scalar_layout);

  EXPECT_THAT(frame_iterator_or_status,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("slot type does not match")));
}

TEST(FrameIterator, WrongSize) {
  FrameLayout::Builder scalar_bldr;
  auto scalar_f_slot1 = scalar_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_i_slot1 = scalar_bldr.AddSlot<OptionalValue<int64_t>>();
  auto scalar_layout = std::move(scalar_bldr).Build();

  std::vector<TypedSlot> scalar_slots = {TypedSlot::FromSlot(scalar_f_slot1),
                                         TypedSlot::FromSlot(scalar_i_slot1)};

  DenseArray<float> arr_f1 =
      CreateDenseArray<float>({1.5, std::nullopt, 2.5, 3.5});
  DenseArray<int64_t> arr_i1 = CreateDenseArray<int64_t>({3, 4, 5});

  auto frame_iterator_or_status = FrameIterator::Create(
      {TypedRef::FromValue(arr_f1), TypedRef::FromValue(arr_i1)}, scalar_slots,
      {}, {}, &scalar_layout);

  EXPECT_THAT(frame_iterator_or_status,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("input arrays have different sizes")));
}

}  // namespace
}  // namespace arolla
