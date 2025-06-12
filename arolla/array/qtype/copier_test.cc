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
#include "arolla/array/qtype/copier.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::testing {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(ArrayToFramesCopier, ArraySizeValidation) {
  Array<int64_t> arr1{CreateDenseArray<int64_t>({3, 4})};
  Array<int64_t> arr2{CreateDenseArray<int64_t>({3, 4, 5})};

  FrameLayout::Builder bldr;
  auto slot1 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto slot2 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto layout = std::move(bldr).Build();

  auto copier = std::make_unique<ArrayToFramesCopier<int64_t>>();
  EXPECT_OK(copier->AddMapping(TypedRef::FromValue(arr1),
                               TypedSlot::FromSlot(slot1)));
  EXPECT_THAT(
      copier->AddMapping(TypedRef::FromValue(arr2), TypedSlot::FromSlot(slot2)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("array size doesn't match: 2 vs 3")));
}

TEST(ArrayToFramesCopier, TryAddMappingWhenStarted) {
  Array<int64_t> arr1{CreateDenseArray<int64_t>({3, 4})};
  Array<int64_t> arr2{CreateDenseArray<int64_t>({4, 5})};

  FrameLayout::Builder bldr;
  auto slot1 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto slot2 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto layout = std::move(bldr).Build();

  auto copier = std::make_unique<ArrayToFramesCopier<int64_t>>();
  EXPECT_OK(copier->AddMapping(TypedRef::FromValue(arr1),
                               TypedSlot::FromSlot(slot1)));
  copier->Start();
  EXPECT_THAT(
      copier->AddMapping(TypedRef::FromValue(arr2), TypedSlot::FromSlot(slot2)),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("cannot add new mappings when started")));
}

TEST(ArrayToFramesCopier, TypesValidation) {
  Array<float> arr1{CreateDenseArray<float>({3.0, 4.0})};
  Array<int64_t> arr2{CreateDenseArray<int64_t>({3, 4})};

  FrameLayout::Builder bldr;
  auto slot1 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto slot2 = bldr.AddSlot<OptionalValue<float>>();
  auto layout = std::move(bldr).Build();

  auto copier = std::make_unique<ArrayToFramesCopier<int64_t>>();
  EXPECT_THAT(
      copier->AddMapping(TypedRef::FromValue(arr1), TypedSlot::FromSlot(slot1)),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("type mismatch")));
  EXPECT_THAT(
      copier->AddMapping(TypedRef::FromValue(arr2), TypedSlot::FromSlot(slot2)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("slot type does not match C++ type")));
}

TEST(ArrayToFramesCopier, Iterate) {
  FrameLayout::Builder scalars_bldr;
  auto scalar_f_slot1 = scalars_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_f_slot2 = scalars_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_f_slot3 = scalars_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_layout = std::move(scalars_bldr).Build();

  Array<float> arr_f1{CreateDenseArray<float>({1.5, {}, 2.5, {}, 4.5})};
  Array<float> arr_f2{CreateDenseArray<float>({{}, 2.2, {}, 1.2, 0.2})};
  Array<float> arr_f3{CreateDenseArray<float>({2.0, {}, 0.1, {}, 2.0})};
  arr_f2 = arr_f2.ToSparseForm();     // sparse form without bitmap
  arr_f3 = arr_f3.ToSparseForm(2.0);  // sparse form with bitmap

  MemoryAllocation ctx0(&scalar_layout);
  MemoryAllocation ctx1(&scalar_layout);
  MemoryAllocation ctx2(&scalar_layout);
  MemoryAllocation ctx3(&scalar_layout);
  MemoryAllocation ctx4(&scalar_layout);

  auto float_copier = std::make_unique<ArrayToFramesCopier<float>>();
  EXPECT_OK(float_copier->AddMapping(TypedRef::FromValue(arr_f1),
                                     TypedSlot::FromSlot(scalar_f_slot1)));
  EXPECT_OK(float_copier->AddMapping(TypedRef::FromValue(arr_f2),
                                     TypedSlot::FromSlot(scalar_f_slot2)));
  EXPECT_OK(float_copier->AddMapping(TypedRef::FromValue(arr_f3),
                                     TypedSlot::FromSlot(scalar_f_slot3)));
  float_copier->Start();

  FramePtr memory_ptrs1[2] = {ctx0.frame(), ctx1.frame()};
  float_copier->CopyNextBatch({memory_ptrs1, 2});

  EXPECT_EQ(ctx0.frame().Get(scalar_f_slot1), OptionalValue<float>(1.5));
  EXPECT_EQ(ctx0.frame().Get(scalar_f_slot2), OptionalValue<float>());
  EXPECT_EQ(ctx0.frame().Get(scalar_f_slot3), OptionalValue<float>(2.0));

  EXPECT_EQ(ctx1.frame().Get(scalar_f_slot1), OptionalValue<float>());
  EXPECT_EQ(ctx1.frame().Get(scalar_f_slot2), OptionalValue<float>(2.2));
  EXPECT_EQ(ctx1.frame().Get(scalar_f_slot3), OptionalValue<float>());

  FramePtr memory_ptrs2[2] = {ctx2.frame(), ctx3.frame()};
  float_copier->CopyNextBatch({memory_ptrs2, 2});

  EXPECT_EQ(ctx2.frame().Get(scalar_f_slot1), OptionalValue<float>(2.5));
  EXPECT_EQ(ctx2.frame().Get(scalar_f_slot2), OptionalValue<float>());
  EXPECT_EQ(ctx2.frame().Get(scalar_f_slot3), OptionalValue<float>(0.1));

  EXPECT_EQ(ctx3.frame().Get(scalar_f_slot1), OptionalValue<float>());
  EXPECT_EQ(ctx3.frame().Get(scalar_f_slot2), OptionalValue<float>(1.2));
  EXPECT_EQ(ctx3.frame().Get(scalar_f_slot3), OptionalValue<float>());

  FramePtr memory_ptrs3[1] = {ctx4.frame()};
  float_copier->CopyNextBatch({memory_ptrs3, 1});

  EXPECT_EQ(ctx4.frame().Get(scalar_f_slot1), OptionalValue<float>(4.5));
  EXPECT_EQ(ctx4.frame().Get(scalar_f_slot2), OptionalValue<float>(0.2));
  EXPECT_EQ(ctx4.frame().Get(scalar_f_slot3), OptionalValue<float>(2.0));
}

TEST(ArrayFromFramesCopier, TryAddMappingWhenStarted) {
  FrameLayout::Builder arrays_bldr;
  auto array_slot1 = arrays_bldr.AddSlot<Array<int64_t>>();
  auto array_slot2 = arrays_bldr.AddSlot<Array<int64_t>>();
  auto arrays_layout = std::move(arrays_bldr).Build();

  FrameLayout::Builder bldr;
  auto slot1 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto slot2 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto layout = std::move(bldr).Build();

  auto copier = std::make_unique<ArrayFromFramesCopier<int64_t>>();
  EXPECT_OK(copier->AddMapping(TypedSlot::FromSlot(slot1),
                               TypedSlot::FromSlot(array_slot1)));
  copier->Start(4);
  EXPECT_THAT(copier->AddMapping(TypedSlot::FromSlot(slot2),
                                 TypedSlot::FromSlot(array_slot2)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("cannot add new mappings when started")));
}

TEST(ArrayFromFramesCopier, TypesValidation) {
  FrameLayout::Builder arrays_bldr;
  auto array_slot1 = arrays_bldr.AddSlot<Array<float>>();
  auto array_slot2 = arrays_bldr.AddSlot<Array<int64_t>>();
  auto arrays_layout = std::move(arrays_bldr).Build();

  FrameLayout::Builder bldr;
  auto slot1 = bldr.AddSlot<OptionalValue<int64_t>>();
  auto slot2 = bldr.AddSlot<OptionalValue<float>>();
  auto layout = std::move(bldr).Build();

  auto copier = std::make_unique<ArrayFromFramesCopier<int64_t>>();
  EXPECT_THAT(copier->AddMapping(TypedSlot::FromSlot(slot1),
                                 TypedSlot::FromSlot(array_slot1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("slot type does not match C++ type")));
  EXPECT_THAT(copier->AddMapping(TypedSlot::FromSlot(slot2),
                                 TypedSlot::FromSlot(array_slot2)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("slot type does not match C++ type")));
}

TEST(ArrayFromFramesCopier, Iterate) {
  FrameLayout::Builder arrays_bldr;
  auto array_slot1 = arrays_bldr.AddSlot<Array<float>>();
  auto array_slot2 = arrays_bldr.AddSlot<Array<float>>();
  auto arrays_layout = std::move(arrays_bldr).Build();

  FrameLayout::Builder scalars_bldr;
  auto scalar_f_slot1 = scalars_bldr.AddSlot<OptionalValue<float>>();
  auto scalar_f_slot2 = scalars_bldr.AddSlot<float>();
  auto scalar_layout = std::move(scalars_bldr).Build();

  MemoryAllocation ctx0(&scalar_layout);
  MemoryAllocation ctx1(&scalar_layout);
  MemoryAllocation ctx2(&scalar_layout);
  MemoryAllocation ctx3(&scalar_layout);

  ctx0.frame().Set(scalar_f_slot1, 1.5);
  ctx0.frame().Set(scalar_f_slot2, 3.2);

  ctx1.frame().Set(scalar_f_slot1, {});
  ctx1.frame().Set(scalar_f_slot2, 2.2);

  ctx2.frame().Set(scalar_f_slot1, 2.5);
  ctx2.frame().Set(scalar_f_slot2, 0.0);

  ctx3.frame().Set(scalar_f_slot1, 3.5);
  ctx3.frame().Set(scalar_f_slot2, 1.2);

  auto copier = std::make_unique<ArrayFromFramesCopier<float>>();
  EXPECT_OK(copier->AddMapping(TypedSlot::FromSlot(scalar_f_slot1),
                               TypedSlot::FromSlot(array_slot1)));
  EXPECT_OK(copier->AddMapping(TypedSlot::FromSlot(scalar_f_slot2),
                               TypedSlot::FromSlot(array_slot2)));

  ConstFramePtr memory_ptrs1[2] = {ctx0.frame(), ctx1.frame()};
  ConstFramePtr memory_ptrs2[2] = {ctx2.frame(), ctx3.frame()};
  MemoryAllocation arrays_ctx(&arrays_layout);

  copier->Start(4);
  EXPECT_OK(copier->CopyNextBatch({memory_ptrs1, 2}));
  EXPECT_OK(copier->CopyNextBatch({memory_ptrs2, 2}));
  EXPECT_OK(copier->Finalize(arrays_ctx.frame()));
  EXPECT_THAT(copier->Finalize(arrays_ctx.frame()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("finalize can be called only once")));

  EXPECT_THAT(arrays_ctx.frame().Get(array_slot1),
              ElementsAre(1.5, std::nullopt, 2.5, 3.5));
  EXPECT_THAT(arrays_ctx.frame().Get(array_slot2),
              ElementsAre(3.2, 2.2, 0.0, 1.2));
}

}  // namespace
}  // namespace arolla::testing
