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
#include "arolla/codegen/io/multi_loader.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::codegen::io {
namespace {

using ::testing::ElementsAre;

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsTrivialAllRequested) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto b_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto c_slot = layout_builder.AddSlot<OptionalValue<int>>();

  HierarchicalSingleValueRequestedInputsData<3, 4> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {TypedSlot::FromSlot(a_slot), TypedSlot::FromSlot(b_slot),
       TypedSlot::FromSlot(c_slot)},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {}, {0, 1, 2}}, &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(a_slot.byte_offset(), b_slot.byte_offset(),
                          c_slot.byte_offset()));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true));
  // size offsets go at the end.
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(a_slot.byte_offset(), c_slot.byte_offset(),
                          b_slot.byte_offset()));
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{0, 2}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{2, 3}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsTrivialNothingRequested) {
  HierarchicalSingleValueRequestedInputsData<3, 4> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {std::nullopt, std::nullopt, std::nullopt},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {}, {0, 1, 2}}, &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(kSkippedOffset, kSkippedOffset, kSkippedOffset));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(false));
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(kSkippedOffset, kSkippedOffset, kSkippedOffset));
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{0, 0}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{0, 0}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsTrivialSizeRequested) {
  FrameLayout::Builder layout_builder;
  auto b_slot = layout_builder.AddSlot<OptionalValue<int>>();
  HierarchicalSingleValueRequestedInputsData<3, 4> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {std::nullopt, TypedSlot::FromSlot(b_slot), std::nullopt},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {}, {0, 1, 2}}, &inputs);
  EXPECT_THAT(
      inputs.common.leaf_frame_offsets,
      ElementsAre(kSkippedOffset, b_slot.byte_offset(), kSkippedOffset));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true));
  EXPECT_THAT(
      inputs.requested_offsets,
      ElementsAre(b_slot.byte_offset(), kSkippedOffset, kSkippedOffset));
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{0, 0}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{0, 1}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsTrivialOptionalRequested) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<int>>();
  HierarchicalSingleValueRequestedInputsData<3, 4> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {TypedSlot::FromSlot(a_slot), std::nullopt, std::nullopt},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {}, {0, 1, 2}}, &inputs);
  EXPECT_THAT(
      inputs.common.leaf_frame_offsets,
      ElementsAre(a_slot.byte_offset(), kSkippedOffset, kSkippedOffset));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true));
  EXPECT_THAT(
      inputs.requested_offsets,
      ElementsAre(a_slot.byte_offset(), kSkippedOffset, kSkippedOffset));
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{0, 1}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(HierarchicalSingleValueClearInfo{1, 1}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsHierarchyAllRequested) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto b_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto c_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto d_slot = layout_builder.AddSlot<OptionalValue<int>>();
  HierarchicalSingleValueRequestedInputsData<4, 7> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {TypedSlot::FromSlot(a_slot), TypedSlot::FromSlot(b_slot),
       TypedSlot::FromSlot(c_slot), TypedSlot::FromSlot(d_slot)},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {0, 1}, {}, {3}, {}, {2, 4, 5}},
      &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(a_slot.byte_offset(), b_slot.byte_offset(),
                          c_slot.byte_offset(), d_slot.byte_offset()));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true, true, true));
  // size offsets go at the end.
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(a_slot.byte_offset(), c_slot.byte_offset(),
                          d_slot.byte_offset(), b_slot.byte_offset()));
  using CI = HierarchicalSingleValueClearInfo;
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(CI{0, 1}, CI{1, 2}, CI{0, 3}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(CI{3, 4}, CI{4, 4}, CI{3, 4}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsAFewRequestedWithFullValue) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto c_slot = layout_builder.AddSlot<int>();
  HierarchicalSingleValueRequestedInputsData<4, 7> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {TypedSlot::FromSlot(a_slot), std::nullopt, TypedSlot::FromSlot(c_slot),
       std::nullopt},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {0, 1}, {}, {3}, {}, {2, 4, 5}},
      &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(a_slot.byte_offset(), kSkippedOffset,
                          c_slot.byte_offset(), kSkippedOffset));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true, true, true));
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(a_slot.byte_offset(), kSkippedOffset, kSkippedOffset,
                          kSkippedOffset));
  using CI = HierarchicalSingleValueClearInfo;
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(CI{0, 1}, CI{1, 1}, CI{0, 1}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(CI{1, 1}, CI{1, 1}, CI{1, 1}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsAllRequestedWithFullValue) {
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<int>>();
  auto b_slot = layout_builder.AddSlot<DenseArrayShape>();
  auto c_slot = layout_builder.AddSlot<int>();
  auto d_slot = layout_builder.AddSlot<OptionalValue<int>>();
  HierarchicalSingleValueRequestedInputsData<4, 7> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {TypedSlot::FromSlot(a_slot), TypedSlot::FromSlot(b_slot),
       TypedSlot::FromSlot(c_slot), TypedSlot::FromSlot(d_slot)},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {0, 1}, {}, {3}, {}, {2, 4, 5}},
      &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(a_slot.byte_offset(), b_slot.byte_offset(),
                          c_slot.byte_offset(), d_slot.byte_offset()));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true, true, true));
  // size offsets go at the end.
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(a_slot.byte_offset(), d_slot.byte_offset(),
                          b_slot.byte_offset(), kSkippedOffset));
  using CI = HierarchicalSingleValueClearInfo;
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(CI{0, 1}, CI{1, 1}, CI{0, 2}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(CI{2, 3}, CI{3, 3}, CI{2, 3}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsHierarchySizeRequested) {
  FrameLayout::Builder layout_builder;
  auto b_slot = layout_builder.AddSlot<OptionalValue<int>>();
  HierarchicalSingleValueRequestedInputsData<4, 7> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {std::nullopt, TypedSlot::FromSlot(b_slot), std::nullopt, std::nullopt},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {0, 1}, {}, {3}, {}, {2, 4}},
      &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(kSkippedOffset, b_slot.byte_offset(), kSkippedOffset,
                          kSkippedOffset));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(true, false, true));
  // size offsets go at the end.
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(b_slot.byte_offset(), kSkippedOffset, kSkippedOffset,
                          kSkippedOffset));
  using CI = HierarchicalSingleValueClearInfo;
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(CI{0, 0}, CI{0, 0}, CI{0, 0}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(CI{0, 1}, CI{1, 1}, CI{0, 1}));
}

TEST(SingleValueTest,
     CreateHierarchicalSingleValueRequestedInputsHierarchyOptionalRequested) {
  FrameLayout::Builder layout_builder;
  auto c_slot = layout_builder.AddSlot<OptionalValue<int>>();
  HierarchicalSingleValueRequestedInputsData<4, 7> inputs;
  CreateHierarchicalSingleValueRequestedInputs(
      {std::nullopt, std::nullopt, TypedSlot::FromSlot(c_slot), std::nullopt},
      /*size_leaves=*/{1}, /*tree=*/{{}, {}, {0, 1}, {}, {3}, {}, {2, 4}},
      &inputs);
  EXPECT_THAT(inputs.common.leaf_frame_offsets,
              ElementsAre(kSkippedOffset, kSkippedOffset, c_slot.byte_offset(),
                          kSkippedOffset));
  EXPECT_THAT(inputs.common.node_requested, ElementsAre(false, true, true));
  // size offsets go at the end.
  EXPECT_THAT(inputs.requested_offsets,
              ElementsAre(c_slot.byte_offset(), kSkippedOffset, kSkippedOffset,
                          kSkippedOffset));
  using CI = HierarchicalSingleValueClearInfo;
  EXPECT_THAT(inputs.node_optional_clear_infos,
              ElementsAre(CI{0, 0}, CI{0, 1}, CI{0, 1}));
  EXPECT_THAT(inputs.node_size_clear_infos,
              ElementsAre(CI{1, 1}, CI{1, 1}, CI{1, 1}));
}

TEST(ResizeRepeatedProtoFieldTest, MessageResize) {
  testing_namespace::Root root;
  // increase from 0
  ResizeRepeatedProtoField(root.mutable_inners(), 5);
  EXPECT_EQ(root.inners_size(), 5);
  EXPECT_FALSE(root.inners(0).has_a());
  root.mutable_inners(0)->set_a(13);

  // increase from non 0
  ResizeRepeatedProtoField(root.mutable_inners(), 7);
  EXPECT_EQ(root.inners_size(), 7);
  EXPECT_TRUE(root.inners(0).has_a());
  EXPECT_EQ(root.inners(0).a(), 13);

  // reduce
  ResizeRepeatedProtoField(root.mutable_inners(), 3);
  EXPECT_EQ(root.inners_size(), 3);
  EXPECT_TRUE(root.inners(0).has_a());
  EXPECT_EQ(root.inners(0).a(), 13);

  // no resize
  ResizeRepeatedProtoField(root.mutable_inners(), 3);
  EXPECT_EQ(root.inners_size(), 3);
  EXPECT_TRUE(root.inners(0).has_a());
  EXPECT_EQ(root.inners(0).a(), 13);
}

}  // namespace
}  // namespace arolla::codegen::io
