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
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "arolla/codegen/io/testing/test_proto_multi_value_input_loader_stress.h"
#include "arolla/codegen/io/testing/test_proto_single_value_input_loader_stress.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

TEST(InputLoaderTest, TestGetSingleValueProtoLoader) {
  auto fn = [&]() {
    const auto& input_loader =
        ::my_namespace_stress::GetSingleValueProtoLoader();
    absl::flat_hash_map<std::string, QTypePtr> output_types;
    constexpr size_t N = 10;
    for (int i = 0; i != N; ++i) {
      for (int j = 0; j != N; ++j) {
        for (int k = 0; k != N; ++k) {
          std::string key = absl::StrFormat("x%d_%d_%d", i, j, k);
          EXPECT_EQ(input_loader->GetQTypeOf(key), GetOptionalQType<int32_t>());
          output_types.emplace(key, GetOptionalQType<int32_t>());
        }
      }
    }
    FrameLayout::Builder layout_builder;
    auto slots_map = AddSlotsMap(output_types, &layout_builder);
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->Bind(slots_map));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ::testing_namespace::Root root;
    ASSERT_OK(bound_input_loader(root, frame));
    for (int i = 0; i != N; ++i) {
      for (int j = 0; j != N; ++j) {
        for (int k = 0; k != N; ++k) {
          std::string key = absl::StrFormat("x%d_%d_%d", i, j, k);
          ASSERT_OK_AND_ASSIGN(
              auto slot, slots_map.at(key).ToSlot<OptionalValue<int32_t>>());
          EXPECT_EQ(frame.Get(slot), std::nullopt);
        }
      }
    }
  };
  fn();
}

TEST(InputLoaderTest, TestGetMultiValueProtoLoader) {
  auto fn = [&]() {
    const auto& input_loader =
        ::my_namespace_stress::GetMultiValueProtoLoader();
    absl::flat_hash_map<std::string, QTypePtr> output_types;
    constexpr size_t N = 10;
    for (int i = 0; i != N; ++i) {
      for (int j = 0; j != N; ++j) {
        for (int k = 0; k != N; ++k) {
          std::string key = absl::StrFormat("x%d_%d_%d", i, j, k);
          EXPECT_EQ(input_loader->GetQTypeOf(key),
                    GetDenseArrayQType<int32_t>());
          output_types.emplace(key, GetDenseArrayQType<int32_t>());
        }
      }
    }
    FrameLayout::Builder layout_builder;
    auto slots_map = AddSlotsMap(output_types, &layout_builder);
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->Bind(slots_map));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    std::vector<::testing_namespace::Root> roots(2);
    ASSERT_OK(bound_input_loader(roots, frame));
    for (int i = 0; i != N; ++i) {
      for (int j = 0; j != N; ++j) {
        for (int k = 0; k != N; ++k) {
          std::string key = absl::StrFormat("x%d_%d_%d", i, j, k);
          ASSERT_OK_AND_ASSIGN(auto slot,
                               slots_map.at(key).ToSlot<DenseArray<int32_t>>());
          EXPECT_THAT(frame.Get(slot), testing::IsEmpty());
        }
      }
    }
  };
  fn();
}

}  // namespace
}  // namespace arolla
