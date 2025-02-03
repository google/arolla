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
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "arolla/codegen/io/testing/test_input_loader_compilation_stress.h"
#include "arolla/codegen/io/testing/test_input_loader_compilation_stress_sharded.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

TEST(InputLoaderTest, TestCompilationStressLoader) {
  auto i32 = GetQType<int32_t>();
  for (const auto& input_loader :
       {::my_namespace_stress::GetStressLoader(),
        ::my_namespace_stress::GetStressShardedLoader()}) {
    absl::flat_hash_map<std::string, QTypePtr> output_types;
    size_t N = output_types.size();
    for (int i = 0; i != N; ++i) {
      std::string name = absl::StrCat("a", i);
      EXPECT_EQ(input_loader->GetQTypeOf(name), i32);
      output_types.emplace(name, i32);
    }
    FrameLayout::Builder layout_builder;
    auto slots_map = AddSlotsMap(output_types, &layout_builder);
    ASSERT_OK_AND_ASSIGN(BoundInputLoader<int> bound_input_loader,
                         input_loader->Bind(slots_map));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    FramePtr frame = alloc.frame();

    ASSERT_OK(bound_input_loader(19, frame));
    for (int i = 0; i != N; ++i) {
      ASSERT_OK_AND_ASSIGN(auto slot,
                           slots_map.at(absl::StrCat("a", i)).ToSlot<int>());
      EXPECT_EQ(frame.Get(slot), 19 + i);
    }
  }
}

}  // namespace
}  // namespace arolla
