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
#include "arolla/io/delegating_input_loader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::arolla::testing::InputLoaderSupports;

struct TestStruct {
  int a;
  double b;
};

struct OuterTestStruct {
  TestStruct* a;
  int b;
};

TEST(InputLoaderTest, DelegatingInputLoader) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  std::unique_ptr<InputLoader<OuterTestStruct>> input_loader;
  std::unique_ptr<InputLoader<OuterTestStruct>> renamed_input_loader;
  std::unique_ptr<InputLoader<OuterTestStruct>> input_loader_with_copy_allowed;
  {  // scope to delete loader_struct. It is still owned by owning_input_loader.
    auto create_struct_loader = []() {
      return CreateAccessorsInputLoader<TestStruct>(
                 "a", [](const TestStruct& s) { return s.a; }, "b",
                 [](const TestStruct& s) { return s.b; })
          .value();
    };
    auto accessor = [](const OuterTestStruct& s) -> const TestStruct& {
      return *s.a;
    };
    ASSERT_OK_AND_ASSIGN(input_loader,
                         CreateDelegatingInputLoader<OuterTestStruct>(
                             create_struct_loader(), accessor));
    ASSERT_OK_AND_ASSIGN(
        renamed_input_loader,
        CreateDelegatingInputLoader<OuterTestStruct>(
            create_struct_loader(), accessor, /*name_prefix=*/"p_"));
    ASSERT_OK_AND_ASSIGN(
        auto loader_span,
        CreateAccessorsInputLoader<absl::Span<const TestStruct>>(
            "a",
            [](const auto& s) {
              CHECK_EQ(s.size(), 1);
              return s[0].a;
            },
            "b",
            [](const auto& s) {
              CHECK_EQ(s.size(), 1);
              return s[0].b;
            }));
    auto span_accessor =
        [](const OuterTestStruct& s) -> absl::Span<const TestStruct> {
      return absl::MakeConstSpan(s.a, 1);
    };
    ASSERT_OK_AND_ASSIGN(
        input_loader_with_copy_allowed,
        CreateDelegatingInputLoaderWithCopyAllowed<OuterTestStruct>(
            std::move(loader_span), span_accessor));
  }
  for (const auto& [prefix, input_loader] :
       std::vector<std::pair<std::string, const InputLoader<OuterTestStruct>*>>{
           {"", input_loader.get()},
           {"p_", renamed_input_loader.get()},
           {"", input_loader_with_copy_allowed.get()}}) {
    EXPECT_THAT(*input_loader, InputLoaderSupports(
                                   {{prefix + "a", i32}, {prefix + "b", f64}}));
    FrameLayout::Builder layout_builder;
    auto a_slot = layout_builder.AddSlot<int>();
    auto b_slot = layout_builder.AddSlot<double>();
    ASSERT_OK_AND_ASSIGN(BoundInputLoader<OuterTestStruct> bound_input_loader,
                         input_loader->Bind({
                             {prefix + "a", TypedSlot::FromSlot(a_slot)},
                             {prefix + "b", TypedSlot::FromSlot(b_slot)},
                         }));
    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    TestStruct ts{5, 3.5};
    ASSERT_OK(bound_input_loader({&ts, -1}, alloc.frame()));
    EXPECT_EQ(alloc.frame().Get(a_slot), 5);
    EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
  }
}

}  // namespace
}  // namespace arolla
