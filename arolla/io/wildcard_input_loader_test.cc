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
#include "arolla/io/wildcard_input_loader.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/optional.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::InputLoaderSupports;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::MatchesRegex;

struct DummyInput {};

TEST(WildcardInputLoaderCombinationTest, MakeNameToKeyFn) {
  {
    auto fn = input_loader_impl::MakeNameToKeyFn(absl::ParsedFormat<'s'>("%s"));
    EXPECT_THAT(fn(""), Eq(""));
    EXPECT_THAT(fn("foo"), Eq("foo"));
    EXPECT_THAT(fn("foobarbaz\\[.*\"]"), Eq("foobarbaz\\[.*\"]"));
  }
  {
    auto fn = input_loader_impl::MakeNameToKeyFn(
        absl::ParsedFormat<'s'>("%s_only_suffix"));
    EXPECT_THAT(fn(""), Eq(std::nullopt));
    EXPECT_THAT(fn("_only_suffix"), Eq(""));
    EXPECT_THAT(fn("foo_only_suffix"), Eq("foo"));
  }
  {
    auto fn = input_loader_impl::MakeNameToKeyFn(
        absl::ParsedFormat<'s'>("only_prefix_%s"));
    EXPECT_THAT(fn(""), Eq(std::nullopt));
    EXPECT_THAT(fn("only_prefix_"), Eq(""));
    EXPECT_THAT(fn("only_prefix_foo"), Eq("foo"));
  }
  {
    auto fn = input_loader_impl::MakeNameToKeyFn(
        absl::ParsedFormat<'s'>("prefix_%s_and_suffix"));
    EXPECT_THAT(fn(""), Eq(std::nullopt));
    EXPECT_THAT(fn("prefix_"), Eq(std::nullopt));
    EXPECT_THAT(fn("_and_suffix"), Eq(std::nullopt));
    EXPECT_THAT(fn("prefix__and_suffix"), Eq(""));
    EXPECT_THAT(fn("prefix_foo_and_suffix"), Eq("foo"));
  }
}

TEST(InputLoaderTest, InputLoaderAccessorResultType) {
  using Input = absl::flat_hash_map<std::string, int>;
  {
    auto accessor = [](const Input& input, const std::string& key) {
      return 1;
    };

    static_assert(
        std::is_same_v<
            WildcardAccessorResultType<decltype(accessor), Input, std::string>,
            int>);
  }
  {
    auto accessor = [](const Input& input,
                       const std::string& key) -> absl::StatusOr<int> {
      return 1;
    };

    static_assert(
        std::is_same_v<
            WildcardAccessorResultType<decltype(accessor), Input, std::string>,
            int>);
  }
  {
    auto accessor = [](const Input& input, const std::string& key,
                       RawBufferFactory*) { return 1; };

    static_assert(
        std::is_same_v<
            WildcardAccessorResultType<decltype(accessor), Input, std::string>,
            int>);
  }
  {
    auto accessor = [](const Input& input, const std::string& key,
                       RawBufferFactory*) -> absl::StatusOr<int> { return 1; };

    static_assert(
        std::is_same_v<
            WildcardAccessorResultType<decltype(accessor), Input, std::string>,
            int>);
  }
  {
    auto accessor = [](const Input& input, const std::string& key, int* res) {
      *res = 1;
    };

    static_assert(
        std::is_same_v<
            WildcardAccessorResultType<decltype(accessor), Input, std::string>,
            int>);
  }
  {
    auto accessor = [](const Input& input, const std::string& key,
                       RawBufferFactory*, int* res) { *res = 1; };

    static_assert(
        std::is_same_v<
            WildcardAccessorResultType<decltype(accessor), Input, std::string>,
            int>);
  }
}

TEST(WildcardInputLoaderTest, FromMapNoError) {
  using OInt = OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  using Input = absl::flat_hash_map<std::string, int>;
  auto accessor = [](const Input& input, const std::string& key) -> OInt {
    if (auto it = input.find(key); it != input.end()) {
      return it->second;
    } else {
      return std::nullopt;
    }
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(
                           accessor, absl::ParsedFormat<'s'>("from_map_%s")));
  EXPECT_THAT(input_loader, InputLoaderSupports(
                                {{"from_map_a", oi32}, {"from_map_b", oi32}}));
  EXPECT_THAT(input_loader->SuggestAvailableNames(), ElementsAre("from_map_*"));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OInt>();
  auto b_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_map_a", TypedSlot::FromSlot(a_slot)},
                           {"from_map_b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({{"a", 5}, {"b", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7);

  ASSERT_OK(bound_input_loader({{"a", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 7);
  EXPECT_EQ(alloc.frame().Get(b_slot), std::nullopt);
}

TEST(WildcardInputLoaderTest, AccessorExecutionOrderIsDetemenistic) {
  std::vector<std::string> accessor_calls_order;
  auto accessor = [&](const DummyInput& input, const std::string& key) -> int {
    accessor_calls_order.push_back(key);
    return 1;
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<DummyInput>::Build(accessor));
  EXPECT_THAT(input_loader, InputLoaderSupports({{"a", GetQType<int>()},
                                                 {"b", GetQType<int>()},
                                                 {"c", GetQType<int>()}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<int>();
  auto c_slot = layout_builder.AddSlot<int>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<DummyInput> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                           {"c", TypedSlot::FromSlot(c_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader(DummyInput{}, alloc.frame()));

  EXPECT_THAT(accessor_calls_order, ElementsAre("a", "b", "c"));
}

TEST(WildcardInputLoaderTest, FromMapNoErrorName2KeyFn) {
  using OInt = OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  using Input = absl::flat_hash_map<std::string, int>;
  auto accessor = [](const Input& input, const std::string& key) -> OInt {
    if (auto it = input.find(key); it != input.end()) {
      return it->second;
    } else {
      return std::nullopt;
    }
  };
  auto name2key = [](absl::string_view name) -> std::optional<std::string> {
    if (!absl::ConsumePrefix(&name, "from_map_")) {
      return std::nullopt;
    }
    if (name != "a" && name != "b") {
      return std::nullopt;
    }
    return std::string(name);
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(accessor, name2key));
  EXPECT_THAT(input_loader, InputLoaderSupports(
                                {{"from_map_a", oi32}, {"from_map_b", oi32}}));
  EXPECT_THAT(input_loader->GetQTypeOf("from_map_x"), IsNull());

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OInt>();
  auto b_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_map_a", TypedSlot::FromSlot(a_slot)},
                           {"from_map_b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({{"a", 5}, {"b", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7);

  ASSERT_OK(bound_input_loader({{"a", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 7);
  EXPECT_EQ(alloc.frame().Get(b_slot), std::nullopt);
}

TEST(WildcardInputLoaderTest, FromMapOutputArg) {
  using OInt = OptionalValue<int>;
  auto oi32 = GetQType<OInt>();
  using Input = absl::flat_hash_map<std::string, int>;
  auto accessor = [](const Input& input, const std::string& key, OInt* output) {
    if (auto it = input.find(key); it != input.end()) {
      *output = it->second;
    } else {
      *output = std::nullopt;
    }
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(
                           accessor, absl::ParsedFormat<'s'>("from_map_%s")));
  EXPECT_THAT(input_loader, InputLoaderSupports(
                                {{"from_map_a", oi32}, {"from_map_b", oi32}}));
  EXPECT_THAT(input_loader->SuggestAvailableNames(), ElementsAre("from_map_*"));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OInt>();
  auto b_slot = layout_builder.AddSlot<OInt>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_map_a", TypedSlot::FromSlot(a_slot)},
                           {"from_map_b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({{"a", 5}, {"b", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7);

  ASSERT_OK(bound_input_loader({{"a", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 7);
  EXPECT_EQ(alloc.frame().Get(b_slot), std::nullopt);
}

TEST(WildcardInputLoaderTest, FromMapError) {
  auto i32 = GetQType<int32_t>();
  using Input = absl::flat_hash_map<std::string, int>;
  auto accessor = [](const Input& input,
                     const std::string& key) -> absl::StatusOr<int> {
    if (auto it = input.find(key); it != input.end()) {
      return it->second;
    }
    return absl::FailedPreconditionError(
        absl::StrFormat("key `%s` is not found", key));
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(
                           accessor, absl::ParsedFormat<'s'>("from_map_%s")));
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{"from_map_a", i32}, {"from_map_b", i32}}));
  EXPECT_THAT(input_loader->SuggestAvailableNames(), ElementsAre("from_map_*"));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<int>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_map_a", TypedSlot::FromSlot(a_slot)},
                           {"from_map_b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({{"a", 5}, {"b", 7}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7);

  EXPECT_THAT(bound_input_loader({{"a", 7}}, alloc.frame()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("key `b` is not found")));
}

TEST(InputLoaderTest, BufferFactoryPropagated) {
  UnsafeArenaBufferFactory global_factory(1000);
  auto accessor = [&](int input, const std::string& key,
                      RawBufferFactory* factory) -> bool {
    return &global_factory == factory;
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<int>::Build(accessor));
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<bool>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<int> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader(0, alloc.frame(), &global_factory));
  EXPECT_TRUE(alloc.frame().Get(a_slot));
  UnsafeArenaBufferFactory global_factory2(1000);
  ASSERT_OK(bound_input_loader(0, alloc.frame(), &global_factory2));
  EXPECT_FALSE(alloc.frame().Get(a_slot));
}

TEST(InputLoaderTest, BufferFactoryPropagatedOutputArg) {
  UnsafeArenaBufferFactory global_factory(1000);
  auto accessor = [&](int input, const std::string& key,
                      RawBufferFactory* factory,
                      bool* output) { *output = (&global_factory == factory); };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<int>::Build(accessor));
  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<bool>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<int> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader(0, alloc.frame(), &global_factory));
  EXPECT_TRUE(alloc.frame().Get(a_slot));
  UnsafeArenaBufferFactory global_factory2(1000);
  ASSERT_OK(bound_input_loader(0, alloc.frame(), &global_factory2));
  EXPECT_FALSE(alloc.frame().Get(a_slot));
}

TEST(WildcardInputLoaderTest, BuildFromCallbackAccessorFnFromStruct) {
  using Input = std::pair<int, float>;
  auto accessor = [](const Input& input, const std::string& key,
                     WildcardInputLoaderCallback callback) -> absl::Status {
    if (key == "x") {
      return callback(input.first);
    }
    if (key == "y") {
      return callback(TypedRef::FromValue(input.second));
    }
    return absl::FailedPreconditionError(
        absl::StrFormat("key `%s` is not found", key));
  };
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      WildcardInputLoader<Input>::BuildFromCallbackAccessorFn(
          accessor, {{"x", GetQType<int32_t>()}, {"y", GetQType<float>()}}));
  EXPECT_THAT(input_loader, InputLoaderSupports({{"x", GetQType<int32_t>()},
                                                 {"y", GetQType<float>()}}));
  EXPECT_THAT(input_loader->GetQTypeOf("z"), IsNull());
  EXPECT_THAT(input_loader->SuggestAvailableNames(), ElementsAre("x", "y"));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"x", TypedSlot::FromSlot(a_slot)},
                           {"y", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 7.f}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7.f);
}

TEST(WildcardInputLoaderTest, BuildFromCallbackAccessorFnFromMap) {
  auto i32 = GetQType<int32_t>();
  auto f32 = GetQType<float>();
  using Input = absl::flat_hash_map<std::string, TypedValue>;
  auto accessor = [](const Input& input, const std::string& key,
                     WildcardInputLoaderCallback callback) -> absl::Status {
    if (auto it = input.find(key); it != input.end()) {
      return callback(it->second.AsRef());
    }
    return absl::FailedPreconditionError(
        absl::StrFormat("key `%s` is not found", key));
  };
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      WildcardInputLoader<Input>::BuildFromCallbackAccessorFn(
          accessor, {{"a", GetQType<int32_t>()}, {"b", GetQType<float>()}},
          absl::ParsedFormat<'s'>("from_map_%s")));
  EXPECT_THAT(*input_loader,
              InputLoaderSupports({{"from_map_a", i32}, {"from_map_b", f32}}));
  EXPECT_THAT(input_loader->GetQTypeOf("from_map_c"), IsNull());
  EXPECT_THAT(input_loader->SuggestAvailableNames(),
              ElementsAre("from_map_a", "from_map_b"));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<float>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_map_a", TypedSlot::FromSlot(a_slot)},
                           {"from_map_b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader(
      {{"a", TypedValue::FromValue(5)}, {"b", TypedValue::FromValue(7.f)}},
      alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7.f);

  EXPECT_THAT(
      bound_input_loader({{"a", TypedValue::FromValue(5)}}, alloc.frame()),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("key `b` is not found")));

  EXPECT_THAT(bound_input_loader({{"a", TypedValue::FromValue(5.)},
                                  {"b", TypedValue::FromValue(5.f)}},
                                 alloc.frame()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex(".*type does not match.*expected "
                                    "FLOAT64, got INT32.*key: `a`")));
}

TEST(WildcardInputLoaderTest, FromVector) {
  auto i32 = GetQType<int32_t>();
  using Input = std::vector<int>;
  auto accessor = [](const Input& input,
                     const size_t& key) -> absl::StatusOr<int> {
    return key < input.size() ? input[key] : -1;
  };
  auto name2key = [](absl::string_view key) -> std::optional<int64_t> {
    if (!absl::ConsumePrefix(&key, "from_vector_")) {
      return std::nullopt;
    }
    int64_t id;
    if (!absl::SimpleAtoi(key, &id)) {
      return std::nullopt;
    }
    return id;
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(accessor, name2key));
  EXPECT_THAT(input_loader, InputLoaderSupports({{"from_vector_0", i32},
                                                 {"from_vector_1", i32}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<int>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_vector_0", TypedSlot::FromSlot(a_slot)},
                           {"from_vector_1", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 7}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7);

  ASSERT_OK(bound_input_loader({7}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 7);
  EXPECT_EQ(alloc.frame().Get(b_slot), -1);
}

// Examples:

struct SeparateSparsityInput {
  absl::flat_hash_set<std::string> presents;
  absl::flat_hash_map<std::string, float> values;
};

TEST(WildcardInputLoaderTest, FromTwoMapsSeparateSparsity) {
  auto of32 = GetOptionalQType<float>();
  using Input = SeparateSparsityInput;
  auto accessor =
      [](const Input& input,
         const std::string& key) -> absl::StatusOr<OptionalValue<float>> {
    if (!input.presents.contains(key)) {
      return std::nullopt;
    }
    if (auto it = input.values.find(key); it != input.values.end()) {
      return {it->second};
    }
    return std::nullopt;
  };
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(
                           accessor, absl::ParsedFormat<'s'>("from_map_%s")));
  EXPECT_THAT(input_loader, InputLoaderSupports(
                                {{"from_map_a", of32}, {"from_map_b", of32}}));
  EXPECT_THAT(input_loader->SuggestAvailableNames(), ElementsAre("from_map_*"));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<OptionalValue<float>>();
  auto b_slot = layout_builder.AddSlot<OptionalValue<float>>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<Input> bound_input_loader,
                       input_loader->Bind({
                           {"from_map_a", TypedSlot::FromSlot(a_slot)},
                           {"from_map_b", TypedSlot::FromSlot(b_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({{"a", "b"}, {{"a", 5.f}, {"b", 7.f}}},
                               alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5.f);
  EXPECT_EQ(alloc.frame().Get(b_slot), 7.f);

  ASSERT_OK(
      bound_input_loader({{"a"}, {{"a", 5.f}, {"b", 7.f}}}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5.f);
  EXPECT_EQ(alloc.frame().Get(b_slot), std::nullopt);
}

}  // namespace
}  // namespace arolla
