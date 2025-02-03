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
#include "arolla/io/accessors_input_loader.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/testing/matchers.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::InputLoaderSupports;
using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

struct TestStruct {
  int a;
  double b;
};

struct GetAConstRef {
  const int& operator()(const TestStruct& s) const { return s.a; }
};

struct GetBValue {
  double operator()(const TestStruct& s) const { return s.b; }
};

struct GetBValueViaOutputArg {
  void operator()(const TestStruct& s, double* res) const { *res = s.b; }
};

struct GetBValueViaOutputArgWithRawBufferFactory {
  void operator()(const TestStruct& s, RawBufferFactory*, double* res) const {
    *res = s.b;
  }
};

TEST(InputLoaderTest, InputLoaderAccessorResultType) {
  static_assert(
      std::is_same_v<InputLoaderAccessorResultType<GetAConstRef, TestStruct>,
                     int>);
  static_assert(
      std::is_same_v<InputLoaderAccessorResultType<GetBValue, TestStruct>,
                     double>);
  static_assert(
      std::is_same_v<
          InputLoaderAccessorResultType<GetBValueViaOutputArg, TestStruct>,
          double>);
  static_assert(
      std::is_same_v<InputLoaderAccessorResultType<
                         GetBValueViaOutputArgWithRawBufferFactory, TestStruct>,
                     double>);
}

TEST(InputLoaderTest, AccessorsInputLoader) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  auto accessors_tuple = std::make_tuple(
      std::make_pair(std::string("a"), GetAConstRef{}),
      std::make_pair(std::string("b"), GetBValue{}),
      std::make_pair(std::string("b2"), GetBValueViaOutputArg{}),
      std::make_pair(std::string("b3"),
                     GetBValueViaOutputArgWithRawBufferFactory{}));
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      (AccessorsInputLoader<TestStruct, decltype(accessors_tuple)>::Build(
          accessors_tuple)));
  EXPECT_THAT(
      input_loader,
      InputLoaderSupports({{"a", i32}, {"b", f64}, {"b2", f64}, {"b3", f64}}));

  {  // bind all
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<double>();
    auto a_slot = layout_builder.AddSlot<int>();
    layout_builder.AddSlot<char>();
    auto b_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<std::string>();
    auto b2_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<int16_t>();
    auto b3_slot = layout_builder.AddSlot<double>();
    ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                         input_loader->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                             {"b", TypedSlot::FromSlot(b_slot)},
                             {"b2", TypedSlot::FromSlot(b2_slot)},
                             {"b3", TypedSlot::FromSlot(b3_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
    EXPECT_EQ(alloc.frame().Get(a_slot), 5);
    EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
    EXPECT_EQ(alloc.frame().Get(b2_slot), 3.5);
    EXPECT_EQ(alloc.frame().Get(b3_slot), 3.5);
  }
  {  // bind only a
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<std::string>();
    auto a_slot = layout_builder.AddSlot<int>();
    layout_builder.AddSlot<char>();
    ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                         input_loader->Bind({
                             {"a", TypedSlot::FromSlot(a_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
    EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  }
  {  // bind only b
    FrameLayout::Builder layout_builder;
    layout_builder.AddSlot<std::string>();
    auto b_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<char>();
    ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                         input_loader->Bind({
                             {"b", TypedSlot::FromSlot(b_slot)},
                         }));

    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
    EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
  }
}

struct GetWithFactoryWithHelper {
  void operator()(const TestStruct& s, RawBufferFactory*, double* res) const {
    *res = 1;
  }
  double operator()(const TestStruct& s, RawBufferFactory*) const { return 3; }
};

struct GetWithoutFactoryWithHelper {
  void operator()(const TestStruct& s, double* res) const { *res = 2; }
  double operator()(const TestStruct& s) const { return 4; }
};

struct GetWithAllVariantsHelper {
  void operator()(const TestStruct& s, RawBufferFactory*, double* res) const {
    *res = 1;
  }
  double operator()(const TestStruct& s, RawBufferFactory*) const { return 3; }
  void operator()(const TestStruct& s, double* res) const { *res = 2; }
  double operator()(const TestStruct& s) const { return 4; }
};

TEST(InputLoaderTest, AccessorsInputLoaderChooseRightSignature) {
  auto f64 = GetQType<double>();
  auto accessors_tuple = std::make_tuple(
      std::make_pair(std::string("a"), GetWithFactoryWithHelper{}),
      std::make_pair(std::string("b"), GetWithoutFactoryWithHelper{}),
      std::make_pair(std::string("c"), GetWithAllVariantsHelper{}));
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      (AccessorsInputLoader<TestStruct, decltype(accessors_tuple)>::Build(
          accessors_tuple)));
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{"a", f64}, {"b", f64}, {"c", f64}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<double>();
  auto b_slot = layout_builder.AddSlot<double>();
  auto c_slot = layout_builder.AddSlot<double>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                           {"c", TypedSlot::FromSlot(c_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({0, 0}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 1);
  EXPECT_EQ(alloc.frame().Get(b_slot), 2);
  EXPECT_EQ(alloc.frame().Get(c_slot), 1);
}

TEST(InputLoaderTest, AccessorsInputLoaderPartialBind) {
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", GetAConstRef{}, "b", GetBValue{}));
  {  // extra slots are ignored by PartialBind, but not Bind.
    FrameLayout::Builder layout_builder;
    auto ignored_slot = layout_builder.AddSlot<int>();
    auto b_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<char>();
    absl::flat_hash_map<std::string, TypedSlot> slots = {
        {"b", TypedSlot::FromSlot(b_slot)},
        {"ignored", TypedSlot::FromSlot(ignored_slot)}};
    EXPECT_THAT(input_loader->Bind(slots),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("unknown inputs: ignored")));
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->PartialBind(&slots));
    EXPECT_THAT(slots, UnorderedElementsAre(
                           Pair("ignored", TypedSlot::FromSlot(ignored_slot))));
    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
    EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
    EXPECT_EQ(alloc.frame().Get(ignored_slot), 0);
  }
  {  // more slots to test.
    FrameLayout::Builder layout_builder;
    auto ignored_a_slot = layout_builder.AddSlot<int>();
    auto ignored_b_slot = layout_builder.AddSlot<int>();
    auto a_slot = layout_builder.AddSlot<int>();
    auto b_slot = layout_builder.AddSlot<double>();
    layout_builder.AddSlot<char>();
    absl::flat_hash_map<std::string, TypedSlot> slots = {
        {"a", TypedSlot::FromSlot(a_slot)},
        {"b", TypedSlot::FromSlot(b_slot)},
        {"ignored_a", TypedSlot::FromSlot(ignored_a_slot)},
        {"ignored_b", TypedSlot::FromSlot(ignored_b_slot)},
    };
    EXPECT_THAT(input_loader->Bind(slots),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("unknown inputs: ignored_a, ignored_b")));
    ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                         input_loader->PartialBind(&slots));
    EXPECT_THAT(slots,
                UnorderedElementsAre(
                    Pair("ignored_a", TypedSlot::FromSlot(ignored_a_slot)),
                    Pair("ignored_b", TypedSlot::FromSlot(ignored_b_slot))));
    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);

    ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
    EXPECT_EQ(alloc.frame().Get(a_slot), 5);
    EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
    EXPECT_EQ(alloc.frame().Get(ignored_a_slot), 0);
    EXPECT_EQ(alloc.frame().Get(ignored_b_slot), 0);
  }
}

TEST(InputLoaderTest, NameDuplicates) {
  auto accessors_tuple =
      std::make_tuple(std::make_pair(std::string("a"), GetAConstRef{}),
                      std::make_pair(std::string("c"), GetAConstRef{}),
                      std::make_pair(std::string("a"), GetBValue{}),
                      std::make_pair(std::string("b"), GetAConstRef{}),
                      std::make_pair(std::string("c"), GetAConstRef{}));
  using Loader = AccessorsInputLoader<TestStruct, decltype(accessors_tuple)>;
  EXPECT_THAT(Loader::Build(accessors_tuple),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("accessors have duplicated names: a, c")));
}

TEST(InputLoaderTest, Errors) {
  auto accessors_tuple =
      std::make_tuple(std::make_pair(std::string("a"), GetAConstRef{}));
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      (AccessorsInputLoader<TestStruct, decltype(accessors_tuple)>::Build(
          accessors_tuple)));
  {  // wrong type
    FrameLayout::Builder layout_builder;
    auto dslot = layout_builder.AddSlot<double>();
    EXPECT_THAT(
        input_loader
            ->Bind({
                {"a", TypedSlot::FromSlot(dslot)},
            })
            .status(),
        StatusIs(
            absl::StatusCode::kFailedPrecondition,
            HasSubstr(
                "slot types mismatch: a{expected:INT32, actual:FLOAT64}")));
  }
}

TEST(InputLoaderTest, CreateAccessorsInputLoader) {
  auto i32 = GetQType<int32_t>();
  auto f64 = GetQType<double>();
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; },  //
                           "b", [](const TestStruct& s) { return s.b; },  //
                           "c", [](const TestStruct& s) { return s.b * s.b; }));
  EXPECT_THAT(input_loader,
              InputLoaderSupports({{"a", i32}, {"b", f64}, {"c", f64}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<int>();
  auto b_slot = layout_builder.AddSlot<double>();
  auto c_slot = layout_builder.AddSlot<double>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                           {"b", TypedSlot::FromSlot(b_slot)},
                           {"c", TypedSlot::FromSlot(c_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame()));
  EXPECT_EQ(alloc.frame().Get(a_slot), 5);
  EXPECT_EQ(alloc.frame().Get(b_slot), 3.5);
  EXPECT_EQ(alloc.frame().Get(c_slot), 3.5 * 3.5);
}

TEST(InputLoaderTest, AccessorsInputLoaderBufferFactoryPropagated) {
  auto qbool = GetQType<bool>();
  UnsafeArenaBufferFactory global_factory(1000);
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      CreateAccessorsInputLoader<TestStruct>(
          "a", [&global_factory](const TestStruct&, RawBufferFactory* factory) {
            return &global_factory == factory;
          }));
  EXPECT_THAT(input_loader, InputLoaderSupports({{"a", qbool}}));

  FrameLayout::Builder layout_builder;
  auto a_slot = layout_builder.AddSlot<bool>();
  ASSERT_OK_AND_ASSIGN(BoundInputLoader<TestStruct> bound_input_loader,
                       input_loader->Bind({
                           {"a", TypedSlot::FromSlot(a_slot)},
                       }));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);

  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame(), &global_factory));
  EXPECT_TRUE(alloc.frame().Get(a_slot));
  UnsafeArenaBufferFactory global_factory2(1000);
  ASSERT_OK(bound_input_loader({5, 3.5}, alloc.frame(), &global_factory2));
  EXPECT_FALSE(alloc.frame().Get(a_slot));
}

using PairStringFunction = std::pair<std::string, std::function<int(int)>>;
template <size_t>
using PairStringFunctionByInt = PairStringFunction;

template <size_t... Is>
absl::StatusOr<std::unique_ptr<InputLoader<int>>>
CreateAccessorsInputLoaderManyInputs(std::index_sequence<Is...>) {
  using T = std::tuple<PairStringFunctionByInt<Is>...>;
  return CreateAccessorsInputLoaderFromTuple<int>(
      T{PairStringFunction(absl::StrCat(Is), [](int) { return Is; })...});
}

TEST(InputLoaderTest, CreateAccessorsInputLoaderCompilationStressTest) {
  // As at 21.08.2020 tests pass with N=1200 in a minute,
  // compilation fails with N=1500 due to OOM.
  // N=50 to have reasonable compilation time (~10 seconds).
  constexpr size_t N = 50;
  ASSERT_OK_AND_ASSIGN(auto loader, CreateAccessorsInputLoaderManyInputs(
                                        std::make_index_sequence<N>()));
  EXPECT_THAT(loader, InputLoaderSupports({{"1", GetQType<int>()},
                                           {"2", GetQType<int>()},
                                           {"49", GetQType<int>()}}));
}

}  // namespace
}  // namespace arolla
