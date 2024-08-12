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
#include "arolla/dense_array/dense_array.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/lifting.h"
#include "arolla/qexpr/operators/dense_array/group_lifter.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/qexpr/operators/testing/accumulators.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"

namespace arolla::testing {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

struct TemplatedAddFn {
  template <typename T>
  T operator()(T a, T b) const {
    return a + b;
  }
};

struct TemplatedAddOneFn {
  template <typename T>
  T operator()(T a) const {
    return a + 1;
  }
};

TEST(Lifter, SimpleCase) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}, 2});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = DenseArrayLifter<TemplatedAddFn, meta::type_list<int, int>>();
  ASSERT_OK_AND_ASSIGN(DenseArray<int> res, op(&ctx, arr1, arr2));

  EXPECT_THAT(res, ElementsAre(4, std::nullopt, std::nullopt, 5));
}

TEST(Lifter, SizeMismatch) {
  DenseArray<int> arr1 = CreateDenseArray<int>({1, {}, 2, 3});
  DenseArray<int> arr2 = CreateDenseArray<int>({3, 6, {}});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = DenseArrayLifter<TemplatedAddFn, meta::type_list<int, int>>();
  EXPECT_THAT(op(&ctx, arr1, arr2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch: (4, 3)")));
}

TEST(Lifter, UnaryOperation) {
  DenseArray<int> arr = CreateDenseArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = DenseArrayLifter<TemplatedAddOneFn, meta::type_list<int>>();
  ASSERT_OK_AND_ASSIGN(DenseArray<int> res, op(&ctx, arr));

  EXPECT_THAT(res, ElementsAre(2, std::nullopt, 3, 4));
}

TEST(Lifter, NonLiftableArg) {
  DenseArray<int> arr = CreateDenseArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = DenseArrayLifter<TemplatedAddFn,
                             meta::type_list<DoNotLiftTag<int>, int>>();
  ASSERT_OK_AND_ASSIGN(DenseArray<int> res, op(&ctx, 5, arr));

  EXPECT_THAT(res, ElementsAre(6, std::nullopt, 7, 8));
}

// Class that is not liftable to array.
struct MyInt {
  int value;

  // Can be added with ints.
  friend int operator+(int x, MyInt y) { return y.value + x; }
};

template <typename... Ts>
struct TemplatedVariadicAddFn {
  int operator()(Ts... vs) const { return (0 + ... + vs); }
};

TEST(Lifter, NonLiftableArgs) {
  DenseArray<int> arr = CreateDenseArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  {
    auto op = DenseArrayLifter<
        TemplatedVariadicAddFn<MyInt, MyInt, int>,
        meta::type_list<DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>, int>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res,
                         op(&ctx, MyInt{3}, MyInt{5}, arr));

    EXPECT_THAT(res, ElementsAre(9, std::nullopt, 10, 11));
  }
  {
    auto op = DenseArrayLifter<
        TemplatedVariadicAddFn<MyInt, int, MyInt>,
        meta::type_list<DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res,
                         op(&ctx, MyInt{3}, arr, MyInt{5}));

    EXPECT_THAT(res, ElementsAre(9, std::nullopt, 10, 11));
  }
  {
    auto op = DenseArrayLifter<
        TemplatedVariadicAddFn<int, MyInt, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res,
                         op(&ctx, arr, MyInt{3}, MyInt{5}));

    EXPECT_THAT(res, ElementsAre(9, std::nullopt, 10, 11));
  }
  {
    auto op =
        DenseArrayLifter<TemplatedVariadicAddFn<int, MyInt, int>,
                         meta::type_list<int, DoNotLiftTag<MyInt>, int>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res, op(&ctx, arr, MyInt{3}, arr));

    EXPECT_THAT(res, ElementsAre(5, std::nullopt, 7, 9));
  }
  {
    auto op = DenseArrayLifter<
        TemplatedVariadicAddFn<MyInt, int, MyInt, int>,
        meta::type_list<DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>, int>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res,
                         op(&ctx, MyInt{5}, arr, MyInt{3}, arr));

    EXPECT_THAT(res, ElementsAre(10, std::nullopt, 12, 14));
  }
  {
    auto op = DenseArrayLifter<
        TemplatedVariadicAddFn<int, MyInt, int, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res,
                         op(&ctx, arr, MyInt{3}, arr, MyInt{5}));

    EXPECT_THAT(res, ElementsAre(10, std::nullopt, 12, 14));
  }
  {
    auto op = DenseArrayLifter<
        TemplatedVariadicAddFn<int, MyInt, int, MyInt, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>,
                        DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(DenseArray<int> res,
                         op(&ctx, arr, MyInt{3}, arr, MyInt{5}, MyInt{4}));

    EXPECT_THAT(res, ElementsAre(14, std::nullopt, 16, 18));
  }
}

TEST(GroupLifter, AggTextAccumulator) {
  auto values = CreateDenseArray<Text>(
      {Text("w1"), std::nullopt, Text("w3"), Text("w4"), Text("w5")});
  auto comments =
      CreateDenseArray<Text>({std::nullopt, Text("it is word #2"), std::nullopt,
                              Text("it is word #4"), std::nullopt});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = DenseArrayGroupLifter<AggTextAccumulator,
                                  meta::type_list<OptionalValue<Text>>,
                                  meta::type_list<Text, OptionalValue<Text>>>();
  ASSERT_OK_AND_ASSIGN(Text res, op(&ctx, Text("prefix:"), values, comments,
                                    DenseArrayGroupScalarEdge(values.size())));
  EXPECT_EQ(res.view(), "prefix:w1\nw3\nw4 (it is word #4)\nw5\n");
}

}  // namespace arolla::testing
