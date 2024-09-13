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
#include "arolla/array/array.h"

#include <optional>
#include <type_traits>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/edge.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/lift_to_optional_operator.h"
#include "arolla/qexpr/lifting.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/array/lifter.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/qexpr/operators/testing/accumulators.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::testing {

using ::absl_testing::IsOkAndHolds;
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

TEST(LifterTest, SimpleCase) {
  Array<int> arr1 = CreateArray<int>({1, {}, 2, 3});
  Array<int> arr2 = CreateArray<int>({3, 6, {}, 2});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = ArrayPointwiseLifter<TemplatedAddFn, meta::type_list<int, int>>();
  ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, arr1, arr2));

  EXPECT_THAT(res, ElementsAre(4, std::nullopt, std::nullopt, 5));
}

struct LogicalOrOp {
  using run_on_missing = std::true_type;

  bool operator()(bool lhs, bool rhs) const { return lhs || rhs; }

  OptionalValue<bool> operator()(const OptionalValue<bool>& lhs,
                                 const OptionalValue<bool>& rhs) const {
    if (lhs.present) {
      return lhs.value ? true : rhs;
    } else if (rhs.present) {
      return rhs.value ? true : lhs;
    } else {
      // Both inputs are missing. Should we return lhs here?
      return OptionalValue<bool>{};
    }
  }
};

TEST(LifterTest, OptionalBoolResultArrays) {
  Array<bool> arr1 = CreateArray<bool>({true, {}, false, true, {}});
  Array<bool> arr2 = CreateArray<bool>({false, true, {}, true, {}});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op =
      ArrayPointwiseLifter<LogicalOrOp,
                           meta::type_list<::arolla::OptionalValue<bool>,
                                           ::arolla::OptionalValue<bool>>>();
  ASSERT_OK_AND_ASSIGN(Array<bool> res, op(&ctx, arr1, arr2));

  EXPECT_THAT(res, ElementsAre(true, true, std::nullopt, true, std::nullopt));
}

TEST(LifterTest, OptionalBoolResultArrayAndConst) {
  Array<bool> arr1 = Array<bool>(5, std::nullopt);
  Array<bool> arr2 = CreateArray<bool>({false, true, {}, true, {}});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op =
      ArrayPointwiseLifter<LogicalOrOp,
                           meta::type_list<::arolla::OptionalValue<bool>,
                                           ::arolla::OptionalValue<bool>>>();
  ASSERT_OK_AND_ASSIGN(Array<bool> res, op(&ctx, arr1, arr2));

  EXPECT_THAT(
      res, ElementsAre(std::nullopt, true, std::nullopt, true, std::nullopt));
}

TEST(LifterTest, OptionalBoolResultConstAndConst) {
  std::vector<OptionalValue<bool>> cases = {std::nullopt, true, false};
  for (OptionalValue<bool> x : cases) {
    for (OptionalValue<bool> y : cases) {
      Array<bool> arr1 = Array<bool>(1, x);
      Array<bool> arr2 = Array<bool>(1, y);

      FrameLayout frame_layout;
      RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
      EvaluationContext ctx(root_ctx);
      auto op = ArrayPointwiseLifter<
          LogicalOrOp, meta::type_list<::arolla::OptionalValue<bool>,
                                       ::arolla::OptionalValue<bool>>>();
      ASSERT_OK_AND_ASSIGN(Array<bool> res, op(&ctx, arr1, arr2));

      EXPECT_THAT(res, ElementsAre(LogicalOrOp()(x, y))) << x << " " << y;
    }
  }
}

TEST(LifterTest, SizeMismatch) {
  Array<int> arr1 = CreateArray<int>({1, {}, 2, 3});
  Array<int> arr2 = CreateArray<int>({3, 6, {}});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = ArrayPointwiseLifter<TemplatedAddFn, meta::type_list<int, int>>();
  EXPECT_THAT(op(&ctx, arr1, arr2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("argument sizes mismatch: (4, 3)")));
}

TEST(LifterTest, UnaryOperation) {
  Array<int> arr = CreateArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op = ArrayPointwiseLifter<TemplatedAddOneFn, meta::type_list<int>>();
  ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, arr));

  EXPECT_THAT(res, ElementsAre(2, std::nullopt, 3, 4));
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

TEST(LifterTest, NonLiftableArg) {
  Array<int> arr = CreateArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);

  auto op = ArrayPointwiseLifter<TemplatedVariadicAddFn<MyInt, int>,
                                 meta::type_list<DoNotLiftTag<MyInt>, int>>();

  ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, MyInt{5}, arr));

  EXPECT_THAT(res, ElementsAre(6, std::nullopt, 7, 8));
}

TEST(LifterTest, NonLiftableArgs) {
  Array<int> arr = CreateArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  {
    auto op = ArrayPointwiseLifter<
        TemplatedVariadicAddFn<MyInt, MyInt, int>,
        meta::type_list<DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>, int>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, MyInt{3}, MyInt{5}, arr));

    EXPECT_THAT(res, ElementsAre(9, std::nullopt, 10, 11));
  }
  {
    auto op = ArrayPointwiseLifter<
        TemplatedVariadicAddFn<MyInt, int, MyInt>,
        meta::type_list<DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, MyInt{3}, arr, MyInt{5}));

    EXPECT_THAT(res, ElementsAre(9, std::nullopt, 10, 11));
  }
  {
    auto op = ArrayPointwiseLifter<
        TemplatedVariadicAddFn<int, MyInt, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, arr, MyInt{3}, MyInt{5}));

    EXPECT_THAT(res, ElementsAre(9, std::nullopt, 10, 11));
  }
  {
    auto op =
        ArrayPointwiseLifter<TemplatedVariadicAddFn<int, MyInt, int>,
                             meta::type_list<int, DoNotLiftTag<MyInt>, int>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, arr, MyInt{3}, arr));

    EXPECT_THAT(res, ElementsAre(5, std::nullopt, 7, 9));
  }
  {
    auto op = ArrayPointwiseLifter<
        TemplatedVariadicAddFn<MyInt, int, MyInt, int>,
        meta::type_list<DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>, int>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res,
                         op(&ctx, MyInt{5}, arr, MyInt{3}, arr));

    EXPECT_THAT(res, ElementsAre(10, std::nullopt, 12, 14));
  }
  {
    auto op = ArrayPointwiseLifter<
        TemplatedVariadicAddFn<int, MyInt, int, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res,
                         op(&ctx, arr, MyInt{3}, arr, MyInt{5}));

    EXPECT_THAT(res, ElementsAre(10, std::nullopt, 12, 14));
  }
  {
    auto op = ArrayPointwiseLifter<
        TemplatedVariadicAddFn<int, MyInt, int, MyInt, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>,
                        DoNotLiftTag<MyInt>>>();
    ASSERT_OK_AND_ASSIGN(Array<int> res,
                         op(&ctx, arr, MyInt{3}, arr, MyInt{5}, MyInt{4}));

    EXPECT_THAT(res, ElementsAre(14, std::nullopt, 16, 18));
  }
}

TEST(LifterTest, ArrayPointwiseLifterOnDenseOp) {
  Array<int> arr = CreateArray<int>({1, {}, 2, 3});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);

  auto op = ArrayPointwiseLifterOnDenseOp<
      DenseArrayLifter<TemplatedAddFn, meta::type_list<int, int>>,
      OptionalLiftedOperator<TemplatedAddFn, meta::type_list<int, int>>,
      meta::type_list<int, int>>();

  ASSERT_OK_AND_ASSIGN(Array<int> res, op(&ctx, arr, arr));

  EXPECT_THAT(res, ElementsAre(2, std::nullopt, 4, 6));
}

TEST(LifterTest, AggTextAccumulator) {
  auto values = CreateArray<Text>(
      {Text("w1"), std::nullopt, Text("w3"), Text("w4"), Text("w5")});
  auto comments =
      CreateArray<Text>({std::nullopt, Text("it is word #2"), std::nullopt,
                         Text("it is word #4"), std::nullopt});

  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, GetHeapBufferFactory());
  EvaluationContext ctx(root_ctx);
  auto op =
      ArrayGroupLifter<AggTextAccumulator, meta::type_list<OptionalValue<Text>>,
                       meta::type_list<Text, OptionalValue<Text>>>();
  ASSERT_OK_AND_ASSIGN(Text res, op(&ctx, Text("prefix:"), values, comments,
                                    ArrayGroupScalarEdge(values.size())));
  EXPECT_EQ(res.view(), "prefix:w1\nw3\nw4 (it is word #4)\nw5\n");
}

TEST(LifterTest, ArrayPresenceAndOp) {
  EXPECT_THAT(InvokeOperator<Array<int>>(
                  "core.presence_and", CreateArray<int>({1, 2, 3}),
                  CreateArray<Unit>({kUnit, std::nullopt, kUnit})),
              IsOkAndHolds(ElementsAre(1, std::nullopt, 3)));
  EXPECT_THAT(InvokeOperator<Array<int>>(
                  "core.presence_and", CreateArray<int>({1, 2, std::nullopt}),
                  CreateArray<Unit>({kUnit, std::nullopt, kUnit})),
              IsOkAndHolds(ElementsAre(1, std::nullopt, std::nullopt)));
  EXPECT_THAT(InvokeOperator<Array<int>>(
                  "core.presence_and", CreateArray<int>({1, 2, std::nullopt}),
                  CreateArray<Unit>({kUnit, kUnit, kUnit})),
              IsOkAndHolds(ElementsAre(1, 2, std::nullopt)));
}

}  // namespace arolla::testing
