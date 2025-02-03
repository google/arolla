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
#include "arolla/qexpr/lift_to_optional_operator.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/lifting.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Eq;

struct IntDivOp {
  template <typename T>
  OptionalValue<T> operator()(const T& lhs, const T& rhs) const {
    if (rhs == 0) {
      return std::nullopt;
    }
    return lhs / rhs;
  }
};

TEST(OptionalLiftedOperatorTest, ReturnOptional) {
  using LiftedIntDivOp =
      OptionalLiftedOperator<IntDivOp, meta::type_list<int, int>>;

  EXPECT_THAT(LiftedIntDivOp()(OptionalValue<int>(5), OptionalValue<int>(2)),
              Eq(OptionalValue<int>(2)));

  // Non-optional args, but returns missing.
  EXPECT_THAT(LiftedIntDivOp()(OptionalValue<int>(5), OptionalValue<int>(0)),
              Eq(OptionalValue<int>()));

  // Optional args
  EXPECT_THAT(LiftedIntDivOp()(OptionalValue<int>(), OptionalValue<int>()),
              Eq(OptionalValue<int>()));
  EXPECT_THAT(LiftedIntDivOp()(OptionalValue<int>(2), OptionalValue<int>()),
              Eq(OptionalValue<int>()));
  EXPECT_THAT(LiftedIntDivOp()(OptionalValue<int>(), OptionalValue<int>(2)),
              Eq(OptionalValue<int>()));
}

TEST(OptionalLiftedOperatorTest, NonLiftableArg) {
  using LiftedIntDivOp =
      OptionalLiftedOperator<IntDivOp, meta::type_list<DoNotLiftTag<int>, int>>;

  EXPECT_THAT(LiftedIntDivOp()(5, OptionalValue<int>(2)),
              Eq(OptionalValue<int>(2)));

  // Non-optional arg, but returns missing.
  EXPECT_THAT(LiftedIntDivOp()(5, OptionalValue<int>(0)),
              Eq(OptionalValue<int>()));

  // Optional arg
  EXPECT_THAT(LiftedIntDivOp()(2, OptionalValue<int>()),
              Eq(OptionalValue<int>()));
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
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<MyInt, MyInt, int>,
        meta::type_list<DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>, int>>();
    OptionalValue<int> res = op(MyInt{3}, MyInt{5}, OptionalValue<int>(1));
    EXPECT_THAT(res, Eq(9));
  }
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<MyInt, int, MyInt>,
        meta::type_list<DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>>>();
    OptionalValue<int> res = op(MyInt{3}, OptionalValue<int>(1), MyInt{5});
    EXPECT_THAT(res, Eq(9));
  }
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<int, MyInt, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>>>();
    OptionalValue<int> res = op(OptionalValue<int>(1), MyInt{3}, MyInt{5});
    EXPECT_THAT(res, Eq(9));
  }
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<int, MyInt, int>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int>>();
    OptionalValue<int> res =
        op(OptionalValue<int>(1), MyInt{3}, OptionalValue<int>(1));
    EXPECT_THAT(res, Eq(5));
  }
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<MyInt, int, MyInt, int>,
        meta::type_list<DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>, int>>();
    OptionalValue<int> res =
        op(MyInt{5}, OptionalValue<int>(1), MyInt{3}, OptionalValue<int>(1));
    EXPECT_THAT(res, Eq(10));
  }
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<int, MyInt, int, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>>>();
    OptionalValue<int> res =
        op(OptionalValue<int>(1), MyInt{3}, OptionalValue<int>(1), MyInt{5});
    EXPECT_THAT(res, Eq(10));
  }
  {
    auto op = OptionalLiftedOperator<
        TemplatedVariadicAddFn<int, MyInt, int, MyInt, MyInt>,
        meta::type_list<int, DoNotLiftTag<MyInt>, int, DoNotLiftTag<MyInt>,
                        DoNotLiftTag<MyInt>>>();
    OptionalValue<int> res = op(OptionalValue<int>(1), MyInt{3},
                                OptionalValue<int>(1), MyInt{5}, MyInt{4});
    EXPECT_THAT(res, Eq(14));
  }
}

TEST(Lifter, NonLiftableArgsMissed) {
  auto op = OptionalLiftedOperator<
      TemplatedVariadicAddFn<MyInt, MyInt, int>,
      meta::type_list<DoNotLiftTag<MyInt>, DoNotLiftTag<MyInt>, int>>();
  OptionalValue<int> res = op(MyInt{3}, MyInt{5}, OptionalValue<int>());
  EXPECT_THAT(res, Eq(std::nullopt));
}

struct FailingDivOp {
  template <typename T>
  absl::StatusOr<OptionalValue<T>> operator()(const T& lhs,
                                              const T& rhs) const {
    if (rhs == 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "division by zero");
    }
    return lhs / rhs;
  }
};

TEST(LiftScalarOperatorTest, ReturnStatusOr) {
  using LiftedOp =
      OptionalLiftedOperator<FailingDivOp, meta::type_list<int, int>>;

  EXPECT_THAT(LiftedOp()(OptionalValue<int>(5), OptionalValue<int>(2)),
              IsOkAndHolds(Eq(OptionalValue<int>(2))));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(), OptionalValue<int>(1)),
              IsOkAndHolds(Eq(OptionalValue<int>())));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(1), OptionalValue<int>()),
              IsOkAndHolds(Eq(OptionalValue<int>())));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(1), OptionalValue<int>(0)),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));
}

// Returns an error if divisor is 0 and missing if divisor is less than 0.
struct StrangeDivOp {
  template <typename T>
  absl::StatusOr<OptionalValue<T>> operator()(const T& lhs,
                                              const T& rhs) const {
    if (rhs == 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "division by zero");
    }
    if (rhs < 0) {
      return std::nullopt;
    }
    return lhs / rhs;
  }
};

TEST(LiftScalarOperatorTest, ReturnStatusOrOptional) {
  using LiftedOp =
      OptionalLiftedOperator<StrangeDivOp, meta::type_list<int, int>>;

  EXPECT_THAT(LiftedOp()(OptionalValue<int>(5), OptionalValue<int>(2)),
              IsOkAndHolds(Eq(OptionalValue<int>(2))));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(), OptionalValue<int>(1)),
              IsOkAndHolds(Eq(OptionalValue<int>())));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(1), OptionalValue<int>()),
              IsOkAndHolds(Eq(OptionalValue<int>())));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(1), OptionalValue<int>(-1)),
              IsOkAndHolds(Eq(OptionalValue<int>())));
  EXPECT_THAT(LiftedOp()(OptionalValue<int>(1), OptionalValue<int>(0)),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));
}

// Returns an error if divisor is 0 and missing if divisor is less than 0.
struct DivWithDefaultOp {
  template <typename T>
  absl::StatusOr<T> operator()(T lhs, T rhs,
                               OptionalValue<T> default_result) const {
    if (rhs == 0) {
      if (default_result.present) {
        return default_result.value;
      } else {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "division by zero");
      }
    }
    return lhs / rhs;
  }
};

TEST(LiftScalarOperatorTest, OptionalArguments) {
  using LiftedOp =
      OptionalLiftedOperator<DivWithDefaultOp,
                             meta::type_list<int, int, OptionalValue<int>>>;

  const OptionalValue<int> missing(std::nullopt);
  const OptionalValue<int> one = 1;
  const OptionalValue<int> zero = 0;

  EXPECT_THAT(LiftedOp()(OptionalValue<int>(5), OptionalValue<int>(2), missing),
              IsOkAndHolds(Eq(2)));
  EXPECT_THAT(LiftedOp()(missing, one, missing), IsOkAndHolds(Eq(missing)));
  EXPECT_THAT(LiftedOp()(one, missing, missing), IsOkAndHolds(Eq(missing)));
  EXPECT_THAT(LiftedOp()(one, zero, missing),
              StatusIs(absl::StatusCode::kInvalidArgument, "division by zero"));
  EXPECT_THAT(LiftedOp()(one, zero, one), IsOkAndHolds(Eq(one)));
}

}  // namespace
}  // namespace arolla
