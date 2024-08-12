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
#include "arolla/qexpr/operators/bool/logic.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;

using OB = OptionalValue<bool>;
using OI = OptionalValue<int64_t>;

constexpr auto NA = std::nullopt;

#define EXPECT_OPERATOR_RESULT_IS(op_name, lhs, rhs, result)                  \
  do {                                                                        \
    EXPECT_THAT(InvokeOperator<OB>(op_name, lhs, rhs), IsOkAndHolds(result)); \
    if (lhs.present && rhs.present) {                                         \
      EXPECT_THAT(InvokeOperator<bool>(op_name, lhs.value, rhs.value),        \
                  IsOkAndHolds(result.value));                                \
    }                                                                         \
  } while (false)

class LogicOperatorsTest : public ::testing::Test {
  void SetUp() final { InitArolla(); }
};

TEST_F(LogicOperatorsTest, LogicalAnd) {
  // Test full logic table.
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{true}, OB{true}, OB{true});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{true}, OB{false}, OB{false});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{true}, OB{}, OB{});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{false}, OB{true}, OB{false});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{false}, OB{false},
                            OB{false});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{false}, OB{}, OB{false});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{}, OB{true}, OB{});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{}, OB{false}, OB{false});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_and", OB{}, OB{}, OB{});

  // Test lift to DenseArray
  EXPECT_THAT(InvokeOperator<DenseArray<bool>>(
                  "bool.logical_and", CreateDenseArray<bool>({true, false}),
                  CreateDenseArray<bool>({true, NA})),
              IsOkAndHolds(ElementsAre(true, false)));
}

TEST_F(LogicOperatorsTest, LogicalOr) {
  // Test full logic table.
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{true}, OB{true}, OB{true});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{true}, OB{false}, OB{true});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{true}, OB{}, OB{true});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{false}, OB{true}, OB{true});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{false}, OB{false}, OB{false});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{false}, OB{}, OB{});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{}, OB{true}, OB{true});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{}, OB{false}, OB{});
  EXPECT_OPERATOR_RESULT_IS("bool.logical_or", OB{}, OB{}, OB{});

  // Test lift to DenseArray
  EXPECT_THAT(InvokeOperator<DenseArray<bool>>(
                  "bool.logical_or",
                  CreateDenseArray<bool>(
                      {true, true, true, false, false, false, NA, NA, NA}),
                  CreateDenseArray<bool>(
                      {true, false, NA, true, false, NA, true, false, NA})),
              IsOkAndHolds(ElementsAre(true, true, true, true, false, NA, true,
                                       NA, NA)));
}

TEST_F(LogicOperatorsTest, LogicalNot) {
  EXPECT_THAT(InvokeOperator<bool>("bool.logical_not", true),
              IsOkAndHolds(false));
  EXPECT_THAT(InvokeOperator<bool>("bool.logical_not", false),
              IsOkAndHolds(true));
  EXPECT_THAT(InvokeOperator<OB>("bool.logical_not", OB{}), IsOkAndHolds(OB{}));
}

TEST_F(LogicOperatorsTest, LogicalIf) {
  EXPECT_THAT(
      InvokeOperator<OI>("bool.logical_if", OB{true}, OI{1}, OI{2}, OI{3}),
      IsOkAndHolds(1));
  EXPECT_THAT(
      InvokeOperator<OI>("bool.logical_if", OB{true}, OI{}, OI{2}, OI{3}),
      IsOkAndHolds(std::nullopt));
  EXPECT_THAT(
      InvokeOperator<OI>("bool.logical_if", OB{false}, OI{1}, OI{2}, OI{3}),
      IsOkAndHolds(2));
  EXPECT_THAT(
      InvokeOperator<OI>("bool.logical_if", OB{false}, OI{1}, OI{}, OI{3}),
      IsOkAndHolds(std::nullopt));
  EXPECT_THAT(InvokeOperator<OI>("bool.logical_if", OB{}, OI{1}, OI{2}, OI{3}),
              IsOkAndHolds(3));
  EXPECT_THAT(InvokeOperator<OI>("bool.logical_if", OB{}, OI{1}, OI{2}, OI{}),
              IsOkAndHolds(std::nullopt));
}

TEST_F(LogicOperatorsTest, LogicalIfOnLambdas) {
  auto lambda = [](auto x) { return [x]() { return x; }; };
  auto no_call_lambda = [](auto x) {
    return [x]() {
      LOG(FATAL) << "Lambda shouldn't be called. " << x;
      return x;
    };
  };
  EXPECT_THAT(LogicalIfOp()(OB{true}, OI{1}, OI{2}, OI{3}), Eq(OI{1}));
  // first argument lambda
  EXPECT_THAT(LogicalIfOp()(OB{true}, lambda(OI{1}), OI{2}, OI{3}), Eq(OI{1}));
  EXPECT_THAT(LogicalIfOp()(OB{false}, no_call_lambda(OI{1}), OI{2}, OI{3}),
              Eq(OI{2}));
  EXPECT_THAT(LogicalIfOp()(OB{}, no_call_lambda(OI{1}), OI{2}, OI{3}),
              Eq(OI{3}));

  // second argument lambda
  EXPECT_THAT(LogicalIfOp()(OB{true}, OI{1}, no_call_lambda(OI{2}), OI{3}),
              Eq(OI{1}));
  EXPECT_THAT(LogicalIfOp()(OB{false}, OI{1}, lambda(OI{2}), OI{3}), Eq(OI{2}));
  EXPECT_THAT(LogicalIfOp()(OB{}, OI{1}, no_call_lambda(OI{2}), OI{3}),
              Eq(OI{3}));

  // third argument lambda
  EXPECT_THAT(LogicalIfOp()(OB{true}, OI{1}, OI{2}, no_call_lambda(OI{3})),
              Eq(OI{1}));
  EXPECT_THAT(LogicalIfOp()(OB{false}, OI{1}, OI{2}, no_call_lambda(OI{3})),
              Eq(OI{2}));
  EXPECT_THAT(LogicalIfOp()(OB{}, OI{1}, OI{2}, lambda(OI{3})), Eq(OI{3}));

  // 1,2 arguments lambda
  EXPECT_THAT(
      LogicalIfOp()(OB{true}, lambda(OI{1}), no_call_lambda(OI{2}), OI{3}),
      Eq(OI{1}));
  EXPECT_THAT(
      LogicalIfOp()(OB{false}, no_call_lambda(OI{1}), lambda(OI{2}), OI{3}),
      Eq(OI{2}));
  EXPECT_THAT(
      LogicalIfOp()(OB{}, no_call_lambda(OI{1}), no_call_lambda(OI{2}), OI{3}),
      Eq(OI{3}));

  // 1,3 arguments lambda
  EXPECT_THAT(
      LogicalIfOp()(OB{true}, lambda(OI{1}), OI{2}, no_call_lambda(OI{3})),
      Eq(OI{1}));
  EXPECT_THAT(LogicalIfOp()(OB{false}, no_call_lambda(OI{1}), OI{2},
                            no_call_lambda(OI{3})),
              Eq(OI{2}));
  EXPECT_THAT(LogicalIfOp()(OB{}, no_call_lambda(OI{1}), OI{2}, lambda(OI{3})),
              Eq(OI{3}));

  // 2,3 arguments lambda
  EXPECT_THAT(LogicalIfOp()(OB{true}, OI{1}, no_call_lambda(OI{2}),
                            no_call_lambda(OI{3})),
              Eq(OI{1}));
  EXPECT_THAT(
      LogicalIfOp()(OB{false}, OI{1}, lambda(OI{2}), no_call_lambda(OI{3})),
      Eq(OI{2}));
  EXPECT_THAT(LogicalIfOp()(OB{}, OI{1}, no_call_lambda(OI{2}), lambda(OI{3})),
              Eq(OI{3}));

  // all arguments lambda
  EXPECT_THAT(LogicalIfOp()(OB{true}, lambda(OI{1}), no_call_lambda(OI{2}),
                            no_call_lambda(OI{3})),
              Eq(OI{1}));
  EXPECT_THAT(LogicalIfOp()(OB{false}, no_call_lambda(OI{1}), lambda(OI{2}),
                            no_call_lambda(OI{3})),
              Eq(OI{2}));
  EXPECT_THAT(LogicalIfOp()(OB{}, no_call_lambda(OI{1}), no_call_lambda(OI{2}),
                            lambda(OI{3})),
              Eq(OI{3}));
}

TEST_F(LogicOperatorsTest, LogicalIfOnLambdasWithError) {
  auto lambda = [](auto x) { return [x]() { return x; }; };
  auto lambda_ok = [](auto x) {
    return [x]() { return absl::StatusOr<decltype(x)>(x); };
  };
  auto lambda_fail = [](auto x) {
    return [x]() {
      return absl::StatusOr<decltype(x)>(absl::UnimplementedError("fake"));
    };
  };
  auto no_call_lambda = [](auto x) {
    return [x]() {
      LOG(FATAL) << "Lambda shouldn't be called. " << x;
      return x;
    };
  };
  auto no_call_lambda_ok = [](auto x) {
    return [x]() {
      LOG(FATAL) << "Lambda shouldn't be called. " << x;
      return absl::StatusOr<decltype(x)>(x);
    };
  };
  auto op = LogicalIfOp();
  EXPECT_THAT(op(OB{true}, OI{1}, OI{2}, OI{3}), Eq(OI{1}));
  // first argument lambda
  EXPECT_THAT(op(OB{true}, lambda_ok(OI{1}), OI{2}, OI{3}),
              IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(op(OB{false}, no_call_lambda_ok(OI{1}), OI{2}, OI{3}),
              IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, no_call_lambda_ok(OI{1}), OI{2}, OI{3}),
              IsOkAndHolds(Eq(OI{3})));

  // second argument lambda
  EXPECT_THAT(op(OB{true}, OI{1}, no_call_lambda_ok(OI{2}), OI{3}),
              IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(op(OB{false}, OI{1}, lambda_ok(OI{2}), OI{3}),
              IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, OI{1}, no_call_lambda_ok(OI{2}), OI{3}),
              IsOkAndHolds(Eq(OI{3})));

  // third argument lambda
  EXPECT_THAT(op(OB{true}, OI{1}, OI{2}, no_call_lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(op(OB{false}, OI{1}, OI{2}, no_call_lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, OI{1}, OI{2}, lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{3})));

  // 1,2 arguments lambda
  EXPECT_THAT(op(OB{true}, lambda_ok(OI{1}), no_call_lambda(OI{2}), OI{3}),
              IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(op(OB{false}, no_call_lambda(OI{1}), lambda_ok(OI{2}), OI{3}),
              IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, no_call_lambda(OI{1}), no_call_lambda_ok(OI{2}), OI{3}),
              IsOkAndHolds(Eq(OI{3})));

  // 1,3 arguments lambda
  EXPECT_THAT(op(OB{true}, lambda_ok(OI{1}), OI{2}, no_call_lambda(OI{3})),
              IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(
      op(OB{false}, no_call_lambda(OI{1}), OI{2}, no_call_lambda_ok(OI{3})),
      IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, no_call_lambda(OI{1}), OI{2}, lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{3})));

  // 2,3 arguments lambda
  EXPECT_THAT(
      op(OB{true}, OI{1}, no_call_lambda_ok(OI{2}), no_call_lambda(OI{3})),
      IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(op(OB{false}, OI{1}, lambda(OI{2}), no_call_lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, OI{1}, no_call_lambda_ok(OI{2}), lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{3})));

  // all arguments lambda
  EXPECT_THAT(op(OB{true}, lambda_ok(OI{1}), no_call_lambda(OI{2}),
                 no_call_lambda(OI{3})),
              IsOkAndHolds(Eq(OI{1})));
  EXPECT_THAT(op(OB{false}, no_call_lambda_ok(OI{1}), lambda(OI{2}),
                 no_call_lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{2})));
  EXPECT_THAT(op(OB{}, no_call_lambda_ok(OI{1}), no_call_lambda_ok(OI{2}),
                 lambda_ok(OI{3})),
              IsOkAndHolds(Eq(OI{3})));

  // errors
  EXPECT_THAT(op(OB{true}, lambda_fail(OI{1}), no_call_lambda(OI{2}),
                 no_call_lambda(OI{3})),
              StatusIs(absl::StatusCode::kUnimplemented, "fake"));
  EXPECT_THAT(op(OB{false}, no_call_lambda(OI{1}), lambda_fail(OI{2}),
                 no_call_lambda(OI{3})),
              StatusIs(absl::StatusCode::kUnimplemented, "fake"));
  EXPECT_THAT(op(OB{}, no_call_lambda_ok(OI{1}), no_call_lambda(OI{2}),
                 lambda_fail(OI{3})),
              StatusIs(absl::StatusCode::kUnimplemented, "fake"));
}

TEST_F(LogicOperatorsTest, LogicalIfDenseArray) {
  EXPECT_THAT(InvokeOperator<DenseArray<int64_t>>(
                  "bool.logical_if",
                  CreateDenseArray<bool>(
                      {true, true, false, false, std::nullopt, std::nullopt}),
                  CreateDenseArray<int64_t>(
                      {1, std::nullopt, 1, std::nullopt, 1, std::nullopt}),
                  CreateDenseArray<int64_t>(
                      {2, std::nullopt, 2, std::nullopt, 2, std::nullopt}),
                  CreateDenseArray<int64_t>(
                      {3, std::nullopt, 3, std::nullopt, 3, std::nullopt})),
              IsOkAndHolds(ElementsAre(1, std::nullopt, 2, std::nullopt, 3,
                                       std::nullopt)));
}

TEST_F(LogicOperatorsTest, LogicalIfDenseArrayWithScalars) {
  EXPECT_THAT(InvokeOperator<DenseArray<int64_t>>(
                  "bool.logical_if",
                  CreateDenseArray<bool>({true, false, std::nullopt}), OI{1},
                  OI{2}, OI{3}),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
}

}  // namespace
}  // namespace arolla
