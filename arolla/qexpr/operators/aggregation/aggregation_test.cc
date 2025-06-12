// Copyright 2025 Google LLC
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
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::FloatEq;
using ::testing::HasSubstr;

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

struct TestAccumulator : Accumulator<AccumulatorType::kAggregator, int,
                                     meta::type_list<>, meta::type_list<int>> {
  explicit TestAccumulator(int init = 0) : init_val(init) {}
  void Reset() final { res = init_val; };
  void Add(int v) final { res += v; }
  int GetResult() final { return res; }
  int init_val;
  int res;
};

// Subclass of TestAccumulator.
struct TestAccumulatorWithEvalContext : TestAccumulator {
  explicit TestAccumulatorWithEvalContext(EvaluationOptions eval_options,
                                          int init = 0)
      : TestAccumulator(init), eval_options(eval_options) {}

  EvaluationOptions eval_options;
};

TEST(Accumulator, AddN) {
  TestAccumulator acc;
  acc.Reset();
  acc.AddN(10, 5);
  EXPECT_EQ(acc.GetResult(), 50);
}

TEST(OpInterface, CreateAccumulator) {
  EvaluationOptions eval_options;

  TestAccumulator default_accumulator =
      CreateAccumulator<TestAccumulator>(eval_options);
  EXPECT_EQ(default_accumulator.init_val, 0);

  TestAccumulator init_accumulator =
      CreateAccumulator<TestAccumulator>(eval_options, 5);
  EXPECT_EQ(init_accumulator.init_val, 5);
}

TEST(OpInterface, CreateAccumulatorWithEvalOptions) {
  EvaluationOptions eval_options;
  TestAccumulatorWithEvalContext default_accumulator =
      CreateAccumulator<TestAccumulatorWithEvalContext>(eval_options);
  EXPECT_EQ(default_accumulator.init_val, 0);

  TestAccumulatorWithEvalContext init_accumulator =
      CreateAccumulator<TestAccumulatorWithEvalContext>(eval_options, 5);
  EXPECT_EQ(init_accumulator.init_val, 5);
}

TEST(Accumulator, LogicalAdd) {
  // All present true -> true
  // All present true and at least one missing -> missing
  // At least one present false -> false
  LogicalAllAggregator acc;

  acc.Reset();
  EXPECT_EQ(acc.GetResult(), true);

  acc.Reset();
  acc.AddN(2, std::nullopt);
  EXPECT_EQ(acc.GetResult(), std::nullopt);

  acc.Reset();
  acc.AddN(2, std::nullopt);
  acc.Add(false);
  EXPECT_EQ(acc.GetResult(), false);

  acc.Reset();
  acc.Add(std::nullopt);
  acc.AddN(2, true);
  EXPECT_EQ(acc.GetResult(), std::nullopt);

  acc.Reset();
  acc.AddN(2, true);
  EXPECT_EQ(acc.GetResult(), true);
}

TEST(Accumulator, LogicalOr) {
  // All present false -> false
  // All present false and at least one missing -> missing
  // At least one present true -> true
  LogicalAnyAggregator acc;

  acc.Reset();
  EXPECT_EQ(acc.GetResult(), false);

  acc.Reset();
  acc.AddN(2, std::nullopt);
  EXPECT_EQ(acc.GetResult(), std::nullopt);

  acc.Reset();
  acc.AddN(2, std::nullopt);
  acc.Add(false);
  EXPECT_EQ(acc.GetResult(), std::nullopt);

  acc.Reset();
  acc.Add(std::nullopt);
  acc.AddN(2, true);
  EXPECT_EQ(acc.GetResult(), true);

  acc.Reset();
  acc.AddN(2, true);
  EXPECT_EQ(acc.GetResult(), true);
}

TEST(Accumulator, InverseMapping) {
  InverseMappingAccumulator acc;

  // Permutation [1, 3, 2, 0] -> [3, 0, 2, 1]
  acc.Add(1);
  acc.Add(3);
  acc.Add(2);
  acc.Add(0);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), int64_t{3});
  EXPECT_EQ(acc.GetResult(), int64_t{0});
  EXPECT_EQ(acc.GetResult(), int64_t{2});
  EXPECT_EQ(acc.GetResult(), int64_t{1});
  EXPECT_EQ(acc.GetStatus(), absl::OkStatus());

  // [None, 4, 0, None, 2] -> [2, None, 4, None, 1]
  acc.Reset();
  acc.Add(std::nullopt);
  acc.Add(4);
  acc.Add(0);
  acc.Add(std::nullopt);
  acc.Add(2);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), int64_t{2});
  EXPECT_EQ(acc.GetResult(), std::nullopt);
  EXPECT_EQ(acc.GetResult(), int64_t{4});
  EXPECT_EQ(acc.GetResult(), std::nullopt);
  EXPECT_EQ(acc.GetResult(), int64_t{1});
  EXPECT_EQ(acc.GetStatus(), absl::OkStatus());

  // Out of range indices.
  acc.Reset();
  acc.Add(0);
  acc.Add(2);
  acc.FinalizeFullGroup();
  acc.GetResult();
  acc.GetResult();
  EXPECT_THAT(
      acc.GetStatus(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::HasSubstr(
              "unable to compute array.inverse_mapping: invalid permutation, "
              "element 2 is not a valid element of a permutation of size 2")));
  // Accumulator contains same status after Reset.
  acc.Reset();
  EXPECT_THAT(
      acc.GetStatus(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::HasSubstr(
              "unable to compute array.inverse_mapping: invalid permutation, "
              "element 2 is not a valid element of a permutation of size 2")));

  // Duplicate indices.
  acc.Reset();
  acc.Add(0);
  acc.Add(0);
  acc.FinalizeFullGroup();
  acc.GetResult();
  acc.GetResult();
  EXPECT_THAT(
      acc.GetStatus(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "unable to compute array.inverse_mapping: invalid permutation, "
              "element 0 appears twice in the permutation")));
}

TEST(Accumulator, GroupBy) {
  int64_t group_counter = 10;
  GroupByAccumulator<float> acc(&group_counter);

  acc.Reset();
  acc.Add(2.0f);
  EXPECT_EQ(acc.GetResult(), 10);
  acc.Add(3.0f);
  EXPECT_EQ(acc.GetResult(), 11);
  acc.Add(2.0f);
  EXPECT_EQ(acc.GetResult(), 10);

  acc.Reset();
  acc.Add(3.0f);
  EXPECT_EQ(acc.GetResult(), 12);
  acc.Add(2.0f);
  EXPECT_EQ(acc.GetResult(), 13);
  acc.Add(3.0f);
  EXPECT_EQ(acc.GetResult(), 12);
  acc.Add(2.0f);
  EXPECT_EQ(acc.GetResult(), 13);
}

TEST(Accumulator, PermuteInt) {
  ArrayTakeOverAccumulator<int> acc;
  // Simple permutation.
  acc.Add(0, 2);
  acc.Add(1, 0);
  acc.Add(2, 1);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 1);
  EXPECT_EQ(acc.GetStatus(), absl::OkStatus());

  acc.Reset();
  // Missing indices and values.
  acc.Add(10, std::nullopt);
  acc.Add(std::nullopt, 1);
  acc.Add(20, 0);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), std::nullopt);
  EXPECT_EQ(acc.GetResult(), std::nullopt);
  EXPECT_EQ(acc.GetResult(), 10);
  EXPECT_EQ(acc.GetStatus(), absl::OkStatus());

  acc.Reset();
  // Error status.
  acc.Add(0, 0);
  acc.Add(1, 2);
  acc.FinalizeFullGroup();
  acc.GetResult();
  acc.GetResult();
  EXPECT_THAT(acc.GetStatus(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid offsets: 2 is not a valid offset of "
                                 "an array of size 2")));
  acc.Reset();
  // Status is not reset.
  EXPECT_THAT(acc.GetStatus(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid offsets: 2 is not a valid offset of "
                                 "an array of size 2")));
}

TEST(Accumulator, PermuteBytes) {
  ArrayTakeOverAccumulator<Bytes> acc;

  // "the clone war has begun" -> "begun the clone war has"
  // Byte objects need to live until the calls to GetResult are done, so they
  // are allocated in a vector.
  std::vector<std::pair<OptionalValue<Bytes>, OptionalValue<int64_t>>> inputs(
      {{Bytes("the"), 4},
       {Bytes("clone"), 0},
       {Bytes("war"), 1},
       {Bytes("has"), 2},
       {Bytes("begun"), 3}});
  for (const auto& add : inputs) {
    acc.Add(add.first, add.second);
  }
  acc.FinalizeFullGroup();

  EXPECT_EQ(acc.GetResult(), "begun");
  EXPECT_EQ(acc.GetResult(), "the");
  EXPECT_EQ(acc.GetResult(), "clone");
  EXPECT_EQ(acc.GetResult(), "war");
  EXPECT_EQ(acc.GetResult(), "has");
  EXPECT_EQ(acc.GetStatus(), absl::OkStatus());
}

TEST(Accumulator, CDF) {
  WeightedCDFAccumulator<float, float> acc;

  acc.Add(0.1, 0.1);
  acc.Add(0.2, 0.2);
  acc.Add(0.20001, 0.1);
  acc.Add(0.1, 0.2);
  acc.Add(-0.1, 0.3);
  acc.Add(-0.2, 0.1);
  acc.FinalizeFullGroup();
  EXPECT_THAT(acc.GetResult(), FloatEq(0.7));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.9));
  EXPECT_THAT(acc.GetResult(), FloatEq(1));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.7));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.4));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.1));

  acc.Reset();

  acc.Add(1, 1);
  acc.Add(0, 1);
  acc.FinalizeFullGroup();
  EXPECT_THAT(acc.GetResult(), FloatEq(1));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.5));

  acc.Reset();
  // Empty group works.
  acc.FinalizeFullGroup();
}

TEST(Accumulator, CDFBig) {
  WeightedCDFAccumulator<float, float> acc;
  // Tests b/313682569.
  // Accumulation used to break down for > 16777216 entries due to float32.
  for (int i = 0; i < 18000000; ++i) {
    acc.Add(0.0, 1.0);
  }
  for (int i = 0; i < 2000000; ++i) {
    acc.Add(i, 1.0);
  }

  acc.FinalizeFullGroup();
  EXPECT_THAT(acc.GetResult(), FloatEq(0.9));
}

TEST(Accumulator, CDFNanValue) {
  WeightedCDFAccumulator<float, float> acc;

  acc.Add(0.1, 0.1);
  acc.Add(kNaN, 0.2);
  acc.Add(-0.1, 0.3);
  acc.FinalizeFullGroup();
  EXPECT_TRUE(std::isnan(acc.GetResult()));
  EXPECT_TRUE(std::isnan(acc.GetResult()));
  EXPECT_TRUE(std::isnan(acc.GetResult()));

  acc.Reset();

  acc.Add(1, 1);
  acc.Add(0, 1);
  acc.FinalizeFullGroup();
  EXPECT_THAT(acc.GetResult(), FloatEq(1));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.5));

  acc.Reset();
  // Empty group works.
  acc.FinalizeFullGroup();
}

TEST(Accumulator, CDFNanWeight) {
  WeightedCDFAccumulator<float, float> acc;

  acc.Add(0.1, 0.1);
  acc.Add(0.1, kNaN);
  acc.Add(0.1, 0.3);
  acc.FinalizeFullGroup();
  EXPECT_TRUE(std::isnan(acc.GetResult()));
  EXPECT_TRUE(std::isnan(acc.GetResult()));
  EXPECT_TRUE(std::isnan(acc.GetResult()));

  acc.Reset();

  acc.Add(1, 1);
  acc.Add(0, 1);
  acc.FinalizeFullGroup();
  EXPECT_THAT(acc.GetResult(), FloatEq(1));
  EXPECT_THAT(acc.GetResult(), FloatEq(0.5));

  acc.Reset();
  // Empty group works.
  acc.FinalizeFullGroup();
}

TEST(Accumulator, OrdinalRank) {
  OrdinalRankAccumulator<float, int64_t> acc;

  acc.Add(7, 10);
  acc.Add(7, 9);
  acc.Add(1, 7);
  acc.Add(2, 10);
  acc.Add(2, 11);
  acc.Add(2, 10);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 5);
  EXPECT_EQ(acc.GetResult(), 4);
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 1);
  EXPECT_EQ(acc.GetResult(), 3);
  EXPECT_EQ(acc.GetResult(), 2);
}

TEST(Accumulator, OrdinalRank_Descending) {
  OrdinalRankAccumulator<float, int> acc(/*descending=*/true);

  acc.Add(7, 10);
  acc.Add(7, 9);
  acc.Add(kNaN, 10);
  acc.Add(1, 10);
  acc.Add(2, 10);
  acc.Add(kNaN, 10);
  acc.Add(2, 10);
  acc.Add(kNaN, 10);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 1);
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 5);
  EXPECT_EQ(acc.GetResult(), 4);
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 6);
  EXPECT_EQ(acc.GetResult(), 3);
  EXPECT_EQ(acc.GetResult(), 7);
}

TEST(Accumulator, DenseRank) {
  DenseRankAccumulator<int> acc(/*descending=*/false);

  acc.Add(7);
  acc.Add(7);
  acc.Add(1);
  acc.Add(2);
  acc.Add(2);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 1);
  EXPECT_EQ(acc.GetResult(), 1);

  acc.Reset();
  acc.Add(3);
  acc.Add(0);
  acc.Add(2);
  acc.Add(1);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 3);
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 1);
}

TEST(Accumulator, DenseRankWithNan) {
  DenseRankAccumulator<float> acc(/*descending=*/false);

  acc.Add(7);
  acc.Add(2);
  acc.Add(kNaN);
  acc.Add(7);
  acc.Add(1);
  acc.Add(kNaN);
  acc.Add(2);
  acc.FinalizeFullGroup();

  std::set<int64_t> ranks_of_nan;
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 1);
  ranks_of_nan.insert(acc.GetResult());
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 0);
  ranks_of_nan.insert(acc.GetResult());
  EXPECT_EQ(acc.GetResult(), 1);

  // Two NaNs get different ranks because they are not equal to each other.
  EXPECT_EQ(ranks_of_nan, (std::set<int64_t>{3, 4}));
}

TEST(Accumulator, DenseRank_Descending) {
  DenseRankAccumulator<float> acc(/*descending=*/true);

  acc.Add(7);
  acc.Add(7);
  acc.Add(1);
  acc.Add(2);
  acc.Add(2);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 1);
  EXPECT_EQ(acc.GetResult(), 1);

  acc.Reset();
  acc.Add(3);
  acc.Add(0);
  acc.Add(kNaN);
  acc.Add(1);
  acc.FinalizeFullGroup();
  EXPECT_EQ(acc.GetResult(), 0);
  EXPECT_EQ(acc.GetResult(), 2);
  EXPECT_EQ(acc.GetResult(), 3);
  EXPECT_EQ(acc.GetResult(), 1);
}

TEST(Accumulator, AggMedian) {
  MedianAggregator<int> acc;

  EXPECT_EQ(acc.GetResult(), std::nullopt);

  acc.Reset();
  acc.Add(7);
  acc.Add(1);
  acc.Add(1);
  acc.Add(2);
  EXPECT_EQ(acc.GetResult(), 1);

  acc.Reset();
  acc.Add(7);
  acc.Add(1);
  acc.Add(2);
  EXPECT_EQ(acc.GetResult(), 2);
}

TEST(Accumulator, AggMedianNan) {
  MedianAggregator<float> acc;
  acc.Add(7);
  acc.Add(1);
  acc.Add(2);
  acc.Add(kNaN);
  EXPECT_TRUE(std::isnan(acc.GetResult().value));
}

}  // namespace
}  // namespace arolla
