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
#include "arolla/decision_forest/pointwise_evaluation/forest_evaluator.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/decision_forest/testing/test_util.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

constexpr float kInf = std::numeric_limits<float>::infinity();
constexpr auto S = DecisionTreeNodeId::SplitNodeId;
constexpr auto A = DecisionTreeNodeId::AdjustmentId;

const ForestEvaluator::CompilationParams kDefaultEval{
    .enable_regular_eval = true,
    .enable_bitmask_eval = true,
    .enable_single_input_eval = true};
const ForestEvaluator::CompilationParams kRegularEval{
    .enable_regular_eval = true,
    .enable_bitmask_eval = false,
    .enable_single_input_eval = false};
const ForestEvaluator::CompilationParams kBitmaskEval{
    .enable_regular_eval = false,
    .enable_bitmask_eval = true,
    .enable_single_input_eval = false};
const ForestEvaluator::CompilationParams kSingleInputEval{
    .enable_regular_eval = false,
    .enable_bitmask_eval = false,
    .enable_single_input_eval = true};

void FillArgs(FramePtr ctx, int row_id, absl::Span<const TypedSlot> slots) {}

template <typename T, typename... Tn>
void FillArgs(FramePtr frame, int row_id, absl::Span<const TypedSlot> slots,
              const std::vector<OptionalValue<T>>& inputs1,
              const std::vector<OptionalValue<Tn>>&... inputsN) {
  auto slot = slots[0].ToSlot<OptionalValue<T>>().value();
  frame.Set(slot, inputs1[row_id]);
  FillArgs(frame, row_id, slots.subspan(1), inputsN...);
}

// A placeholder implementation for std::source_location, which is not yet
// available on all platforms.
class SourceLocation {
 public:
  SourceLocation(int line, const char* filename)
      : line_(line), file_name_(filename) {}
  const char* file_name() { return file_name_.c_str(); }
  constexpr int line() const { return line_; }

  static SourceLocation current(int line = __builtin_LINE(),
                                const char* file_name = __builtin_FILE()) {
    return SourceLocation(line, file_name);
  }

 private:
  int line_ = 0;
  std::string file_name_;
};

std::string ErrFormat(SourceLocation loc,
                      ForestEvaluator::CompilationParams params,
                      const std::string& msg, int row_id) {
  return absl::StrFormat(
      "%s Test at %s:%d, row_id=%d, params = "
      "{enable_regular_eval=%d, enable_bitmask_eval=%d, "
      "enable_single_input_eval=%d}",
      msg, loc.file_name(), loc.line(), row_id, params.enable_regular_eval,
      params.enable_bitmask_eval, params.enable_single_input_eval);
}

// Compiles given decision forest with given compilation params,
// evaluates it on given input data, and compares with given output.
// Args:
//   loc - Should be SourceLocation::current(). Needed to print debug info if
//       test is failed.
//   expected_outputs - results per test case
//       expected_outputs[I][J] - result for group #J of testcase #I
//   inputs... - each argument corresponds to one of the decision forest inputs,
//       size of each input should be equal to the number of test cases.
template <typename... T>
void TestCases(SourceLocation loc, const DecisionForest& forest,
               absl::Span<const TreeFilter> groups,
               ForestEvaluator::CompilationParams params,
               absl::Span<const std::vector<float>> expected_outputs,
               const std::vector<OptionalValue<T>>&... inputs) {
  ASSERT_TRUE(((expected_outputs.size() == inputs.size()) && ...))
      << absl::StrCat(
             "Input and output vector sizes are different: (",
             absl::StrJoin({expected_outputs.size(), inputs.size()...}, ", "),
             ")");
  std::vector<TypedSlot> input_slots;
  std::vector<ForestEvaluator::Output> outputs;
  FrameLayout::Builder layout_builder;
  CreateSlotsForForest(forest, &layout_builder, &input_slots);
  outputs.reserve(groups.size());
  for (const TreeFilter& filter : groups) {
    outputs.push_back({filter, layout_builder.AddSlot<float>()});
  }
  FrameLayout layout = std::move(layout_builder).Build();

  ASSERT_OK_AND_ASSIGN(
      auto evaluator,
      ForestEvaluator::Compile(forest, input_slots, outputs, params));

  MemoryAllocation alloc(&layout);
  auto frame = alloc.frame();
  for (int i = 0; i < expected_outputs.size(); ++i) {
    FillArgs(frame, i, input_slots, inputs...);
    evaluator.Eval(frame, frame);
    for (int j = 0; j < outputs.size(); ++j) {
      EXPECT_EQ(frame.Get(outputs[j].slot), expected_outputs[i][j])
          << ErrFormat(loc, params, "Incorrect output.", i);
    }
  }
}

void RandomTestAgainstReferenceImplementation(
    SourceLocation loc, std::vector<DecisionTree> trees,
    const std::vector<ForestEvaluator::CompilationParams>& params,
    absl::BitGen* rnd) {
  for (int i = 0; i < trees.size(); ++i) {
    trees[i].tag.submodel_id = i % 4;
  }
  TreeFilter group0{.submodels{0, 3}};
  TreeFilter group1{.submodels{1, 2}};
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));

  std::vector<TypedSlot> input_slots;
  std::vector<ForestEvaluator::Output> outputs;
  FrameLayout::Builder layout_builder;
  CreateSlotsForForest(*forest, &layout_builder, &input_slots);
  outputs.push_back({group0, layout_builder.AddSlot<float>()});
  outputs.push_back({group1, layout_builder.AddSlot<float>()});
  FrameLayout layout = std::move(layout_builder).Build();

  std::vector<ForestEvaluator> evaluators;
  for (auto p : params) {
    ASSERT_OK_AND_ASSIGN(auto evaluator, ForestEvaluator::Compile(
                                             *forest, input_slots, outputs, p));
    evaluators.push_back(std::move(evaluator));
  }

  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();
  for (int item_id = 0; item_id < 15; ++item_id) {
    for (auto slot : input_slots) {
      ASSERT_OK(FillWithRandomValue(slot, frame, rnd,
                                    /*missed_prob=*/0.25));
    }
    float reference_implementation_res0 =
        DecisionForestNaiveEvaluation(*forest, frame, input_slots, group0);
    float reference_implementation_res1 =
        DecisionForestNaiveEvaluation(*forest, frame, input_slots, group1);
    for (int eval_id = 0; eval_id < evaluators.size(); ++eval_id) {
      const ForestEvaluator& evaluator = evaluators[eval_id];

      // Clear previous evaluator results
      frame.Set(outputs[0].slot, 0.0f);
      frame.Set(outputs[1].slot, 0.0f);

      // Test Eval(frame, output_span)
      evaluator.Eval(frame, frame);
      EXPECT_FLOAT_EQ(reference_implementation_res0, frame.Get(outputs[0].slot))
          << ErrFormat(loc, params[eval_id], "Incorrect output #0 in Eval",
                       item_id);
      EXPECT_FLOAT_EQ(reference_implementation_res1, frame.Get(outputs[1].slot))
          << ErrFormat(loc, params[eval_id], "Incorrect output #1 in Eval",
                       item_id);
    }
  }
}

TEST(ForestEvaluator, GroupsValidation) {
  std::vector<DecisionTree> trees(3);
  trees[0].tag.submodel_id = 3;
  trees[0].adjustments = {1.0};
  trees[1].tag.submodel_id = 2;
  trees[1].adjustments = {1.0};
  trees[2].tag.submodel_id = 1;
  trees[2].adjustments = {1.0};
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));

  EXPECT_THAT(ForestEvaluator::Compile(*forest, {}, {}).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("at least one output is expected")));

  auto fake_slot = FrameLayout::Slot<float>::UnsafeUninitializedSlot();
  EXPECT_THAT(
      ForestEvaluator::Compile(
          *forest, {},
          {ForestEvaluator::Output{{.submodels = {1, 3}}, fake_slot},
           ForestEvaluator::Output{{.submodels = {2, 3}}, fake_slot}})
          .status(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "intersection of groups for outputs #0 and #1 is not empty")));

  EXPECT_OK(ForestEvaluator::Compile(
                *forest, {},
                {ForestEvaluator::Output{{.submodels = {1, 3}}, fake_slot},
                 ForestEvaluator::Output{{.submodels = {2}}, fake_slot}})
                .status());

  EXPECT_OK(ForestEvaluator::Compile(
                *forest, {},  // submodel_id = 3 is not used
                {ForestEvaluator::Output{{.submodels = {1}}, fake_slot},
                 ForestEvaluator::Output{{.submodels = {2}}, fake_slot}})
                .status());
}

TEST(ForestEvaluator, EmptyForest) {
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({}));
  for (auto params :
       {kDefaultEval, kRegularEval, kBitmaskEval, kSingleInputEval}) {
    TestCases<>(SourceLocation::current(), *forest,
                /*groups=*/{{.submodels = {0}}, {.submodels = {1}}}, params,
                /*expected_outputs=*/{{0.0, 0.0}});
  }
}

TEST(ForestEvaluator, Constant) {
  std::vector<DecisionTree> trees(2);
  trees[0].tag = {.submodel_id = 0};
  trees[0].adjustments = {1.5};
  trees[1].tag = {.submodel_id = 1};
  trees[1].adjustments = {2.5};
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));
  for (auto params : {kDefaultEval, kRegularEval, kBitmaskEval}) {
    TestCases<>(SourceLocation::current(), *forest,
                /*groups=*/{{.submodels = {0}}, {.submodels = {1}}}, params,
                /*expected_outputs=*/{{1.5, 2.5}});
  }
}

TEST(ForestEvaluator, SmallForest) {
  std::vector<DecisionTree> trees(2);
  trees[0].tag = {.submodel_id = 0};
  trees[0].adjustments = {0.5, 1.5, 2.5, 3.5};
  trees[0].split_nodes = {
      {S(1), S(2), IntervalSplit(0, 1.5, kInf)},
      {A(0), A(2), SetOfValuesSplit<int64_t>(1, {1, 2}, false)},
      {A(1), A(3), IntervalSplit(0, -kInf, 10)}};
  trees[1].tag = {.submodel_id = 1};
  trees[1].adjustments = {-1.0, 1.0};
  trees[1].split_nodes = {{A(0), A(1), IntervalSplit(0, 1, 5)}};
  ASSERT_OK_AND_ASSIGN(auto forest,
                       DecisionForest::FromTrees(std::move(trees)));

  for (auto params : {kDefaultEval, kRegularEval, kBitmaskEval}) {
    TestCases<float, int64_t>(
        SourceLocation::current(), *forest,
        /*groups=*/{{.submodels = {0}}, {.submodels = {1}}}, params,
        /*expected_outputs=*/
        {{0.5, -1},
         {2.5, -1},
         {2.5, 1},
         {3.5, 1},
         {3.5, -1},
         {1.5, -1},
         {2.5, -1},
         {0.5, -1}},
        /*input #0*/ {0, 0, 1.2, 1.6, 7.0, 13.5, NAN, {}},
        /*input #1*/ {3, 1, 1, 1, 1, 1, 1, {}});
  }
}

TEST(ForestEvaluator, RangesSplits) {
  DecisionTree tree;
  tree.split_nodes = {{S(2), S(1), IntervalSplit(0, -1.0, 1.0)},
                      {A(1), A(2), IntervalSplit(0, 0.5, 0.5)},
                      {A(0), A(3), IntervalSplit(0, 2.5, 3.5)}};
  tree.adjustments = {0.0, 1.0, 2.0, 3.0};
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({tree}));
  for (auto params :
       {kDefaultEval, kRegularEval, kBitmaskEval, kSingleInputEval}) {
    TestCases<float>(
        SourceLocation::current(), *forest, /*groups=*/{{}}, params,
        /*expected_outputs=*/{{0}, {0}, {0}, {1}, {2}, {3}, {3}, {3}},
        /*input #0*/ {{}, -5, 5, -1, 0.5, 2.5, 3.0, 3.5});
  }
}

TEST(ForestEvaluator, EqualSplits) {
  DecisionTree tree;
  tree.split_nodes = {{S(2), S(1), IntervalSplit(0, 1.0, 1.0)},
                      {A(1), A(2), IntervalSplit(1, 5.0, 5.0)},
                      {A(0), A(3), IntervalSplit(1, -5.0, -5.0)}};
  tree.adjustments = {0.0, 1.0, 2.0, 3.0};
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({tree}));
  for (auto params : {kDefaultEval, kRegularEval, kBitmaskEval}) {
    TestCases<float, float>(
        SourceLocation::current(), *forest, /*groups=*/{{}}, params,
        /*expected_outputs=*/{{0}, {0}, {0}, {1}, {1}, {2}, {3}, {3}},
        /*input #0*/ {{}, 0.0, -5.0, 1.0, 1.0, 1.0, 0.0, {}},
        /*input #1*/ {{}, {}, {}, {}, -5.0, +5.0, -5.0, -5.0});
  }
}

TEST(ForestEvaluator, BytesInput) {
  DecisionTree tree;
  tree.split_nodes = {
      {A(0), A(1), SetOfValuesSplit<Bytes>(0, {Bytes("X")}, false)}};
  tree.adjustments = {0.0, 1.0};
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({tree}));
  for (auto params : {kDefaultEval, kRegularEval}) {
    TestCases<Bytes>(SourceLocation::current(), *forest, /*groups=*/{{}},
                     params,
                     /*expected_outputs=*/{{0}, {1}, {0}},
                     /*input #0*/ {{}, Bytes("X"), Bytes("Y")});
  }
}

TEST(ForestEvaluator, BitmaskNotPossible) {
  absl::BitGen rnd;
  auto forest =
      CreateRandomForest(&rnd, /*num_features=*/10, /*interactions=*/true,
                         /*min_num_splits=*/70, /*max_num_splits=*/70,
                         /*num_trees=*/1);
  std::vector<TypedSlot> slots;
  FrameLayout::Builder layout_builder;
  CreateSlotsForForest(*forest, &layout_builder, &slots);
  EXPECT_THAT(
      SimpleForestEvaluator::Compile(*forest, slots, kBitmaskEval),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("No suitable evaluator. Use enable_regular_eval=true.")));
}

TEST(ForestEvaluator, SingleInputEvalNotPossible) {
  DecisionTree tree;
  tree.split_nodes = {{S(2), S(1), IntervalSplit(0, 1.0, 1.0)},
                      {A(1), A(2), IntervalSplit(1, 5.0, 5.0)},
                      {A(0), A(3), IntervalSplit(1, -5.0, -5.0)}};
  tree.adjustments = {0.0, 1.0, 2.0, 3.0};
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({tree}));
  std::vector<TypedSlot> slots;
  FrameLayout::Builder layout_builder;
  CreateSlotsForForest(*forest, &layout_builder, &slots);
  EXPECT_THAT(
      SimpleForestEvaluator::Compile(*forest, slots, kSingleInputEval),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("No suitable evaluator. Use enable_regular_eval=true.")));
}

TEST(ForestEvaluator, ObliviousTree) {
  DecisionTree tree;
  std::vector<std::shared_ptr<SplitCondition>> conditions = {
      IntervalSplit(0, -5, 5),
      IntervalSplit(1, 0, kInf),
      SetOfValuesSplit<int64_t>(2, {1, 2}, false),
      IntervalSplit(3, -kInf, 3.0),
      SetOfValuesSplit<int64_t>(4, {4, 2}, true),
      IntervalSplit(5, -1, 7),
      IntervalSplit(6, -kInf, -5)};
  int layer_size = 1;
  for (int layer = 0; layer < conditions.size(); ++layer) {
    int layer_offset = tree.split_nodes.size() + layer_size;
    for (int i = 0; i < layer_size; ++i) {
      auto left =
          (layer == conditions.size() - 1) ? A(i * 2) : S(layer_offset + i * 2);
      auto right = (layer == conditions.size() - 1)
                       ? A(i * 2 + 1)
                       : S(layer_offset + i * 2 + 1);
      tree.split_nodes.push_back({left, right, conditions[layer]});
    }
    layer_size *= 2;
  }
  for (int i = 0; i < layer_size; ++i) {
    tree.adjustments.push_back(i);
  }
  ASSERT_OK_AND_ASSIGN(auto forest, DecisionForest::FromTrees({tree}));
  for (auto params : {kDefaultEval, kRegularEval, kBitmaskEval}) {
    TestCases<float, float, int64_t, float, int64_t, float, float>(
        SourceLocation::current(), *forest, /*groups=*/{{}}, params,
        /*expected_outputs=*/{{58}, {86}, {12}, {39}, {112}},
        /*input #0*/ {{}, 3, -7, 15, -4},
        /*input #1*/ {10, -1, {}, 25, 1},
        /*input #2*/ {2, 1, 3, {}, 1},
        /*input #3*/ {0, {}, -5, 8, 14},
        /*input #4*/ {1, 2, {}, 4, 5},
        /*input #5*/ {0, 4, -3, 7, {}},
        /*input #6*/ {10, 5, -3, -8, {}});
  }
}

TEST(ForestEvaluator, TestAgainstReferenceOnSmallTrees) {
  absl::BitGen rnd;

  std::vector<QTypePtr> types;
  for (int input_id = 0; input_id < 10; input_id++) {
    types.push_back(GetOptionalQType<float>());
  }
  for (int input_id = 10; input_id < 15; input_id++) {
    types.push_back(GetOptionalQType<int64_t>());
  }

  for (int iteration = 0; iteration < 10; ++iteration) {
    std::vector<DecisionTree> trees;
    for (int i = 0; i < 10; ++i) {
      int num_splits = absl::Uniform<int32_t>(rnd, 0, 32);
      trees.push_back(
          CreateRandomTree(&rnd, /*interactions=*/true, num_splits, &types));
    }

    RandomTestAgainstReferenceImplementation(
        SourceLocation::current(), trees,
        {kDefaultEval, kRegularEval, kBitmaskEval}, &rnd);
  }
}

TEST(ForestEvaluator, TestAgainstReferenceOnSingleInputTrees) {
  absl::BitGen rnd;

  std::vector<QTypePtr> types;
  for (int input_id = 0; input_id < 10; input_id++) {
    types.push_back(GetOptionalQType<float>());
  }
  for (int input_id = 10; input_id < 15; input_id++) {
    types.push_back(GetOptionalQType<int64_t>());
  }

  for (int iteration = 0; iteration < 10; ++iteration) {
    std::vector<DecisionTree> trees;
    for (int i = 0; i < 10; ++i) {
      int num_splits = absl::Uniform<int32_t>(rnd, 1, 1024);
      trees.push_back(
          CreateRandomTree(&rnd, /*interactions=*/false, num_splits, &types));
    }

    RandomTestAgainstReferenceImplementation(
        SourceLocation::current(), trees,
        {kDefaultEval, kRegularEval, kSingleInputEval}, &rnd);
  }
}

TEST(ForestEvaluator, TestAgainstReference) {
  absl::BitGen rnd;

  std::vector<QTypePtr> types;
  for (int feature_id = 0; feature_id < 10; feature_id++) {
    types.push_back(GetOptionalQType<float>());
  }
  for (int feature_id = 10; feature_id < 15; feature_id++) {
    types.push_back(GetOptionalQType<int64_t>());
  }

  for (int iteration = 0; iteration < 5; ++iteration) {
    std::vector<DecisionTree> trees;
    // Add deep trees
    for (int i = 0; i < 10; ++i) {
      int num_splits = absl::Uniform<int32_t>(rnd, 0, 1024);
      trees.push_back(
          CreateRandomTree(&rnd, /*interactions=*/true, num_splits, &types));
    }
    // Add single feature trees (without interactions)
    for (int i = 0; i < 10; ++i) {
      int num_splits = absl::Uniform<int32_t>(rnd, 0, 1024);
      trees.push_back(
          CreateRandomTree(&rnd, /*interactions=*/false, num_splits, &types));
    }
    // Add deep float trees with ranges and equality splits
    for (int i = 0; i < 10; ++i) {
      int num_splits = absl::Uniform<int32_t>(rnd, 0, 1024);
      trees.push_back(CreateRandomFloatTree(
          &rnd, /*num_features=*/10, /*interactions=*/true, num_splits,
          /*range_split_prob=*/0.4, /*equality_split_prob=*/0.4));
    }
    // Add small trees (bitmask evaluator can be used)
    for (int i = 0; i < 10; ++i) {
      int num_splits = absl::Uniform<int32_t>(rnd, 0, 32);
      trees.push_back(
          CreateRandomTree(&rnd, /*interactions=*/true, num_splits, &types));
    }
    for (int i = 0; i < 5; ++i) {
      int depth = absl::Uniform<int32_t>(rnd, 1, 20);
      trees.push_back(CreateRandomObliviousTree(&rnd, depth, &types));
    }

    RandomTestAgainstReferenceImplementation(
        SourceLocation::current(), trees, {kDefaultEval, kRegularEval}, &rnd);
  }
}

}  // namespace
}  // namespace arolla
