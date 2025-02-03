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
#include "arolla/decision_forest/pointwise_evaluation/pointwise.h"

#include <functional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;

template <class T>
struct LessTest {
  bool operator()(absl::Span<const T> values) const {
    return values[feature_id] < threshold;
  }
  int feature_id;
  T threshold;
};

TEST(PointwiseTest, CompileOk) {
  PredictorCompiler<float, LessTest<float>> compiler(3);
  EXPECT_OK(compiler.SetNode(0, 2, 1, {0, 13.0}));
  EXPECT_OK(compiler.SetLeaf(1, 0.0));
  EXPECT_OK(compiler.SetLeaf(2, 1.0));
  auto eval_or = compiler.Compile();
  EXPECT_THAT(compiler.Compile().status(),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_OK(eval_or.status());
  auto eval = std::move(eval_or).value();
  EXPECT_EQ(eval.Predict(std::vector<float>{5.0}), 1.0);
  EXPECT_EQ(eval.Predict(std::vector<float>{15.0}), 0.0);
}

TEST(PointwiseTest, BoostedCompileOk) {
  BoostedPredictorCompiler<float, LessTest<float>, std::plus<float>> compiler;
  auto tree_compiler1 = compiler.AddTree(3);
  EXPECT_OK(tree_compiler1.SetNode(0, 2, 1, {0, 13.0}));
  EXPECT_OK(tree_compiler1.SetLeaf(1, 0.0));
  EXPECT_OK(tree_compiler1.SetLeaf(2, 1.0));
  auto tree_compiler2 = compiler.AddTree(1);
  EXPECT_OK(tree_compiler2.SetLeaf(0, 4.0));
  auto eval_or = compiler.Compile();
  EXPECT_THAT(compiler.Compile().status(),
              StatusIs(absl::StatusCode::kFailedPrecondition));
  EXPECT_OK(eval_or.status());
  auto eval = std::move(eval_or).value();
  EXPECT_EQ(eval.Predict(std::vector<float>{5.0}), 5.0);
  EXPECT_EQ(eval.Predict(std::vector<float>{15.0}), 4.0);
}

TEST(PointwiseTest, BoostedWithFilter) {
  BoostedPredictorCompiler<float, LessTest<float>, std::plus<float>, int>
      compiler;
  auto tree_compiler1 = compiler.AddTree(3, 0);
  EXPECT_OK(tree_compiler1.SetNode(0, 2, 1, {0, 13.0}));
  EXPECT_OK(tree_compiler1.SetLeaf(1, 0.0));
  EXPECT_OK(tree_compiler1.SetLeaf(2, 1.0));
  auto tree_compiler2 = compiler.AddTree(1, 1);
  EXPECT_OK(tree_compiler2.SetLeaf(0, 4.0));
  auto eval_or = compiler.Compile();
  EXPECT_OK(eval_or.status());
  auto eval = std::move(eval_or).value();
  EXPECT_EQ(
      eval.Predict(std::vector<float>{5.0}, 0.0, [](int x) { return x == 0; }),
      1.0);
  EXPECT_EQ(
      eval.Predict(std::vector<float>{5.0}, 0.0, [](int x) { return x == 1; }),
      4.0);
}

SinglePredictor<std::pair<int, int>, LessTest<int>> CompileChessBoard(
    int depth) {
  const int size = (1 << depth);
  PredictorCompiler<std::pair<int, int>, LessTest<int>> compiler(
      size * size * 2 - 1);
  int id = 0;
  for (int layer = 0; layer < depth; ++layer) {
    const int num_splits = (1 << layer);
    const int step = size * 2 / num_splits;
    const int start = size / num_splits;
    for (int i = 0; i < num_splits; ++i) {
      EXPECT_OK(
          compiler.SetNode(id, id * 2 + 1, id * 2 + 2, {0, step * i + start}));
      ++id;
    }
  }
  for (int layer = 0; layer < depth; ++layer) {
    const int num_splits = (1 << layer);
    const int step = size * 2 / num_splits;
    const int start = size >> layer;
    for (int r = 0; r < size; ++r) {  // repeat for every leaf from first stage
      for (int i = 0; i < num_splits; ++i) {
        EXPECT_OK(compiler.SetNode(id, id * 2 + 1, id * 2 + 2,
                                   {1, step * i + start}));
        ++id;
      }
    }
  }
  for (int i = 0; i < size; ++i) {
    for (int j = 0; j < size; ++j) {
      EXPECT_OK(compiler.SetLeaf(id++, {i, j}));
    }
  }
  auto eval_or = compiler.Compile();
  EXPECT_OK(eval_or.status());
  return std::move(eval_or).value();
}

TEST(PointwiseTest, ChessBoard) {
  for (int depth = 1; depth <= 7; ++depth) {
    const int size = (1 << depth);
    auto eval = CompileChessBoard(depth);
    for (int i = 0; i < size; ++i) {
      for (int j = 0; j < size; ++j) {
        auto p = eval.Predict(std::vector<int>{i * 2 + 1, j * 2 + 1});
        ASSERT_EQ(i, p.first);
        ASSERT_EQ(j, p.second);
      }
    }
  }
}

TEST(PointwiseTest, PreCompileFail) {
  PredictorCompiler<float, LessTest<float>> compiler(7);
  EXPECT_OK(compiler.SetNode(0, 2, 1, {0, 13.0}));
  EXPECT_THAT(compiler.SetLeaf(0, 13.0),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(compiler.SetNode(0, 5, 6, {0, 13.0}),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(compiler.SetNode(13, 5, 6, {0, 13.0}),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(compiler.SetNode(1, 14, 3, {0, 13.0}),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(compiler.SetNode(2, 4, 14, {0, 13.0}),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(PointwiseTest, BoostedPreCompileFail) {
  BoostedPredictorCompiler<float, LessTest<float>, std::plus<float>> compiler;
  auto tree_compiler = compiler.AddTree(7);
  EXPECT_OK(tree_compiler.SetNode(0, 2, 1, {0, 13.0}));
  EXPECT_THAT(tree_compiler.SetLeaf(0, 13.0),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(tree_compiler.SetNode(0, 5, 6, {0, 13.0}),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(tree_compiler.SetNode(13, 5, 6, {0, 13.0}),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(tree_compiler.SetNode(1, 14, 3, {0, 13.0}),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(tree_compiler.SetNode(2, 4, 14, {0, 13.0}),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(PointwiseTest, CompileFail) {
  {  // empty tree
    PredictorCompiler<float, LessTest<float>> compiler(0);
    EXPECT_THAT(compiler.Compile().status(),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {  // no leaf set
    PredictorCompiler<float, LessTest<float>> compiler(3);
    EXPECT_OK(compiler.SetNode(0, 2, 1, {0, 13.0}));
    EXPECT_THAT(compiler.Compile().status(),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {  // node not used at all
    PredictorCompiler<float, LessTest<float>> compiler(4);
    EXPECT_OK(compiler.SetNode(0, 2, 1, {0, 13.0}));
    EXPECT_OK(compiler.SetLeaf(1, 0.0));
    EXPECT_OK(compiler.SetLeaf(2, 1.0));
    EXPECT_THAT(compiler.Compile().status(),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST(PointwiseTest, BoostedCompileFail) {
  {  // first tree fail
    BoostedPredictorCompiler<float, LessTest<float>, std::plus<float>> compiler;
    compiler.AddTree(0);
    EXPECT_THAT(compiler.Compile().status(),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {  // 2nd tree fail
    BoostedPredictorCompiler<float, LessTest<float>, std::plus<float>> compiler;
    auto tree_compiler1 = compiler.AddTree(1);
    EXPECT_OK(tree_compiler1.SetLeaf(0, 1.0));
    compiler.AddTree(0);
    EXPECT_THAT(compiler.Compile().status(),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
}

}  // namespace
}  // namespace arolla
