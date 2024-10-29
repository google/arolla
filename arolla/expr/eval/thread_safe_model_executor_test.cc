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
#include "arolla/expr/eval/thread_safe_model_executor.h"

#include <cstdint>
#include <functional>
#include <future>  // NOLINT
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"

namespace arolla::expr {
namespace {

constexpr int kNumIterations = 100;
constexpr int kNumThreads = 10;

using ::absl_testing::IsOkAndHolds;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::UnorderedElementsAreArray;

struct TestInput {
  int64_t x;
};

absl::StatusOr<std::unique_ptr<InputLoader<TestInput>>>
CreateTestInputsLoader() {
  return CreateAccessorsInputLoader<TestInput>(
      "x", [](const TestInput& in) { return in.x; });
}

absl::StatusOr<std::unique_ptr<InputLoader<TestInput>>>
CreateDenseArrayTestInputsLoader() {
  return CreateAccessorsInputLoader<TestInput>("x", [](const TestInput& in) {
    return CreateDenseArray<int64_t>({in.x, in.x, in.x});
  });
}

std::vector<int64_t> Iota(int64_t num_elements) {
  std::vector<int64_t> result(num_elements);
  std::iota(result.begin(), result.end(), 0);
  return result;
}

template <template <typename I, typename O, typename S> typename ME,
          typename Input, typename Output, typename SideOutput>
std::vector<absl::StatusOr<Output>> RunManyThreadFunc(
    int thread_nr, const ME<Input, Output, SideOutput>& executor) {
  std::vector<absl::StatusOr<Output>> results;
  for (int j = 0; j < kNumIterations; ++j) {
    results.emplace_back(
        std::move(executor(TestInput{thread_nr * kNumIterations + j})));
  }
  return results;
}

// Runs kNumThreads threads each running `executor` kNumIterations times on the
// given `input`. `executor` and `input` must stay in scope until all the
// resulting futures are ready. Each of the model executions accepts different
// input in the range of 0 .. kNumThreads*kNumIterations.
template <template <typename I, typename O, typename S> typename ME,
          typename Input, typename Output, typename SideOutput>
std::vector<absl::StatusOr<Output>> RunMany(
    const ME<Input, Output, SideOutput>& executor, bool copy_for_each_thread) {
  std::vector<std::future<std::vector<absl::StatusOr<Output>>>> futures;
  for (int i = 0; i < kNumThreads; ++i) {
    std::function<std::vector<absl::StatusOr<Output>>()> thread_func;
    if (copy_for_each_thread) {
      thread_func = [i, executor]() { return RunManyThreadFunc(i, executor); };
    } else {
      thread_func = [i, &executor]() { return RunManyThreadFunc(i, executor); };
    }
    futures.push_back(std::async(std::launch::async, thread_func));
  }
  std::vector<absl::StatusOr<Output>> results;
  for (auto& future : futures) {
    auto thread_results = future.get();
    results.insert(results.end(), thread_results.begin(), thread_results.end());
  }
  return results;
}

TEST(ThreadSafeModelExecutorTest, Move) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafeModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  ASSERT_THAT(thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  ThreadSafeModelExecutor<TestInput, int64_t> other_thread_safe_executor(
      std::move(thread_safe_executor));
  ASSERT_THAT(other_thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_THAT(thread_safe_executor.IsValid(), IsFalse());

  thread_safe_executor = std::move(other_thread_safe_executor);
  ASSERT_THAT(thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_THAT(other_thread_safe_executor.IsValid(), IsFalse());
}

TEST(ThreadSafeModelExecutorTest, Copy) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafeModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  ASSERT_THAT(thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  ThreadSafeModelExecutor<TestInput, int64_t> other_thread_safe_executor(
      thread_safe_executor);
  ASSERT_THAT(other_thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  EXPECT_THAT(thread_safe_executor.IsValid(), IsTrue());
}

TEST(ThreadSafeModelExecutorTest, ExecuteOnce) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafeModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
}

TEST(ThreadSafeModelExecutorTest, ExecuteMany) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafeModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(thread_safe_executor, /*copy_for_each_thread=*/false)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

TEST(ThreadSafeModelExecutorTest, ExecuteManyOnDenseArrays) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateDenseArrayTestInputsLoader());

  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (CompileModelExecutor<DenseArray<int64_t>>(ast, *input_loader)));

  ThreadSafeModelExecutor<TestInput, DenseArray<int64_t>> thread_safe_executor(
      std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(thread_safe_executor, /*copy_for_each_thread=*/false)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result[0].value);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

TEST(ThreadSafeModelExecutorTest, ExecuteManyOnDenseArraysWithArena) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateDenseArrayTestInputsLoader());

  ModelExecutorOptions options;
  options.arena_page_size = 64 << 10;
  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (CompileModelExecutor<DenseArray<int64_t>>(ast, *input_loader, options)));

  ThreadSafePoolModelExecutor<TestInput, DenseArray<int64_t>>
      thread_safe_executor(std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(thread_safe_executor, /*copy_for_each_thread=*/false)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result[0].value);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

TEST(ThreadSafePoolModelExecutorTest, Move) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafePoolModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  ASSERT_THAT(thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  ThreadSafePoolModelExecutor<TestInput, int64_t> other_thread_safe_executor(
      std::move(thread_safe_executor));
  ASSERT_THAT(other_thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_THAT(thread_safe_executor.IsValid(), IsFalse());

  thread_safe_executor = std::move(other_thread_safe_executor);
  ASSERT_THAT(thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_THAT(other_thread_safe_executor.IsValid(), IsFalse());
}

TEST(ThreadSafePoolModelExecutorTest, Copy) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafePoolModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  ASSERT_THAT(thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  ThreadSafePoolModelExecutor<TestInput, int64_t> other_thread_safe_executor(
      thread_safe_executor);
  ASSERT_THAT(other_thread_safe_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
  EXPECT_THAT(thread_safe_executor.IsValid(), IsTrue());
}

TEST(ThreadSafePoolModelExecutorTest, ExecuteOnce) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafePoolModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  EXPECT_THAT(thread_safe_executor(TestInput{57}), IsOkAndHolds(57));
}

TEST(ThreadSafePoolModelExecutorTest, ExecuteMany) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  ThreadSafePoolModelExecutor<TestInput, int64_t> thread_safe_executor(
      std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(thread_safe_executor, /*copy_for_each_thread=*/false)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

TEST(ThreadSafePoolModelExecutorTest, ExecuteManyOnDenseArrays) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateDenseArrayTestInputsLoader());

  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (CompileModelExecutor<DenseArray<int64_t>>(ast, *input_loader)));

  ThreadSafePoolModelExecutor<TestInput, DenseArray<int64_t>>
      thread_safe_executor(std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(thread_safe_executor, /*copy_for_each_thread=*/false)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result[0].value);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

TEST(ThreadSafePoolModelExecutorTest, ExecuteManyOnDenseArraysWithArena) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateDenseArrayTestInputsLoader());

  ModelExecutorOptions options;
  options.arena_page_size = 64 << 10;
  ASSERT_OK_AND_ASSIGN(
      auto executor,
      (CompileModelExecutor<DenseArray<int64_t>>(ast, *input_loader, options)));

  ThreadSafePoolModelExecutor<TestInput, DenseArray<int64_t>>
      thread_safe_executor(std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(thread_safe_executor, /*copy_for_each_thread=*/false)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result[0].value);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

TEST(CopyableThreadUnsafeModelExecutorTest, Move) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  CopyableThreadUnsafeModelExecutor<TestInput, int64_t> copyable_executor(
      std::move(executor));
  ASSERT_THAT(copyable_executor.IsValid(), IsTrue());
  EXPECT_THAT(copyable_executor(TestInput{57}), IsOkAndHolds(57));
  CopyableThreadUnsafeModelExecutor<TestInput, int64_t> other_copyable_executor(
      std::move(copyable_executor));
  ASSERT_THAT(other_copyable_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_copyable_executor(TestInput{57}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_THAT(copyable_executor.IsValid(), IsFalse());

  copyable_executor = std::move(other_copyable_executor);
  ASSERT_THAT(copyable_executor.IsValid(), IsTrue());
  EXPECT_THAT(copyable_executor(TestInput{57}), IsOkAndHolds(57));
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_THAT(other_copyable_executor.IsValid(), IsFalse());
}

TEST(CopyableThreadUnsafeModelExecutorTest, Copy) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  CopyableThreadUnsafeModelExecutor<TestInput, int64_t> copyable_executor(
      std::move(executor));
  ASSERT_THAT(copyable_executor.IsValid(), IsTrue());
  EXPECT_THAT(copyable_executor(TestInput{57}), IsOkAndHolds(57));
  CopyableThreadUnsafeModelExecutor<TestInput, int64_t> other_copyable_executor(
      copyable_executor);
  ASSERT_THAT(other_copyable_executor.IsValid(), IsTrue());
  EXPECT_THAT(other_copyable_executor(TestInput{57}), IsOkAndHolds(57));
  EXPECT_THAT(copyable_executor.IsValid(), IsTrue());
}

TEST(CopyableThreadUnsafeModelExecutorTest, ExecuteOnce) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  CopyableThreadUnsafeModelExecutor<TestInput, int64_t> copyable_executor(
      std::move(executor));
  EXPECT_THAT(copyable_executor(TestInput{57}), IsOkAndHolds(57));
}

TEST(CopyableThreadUnsafeModelExecutorTest, ExecuteMany) {
  auto ast = Leaf("x");
  ASSERT_OK_AND_ASSIGN(auto input_loader, CreateTestInputsLoader());
  ASSERT_OK_AND_ASSIGN(auto executor,
                       (CompileModelExecutor<int64_t>(ast, *input_loader)));

  CopyableThreadUnsafeModelExecutor<TestInput, int64_t> copyable_executor(
      std::move(executor));
  absl::flat_hash_set<int64_t> seen_results;
  for (auto& result_or :
       RunMany(copyable_executor, /*copy_for_each_thread=*/true)) {
    ASSERT_OK_AND_ASSIGN(auto result, result_or);
    seen_results.insert(result);
  }
  EXPECT_THAT(seen_results,
              UnorderedElementsAreArray(Iota(kNumThreads * kNumIterations)));
}

}  // namespace
}  // namespace arolla::expr
