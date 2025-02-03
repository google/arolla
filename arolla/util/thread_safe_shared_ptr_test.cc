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
#include "arolla/util/thread_safe_shared_ptr.h"

#include <array>
#include <memory>
#include <string>

#include "benchmark/benchmark.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"

namespace arolla {
namespace {

TEST(ThreadSafeSharedPtr, DefaultConstructor) {
  ThreadSafeSharedPtr<std::string> ptr;
  EXPECT_TRUE(ptr == nullptr);
  EXPECT_FALSE(ptr != nullptr);
  EXPECT_EQ(ptr.load(), nullptr);
}

TEST(ThreadSafeSharedPtr, Constructor) {
  auto hello = std::make_shared<std::string>("Hello");
  ThreadSafeSharedPtr<std::string> ptr(hello);
  EXPECT_FALSE(ptr == nullptr);
  EXPECT_TRUE(ptr != nullptr);
  EXPECT_EQ(ptr.load(), hello);
}

TEST(ThreadSafeSharedPtr, Store) {
  auto hello = std::make_shared<std::string>("Hello");
  auto world = std::make_shared<std::string>("World");
  ThreadSafeSharedPtr<std::string> ptr;
  EXPECT_EQ(ptr.load(), nullptr);
  ptr.store(hello);
  EXPECT_EQ(ptr.load(), hello);
  ptr.store(world);
  EXPECT_EQ(ptr.load(), world);
}

void BM_ThreadSafeSharedPtr_Load(benchmark::State& state) {
  static absl::NoDestructor<ThreadSafeSharedPtr<std::string>> storage(
      std::make_shared<std::string>("Hello, World!"));
  for (auto _ : state) {
    auto tmp = storage->load();
    benchmark::DoNotOptimize(tmp);
  }
}

void BM_ThreadSafeSharedPtr_Store(benchmark::State& state) {
  static absl::NoDestructor<ThreadSafeSharedPtr<std::string>> storage(
      std::make_shared<std::string>("Hello, World!"));
  std::array<std::shared_ptr<std::string>, 2> values = {
      std::make_shared<std::string>("Hello"),
      std::make_shared<std::string>("World")};
  unsigned int idx = 0;
  for (auto _ : state) {
    storage->store(values[++idx & 1]);
  }
}

BENCHMARK(BM_ThreadSafeSharedPtr_Load)->ThreadRange(1, 8);
BENCHMARK(BM_ThreadSafeSharedPtr_Store)->ThreadRange(1, 8);

}  // namespace
}  // namespace arolla
