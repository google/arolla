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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/barrier.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;

struct TestInput {
  int64_t x;
};

using ModelFunction = std::function<absl::StatusOr<int64_t>(const TestInput&)>;

void BenchmarkSettings(benchmark::internal::Benchmark* bm) { bm->ThreadRange(1, 16); }

// Benchmarks separate instances of a model created by ModelFactory()().
template <typename ModelFactory>
void BM_LocalModel(benchmark::State& state) {
  auto model = ModelFactory()();

  TestInput input{0};

  // Warm-up cache.
  {
    auto x = model(input).value();
    benchmark::DoNotOptimize(x);
  }
  // Wait until all the threads are ready to prevent preparation from stealing
  // evaluation resources.
  absl::Barrier barrier(state.threads());

  for (auto _ : state) {
    auto x = model(input).value();
    benchmark::DoNotOptimize(x);
    input.x++;
  }
}

// Benchmarks shared instance of a model created by ModelFactory()().
template <typename ModelFactory>
void BM_SharedModel(benchmark::State& state) {
  static Indestructible<ModelFunction> model((ModelFactory()()));

  TestInput input{0};

  // Warm-up cache.
  {
    auto x = (*model)(input).value();
    benchmark::DoNotOptimize(x);
  }
  // Wait until all the threads are ready to prevent preparation from stealing
  // evaluation resources.
  absl::Barrier barrier(state.threads());

  for (auto _ : state) {
    auto x = (*model)(input).value();
    benchmark::DoNotOptimize(x);
    input.x++;
  }
}

// Benchmarks shared instances of a model created by ModelFactory()(), used with
// interleaving to simulate real-world usage.
template <typename ModelFactory>
void BM_SharedModelWithInterleaving(benchmark::State& state) {
  constexpr size_t kExecutorsNumber = 1000;
  static Indestructible<std::vector<ModelFunction>> models([&]() {
    std::vector<ModelFunction> result;
    result.reserve(kExecutorsNumber);
    for (size_t i = 0; i < kExecutorsNumber; ++i) {
      result.push_back(ModelFactory()());
    }
    return result;
  }());

  // Generate thread-specific order of models usages. We are
  // doing it out of the loop to avoid benchmarking absl::Uniform.
  absl::BitGen gen;
  std::vector<int> model_indices;
  model_indices.reserve(kExecutorsNumber * 100);
  for (size_t i = 0; i < kExecutorsNumber * 100; ++i) {
    model_indices.push_back(absl::Uniform<size_t>(gen, 0, models->size()));
  }

  TestInput input{0};

  // Warm-up cache.
  for (const auto& executor : *models) {
    auto x = executor(input).value();
    benchmark::DoNotOptimize(x);
  }

  // Wait until all the threads are ready to prevent preparation from stealing
  // evaluation resources.
  absl::Barrier barrier(state.threads());

  auto index = 0;
  for (auto _ : state) {
    auto x = (*models)[model_indices[index]](input).value();
    benchmark::DoNotOptimize(x);
    index = (index + 1) % model_indices.size();
    input.x++;
  }
}

// Generates a model for benchmarking. The model contains 1 leaf, `size`
// operators, `size / 5` different literals.
absl::StatusOr<ExprNodePtr> BenchmarkModel(int size) {
  InitArolla();

  auto expr = Leaf("x");
  for (int i = 0; i < size; ++i) {
    ASSIGN_OR_RETURN(expr, CallOp("math.add", {Literal(i / 5), expr}));
  }
  return expr;
}

ExprCompiler<TestInput, int64_t> GetExprCompiler() {
  return ExprCompiler<TestInput, int64_t>().SetInputLoader(
      CreateAccessorsInputLoader<TestInput>(
          "x", [](const TestInput& in) { return in.x; }));
}

// Model factory setting ThreadUnsafe thread safety policy.
template <int size>
struct ThreadUnsafe {
  ModelFunction operator()() const {
    return GetExprCompiler()
        .SetThreadUnsafe_I_SWEAR_TO_COPY_MODEL_FUNCTION_BEFORE_CALL()
        .Compile(BenchmarkModel(size))
        .value();
  }
};

// Model factory setting AlwaysCloneSafetyPolicy.
template <int size>
struct AlwaysClone {
  ModelFunction operator()() const {
    return GetExprCompiler()
        .SetAlwaysCloneThreadSafetyPolicy()
        .Compile(BenchmarkModel(size))
        .value();
  }
};

// Model factory setting PoolThreadSafetyPolicy.
template <int size>
struct Pool {
  ModelFunction operator()() const {
    return GetExprCompiler()
        .SetPoolThreadSafetyPolicy()
        .Compile(BenchmarkModel(size))
        .value();
  }
};

BENCHMARK(BM_LocalModel<ThreadUnsafe<10>>)->Apply(BenchmarkSettings);
BENCHMARK(BM_LocalModel<AlwaysClone<10>>)->Apply(BenchmarkSettings);
BENCHMARK(BM_LocalModel<Pool<10>>)->Apply(BenchmarkSettings);

BENCHMARK(BM_SharedModel<AlwaysClone<10>>)->Apply(BenchmarkSettings);
BENCHMARK(BM_SharedModel<Pool<10>>)->Apply(BenchmarkSettings);

BENCHMARK(BM_SharedModelWithInterleaving<AlwaysClone<10>>)
    ->Apply(BenchmarkSettings);
BENCHMARK(BM_SharedModelWithInterleaving<Pool<10>>)->Apply(BenchmarkSettings);

BENCHMARK(BM_LocalModel<ThreadUnsafe<1000>>)->Apply(BenchmarkSettings);
BENCHMARK(BM_LocalModel<AlwaysClone<1000>>)->Apply(BenchmarkSettings);
BENCHMARK(BM_LocalModel<Pool<1000>>)->Apply(BenchmarkSettings);

BENCHMARK(BM_SharedModel<AlwaysClone<1000>>)->Apply(BenchmarkSettings);
BENCHMARK(BM_SharedModel<Pool<1000>>)->Apply(BenchmarkSettings);

BENCHMARK(BM_SharedModelWithInterleaving<AlwaysClone<1000>>)
    ->Apply(BenchmarkSettings);
BENCHMARK(BM_SharedModelWithInterleaving<Pool<1000>>)->Apply(BenchmarkSettings);

}  // namespace
}  // namespace arolla
