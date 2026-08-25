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
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "arolla/util/overflow.h"

namespace arolla {
namespace {

template <typename T>
std::vector<T> GenerateRandomInts(size_t n, T min_val, T max_val) {
  std::mt19937_64 gen(42);
  std::uniform_int_distribution<T> dist(min_val, max_val);
  std::vector<T> result(n);
  for (auto& val : result) {
    val = dist(gen);
  }
  return result;
}

// std::partial_sum with unchecked operator+
template <typename T>
void BM_PartialSum_Std_UncheckedAdd(benchmark::State& state) {
  const size_t n = state.range(0);
  const auto in = GenerateRandomInts<T>(n, 1, 100);
  std::vector<T> out(n);
  for (auto _ : state) {
    std::partial_sum(in.begin(), in.end(), out.begin());
    benchmark::DoNotOptimize(out.data());
  }
  state.SetItemsProcessed(state.iterations() * n);
  state.SetBytesProcessed(state.iterations() * n * sizeof(T));
}

// Manual for-loop with unchecked operator+
template <typename T>
void BM_PartialSum_Loop_UncheckedAdd(benchmark::State& state) {
  const size_t n = state.range(0);
  const auto in = GenerateRandomInts<T>(n, 1, 100);
  std::vector<T> out(n + 1, 0);
  for (auto _ : state) {
    out[0] = 0;
    for (size_t i = 0; i < n; ++i) {
      out[i + 1] = out[i] + in[i];
    }
    benchmark::DoNotOptimize(out.data());
  }
  state.SetItemsProcessed(state.iterations() * n);
  state.SetBytesProcessed(state.iterations() * n * sizeof(T));
}

// std::partial_sum with safe_add lambda
template <typename T>
void BM_PartialSum_Std_SafeAdd(benchmark::State& state) {
  const size_t n = state.range(0);
  const auto in = GenerateRandomInts<T>(n, 1, 100);
  std::vector<T> out(n);
  for (auto _ : state) {
    bool overflow = false;
    std::partial_sum(in.begin(), in.end(), out.begin(), [&overflow](T a, T b) {
      return safe_add(a, b, &overflow);
    });
    benchmark::DoNotOptimize(out.data());
    benchmark::DoNotOptimize(overflow);
  }
  state.SetItemsProcessed(state.iterations() * n);
  state.SetBytesProcessed(state.iterations() * n * sizeof(T));
}

// Manual for-loop with safe_add
template <typename T>
void BM_PartialSum_Loop_SafeAdd(benchmark::State& state) {
  const size_t n = state.range(0);
  const auto in = GenerateRandomInts<T>(n, 1, 100);
  std::vector<T> out(n + 1, 0);
  for (auto _ : state) {
    out[0] = 0;
    bool overflow = false;
    for (size_t i = 0; i < n; ++i) {
      out[i + 1] = safe_add(out[i], in[i], &overflow);
    }
    benchmark::DoNotOptimize(out.data());
    benchmark::DoNotOptimize(overflow);
  }
  state.SetItemsProcessed(state.iterations() * n);
  state.SetBytesProcessed(state.iterations() * n * sizeof(T));
}

// Manual for-loop with SafeAdd (returns absl::StatusOr).
template <typename T>
void BM_PartialSum_Loop_SafeAddStatusOr(benchmark::State& state) {
  const size_t n = state.range(0);
  const auto in = GenerateRandomInts<T>(n, 1, 100);
  std::vector<T> out(n + 1, 0);
  for (auto _ : state) {
    out[0] = 0;
    bool ok = true;
    for (size_t i = 0; i < n; ++i) {
      auto res = SafeAdd(out[i], in[i]);
      if (!res.ok()) {
        ok = false;
        break;
      }
      out[i + 1] = *res;
    }
    benchmark::DoNotOptimize(out.data());
    benchmark::DoNotOptimize(ok);
  }
  state.SetItemsProcessed(state.iterations() * n);
  state.SetBytesProcessed(state.iterations() * n * sizeof(T));
}

BENCHMARK_TEMPLATE(BM_PartialSum_Std_UncheckedAdd, int64_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Loop_UncheckedAdd, int64_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Std_SafeAdd, int64_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Loop_SafeAdd, int64_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Loop_SafeAddStatusOr, int64_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Std_UncheckedAdd, int32_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Loop_UncheckedAdd, int32_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Std_SafeAdd, int32_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Loop_SafeAdd, int32_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

BENCHMARK_TEMPLATE(BM_PartialSum_Loop_SafeAddStatusOr, int32_t)
    ->RangeMultiplier(4)
    ->Range(16, 1 << 16);

}  // namespace
}  // namespace arolla
