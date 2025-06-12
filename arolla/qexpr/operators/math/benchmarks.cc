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
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "arolla/qexpr/operators/math/arithmetic.h"

namespace arolla {

namespace {

auto CreateBitGen() { return std::mt19937(34); }

template <typename T>
std::vector<T> RandomVector01(size_t n) {
  auto gen = CreateBitGen();
  std::uniform_real_distribution<T> uniform01;
  std::vector<T> result(n);
  for (auto& x : result) {
    x = uniform01(gen);
  }
  return result;
}

std::vector<int> RandomInts(size_t n, int lower, int upper) {
  auto gen = CreateBitGen();
  std::uniform_int_distribution<int> uniform;
  std::vector<int> result(n);
  int diff = upper - lower;
  for (auto& x : result) {
    x = (uniform(gen) % diff) + lower;
  }
  return result;
}

}  // namespace

template <typename T, typename MinFn>
void MinBenchmark(benchmark::State& state, MinFn fn) {
  auto values = RandomVector01<T>(65536);
  for (int i : RandomInts(1000, 0, values.size())) {
    values[i] = std::numeric_limits<T>::quiet_NaN();
  }
  // Avoid OOB by overflowing.
  uint16_t i = 0;
  for (auto _ : state) {
    auto x = values[i];
    ++i;
    auto y = values[i];
    ++i;
    auto z = fn(x, y);
    benchmark::DoNotOptimize(z);
  }
}

void BM_MinOp_Float32(benchmark::State& state) {
  MinBenchmark<float>(state, MinOp());
}

void BM_MinOp_Float64(benchmark::State& state) {
  MinBenchmark<double>(state, MinOp());
}

// Sanity check that std::min is the same as checking manually.
void BM_MinNoNan_Float32(benchmark::State& state) {
  MinBenchmark<float>(state, [](float x, float y) { return x < y ? x : y; });
}

// Sanity check that std::min is the same as checking manually.
void BM_MinNoNan_Float64(benchmark::State& state) {
  MinBenchmark<double>(state, [](double x, double y) { return x < y ? x : y; });
}

// Inspired by util/math/mathutil.h.
void BM_MinWithNan_Float32(benchmark::State& state) {
  MinBenchmark<float>(
      state, [](float x, float y) { return (std::isnan(x) || x < y) ? x : y; });
}

// Inspired by util/math/mathutil.h.
void BM_MinWithNan_Float64(benchmark::State& state) {
  MinBenchmark<double>(state, [](double x, double y) {
    return (std::isnan(x) || x < y) ? x : y;
  });
}

void BM_StdMinNoNan_Float32(benchmark::State& state) {
  MinBenchmark<float>(state, [](float x, float y) { return std::min(x, y); });
}

void BM_StdMinNoNan_Float64(benchmark::State& state) {
  MinBenchmark<double>(state,
                       [](double x, double y) { return std::min(x, y); });
}

void BM_StdFminNoNan_Float32(benchmark::State& state) {
  MinBenchmark<float>(state, [](float x, float y) { return std::fmin(x, y); });
}

void BM_StdFminNoNan_Float64(benchmark::State& state) {
  MinBenchmark<double>(state,
                       [](double x, double y) { return std::fmin(x, y); });
}

BENCHMARK(BM_MinOp_Float32);
BENCHMARK(BM_MinOp_Float64);
BENCHMARK(BM_MinNoNan_Float32);
BENCHMARK(BM_MinNoNan_Float64);
BENCHMARK(BM_MinWithNan_Float32);
BENCHMARK(BM_MinWithNan_Float64);
BENCHMARK(BM_StdMinNoNan_Float32);
BENCHMARK(BM_StdMinNoNan_Float64);
BENCHMARK(BM_StdFminNoNan_Float32);
BENCHMARK(BM_StdFminNoNan_Float64);

}  // namespace arolla
