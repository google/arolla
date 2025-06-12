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
#include <cstdint>

#include "benchmark/benchmark.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/bytes.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

constexpr auto kSizesFn = [](auto* b) {
  b->Arg(0)->Arg(1)->Arg(100)->Arg(50000)->Arg(500000);
};

template <class T>
void BM_CreateEmptyDenseArray(::benchmark::State& state) {
  int64_t size = state.range(0);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(size);
    auto array = CreateEmptyDenseArray<T>(size);
    ::benchmark::DoNotOptimize(array);
  }
}

BENCHMARK(BM_CreateEmptyDenseArray<int32_t>)->Apply(kSizesFn);
BENCHMARK(BM_CreateEmptyDenseArray<int64_t>)->Apply(kSizesFn);
BENCHMARK(BM_CreateEmptyDenseArray<Bytes>)->Apply(kSizesFn);
BENCHMARK(BM_CreateEmptyDenseArray<Unit>)->Apply(kSizesFn);

template <class T>
void BM_CreateConstDenseArray(::benchmark::State& state) {
  int64_t size = state.range(0);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(size);
    auto array = CreateConstDenseArray<T>(size, T());
    ::benchmark::DoNotOptimize(array);
  }
}

BENCHMARK(BM_CreateConstDenseArray<int32_t>)->Apply(kSizesFn);
BENCHMARK(BM_CreateConstDenseArray<int64_t>)->Apply(kSizesFn);
BENCHMARK(BM_CreateConstDenseArray<Bytes>)->Apply(kSizesFn);
BENCHMARK(BM_CreateConstDenseArray<Unit>)->Apply(kSizesFn);

}  // namespace
}  // namespace arolla
