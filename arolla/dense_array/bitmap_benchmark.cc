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
#include <utility>

#include "benchmark/benchmark.h"
#include "arolla/dense_array/bitmap.h"

namespace arolla::bitmap {
namespace {

void BM_CreateAlmostFullFullBitmap(::benchmark::State& state) {
  int64_t size = state.range(0);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(size);
    AlmostFullBuilder builder(size);
    Bitmap bitmap = std::move(builder).Build();
    ::benchmark::DoNotOptimize(bitmap);
  }
}

BENCHMARK(BM_CreateAlmostFullFullBitmap)->Range(0, 1000);

void BM_CreateAlmostFullEmptyBitmap(::benchmark::State& state) {
  int64_t size = state.range(0);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(size);
    AlmostFullBuilder builder(size);
    for (int64_t i = 0; i < size; ++i) {
      builder.AddMissed(i);
    }
    Bitmap bitmap = std::move(builder).Build();
    ::benchmark::DoNotOptimize(bitmap);
  }
}

BENCHMARK(BM_CreateAlmostFullEmptyBitmap)->Range(0, 1000);

void BM_CreateAlmostFullSparseBitmap(::benchmark::State& state) {
  int64_t size = state.range(0);
  int64_t step = size / 3 + 1;
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(size);
    AlmostFullBuilder builder(size);
    for (int64_t i = 0; i < size; i += step) {
      builder.AddMissed(i);
    }
    Bitmap bitmap = std::move(builder).Build();
    ::benchmark::DoNotOptimize(bitmap);
  }
}

BENCHMARK(BM_CreateAlmostFullSparseBitmap)->Range(0, 1000);

}  // namespace
}  // namespace arolla::bitmap
