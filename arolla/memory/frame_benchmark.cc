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
#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"

namespace arolla {

template <class T>
void BM_Initialize(benchmark::State& state) {
  int64_t cnt = state.range(0);
  FrameLayout::Builder builder;
  for (int64_t i = 0; i != cnt; ++i) {
    builder.AddSlot<T>();
  }
  auto layout = std::move(builder).Build();
  while (state.KeepRunningBatch(cnt)) {
    auto x = MemoryAllocation(&layout);
    benchmark::DoNotOptimize(x);
  }
}

BENCHMARK_TEMPLATE(BM_Initialize, float)->Range(1, 4000);
BENCHMARK_TEMPLATE(BM_Initialize, double)->Range(1, 4000);
BENCHMARK_TEMPLATE(BM_Initialize, OptionalValue<float>)->Range(1, 4000);
BENCHMARK_TEMPLATE(BM_Initialize, std::shared_ptr<void>)->Range(1, 4000);
BENCHMARK_TEMPLATE(BM_Initialize, std::string)->Range(1, 4000);

}  // namespace arolla
