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
#include "benchmark/benchmark.h"
#include "arolla/util/refcount.h"

namespace arolla {
namespace {

void BM_Refcount_Increment(benchmark::State& state) {
  Refcount refcount;
  for (auto _ : state) {
    refcount.increment();
    benchmark::DoNotOptimize(refcount);
  }
}

BENCHMARK(BM_Refcount_Increment);

void BM_Refcount_Decrement(benchmark::State& state) {
  Refcount refcount(Refcount::TestOnly{}, 2'000'000'000);
  for (auto _ : state) {
    bool tmp = refcount.decrement();
    benchmark::DoNotOptimize(tmp);
  }
}

BENCHMARK(BM_Refcount_Decrement);

void BM_Refcount_SkewedDecrement_Last(benchmark::State& state) {
  Refcount refcount;
  for (auto _ : state) {
    bool tmp = refcount.skewed_decrement();
    benchmark::DoNotOptimize(tmp);
  }
}

BENCHMARK(BM_Refcount_SkewedDecrement_Last);

void BM_Refcount_SkewedDecrement_NonLast(benchmark::State& state) {
  Refcount refcount(Refcount::TestOnly{}, 2'000'000'000);
  for (auto _ : state) {
    bool tmp = refcount.skewed_decrement();
    benchmark::DoNotOptimize(tmp);
  }
}

BENCHMARK(BM_Refcount_SkewedDecrement_NonLast);

}  // namespace
}  // namespace arolla
