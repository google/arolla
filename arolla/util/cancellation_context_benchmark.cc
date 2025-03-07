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
#include "absl/time/time.h"
#include "arolla/util/cancellation_context.h"
#include "arolla/util/testing/gmock_cancellation_context.h"

namespace arolla {
namespace {

using ::arolla::testing::MockCancellationContext;
using ::arolla::testing::MockCancellationScope;

template <int kDecrement>
void BM_CancellationContext_SoftCheck(benchmark::State& state) {
  MockCancellationContext cancellation_context(absl::Milliseconds(10));
  for (auto _ : state) {
    benchmark::DoNotOptimize(cancellation_context.SoftCheck(kDecrement));
  }
}

BENCHMARK(BM_CancellationContext_SoftCheck<1>);
BENCHMARK(BM_CancellationContext_SoftCheck<2>);
BENCHMARK(BM_CancellationContext_SoftCheck<4>);
BENCHMARK(BM_CancellationContext_SoftCheck<8>);
BENCHMARK(BM_CancellationContext_SoftCheck<-1>);

template <int kDecrement>
void BM_CheckCancellation(benchmark::State& state) {
  MockCancellationScope cancellation_scope(absl::Milliseconds(10));
  for (auto _ : state) {
    benchmark::DoNotOptimize(CheckCancellation(kDecrement));
  }
}

BENCHMARK(BM_CheckCancellation<1>);
BENCHMARK(BM_CheckCancellation<2>);
BENCHMARK(BM_CheckCancellation<4>);
BENCHMARK(BM_CheckCancellation<8>);
BENCHMARK(BM_CheckCancellation<-1>);

void BM_CheckCancellation_NoCancellationContext(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(CheckCancellation());
  }
}

BENCHMARK(BM_CheckCancellation_NoCancellationContext);

template <int kDecrement>
void BM_IsCancelled(benchmark::State& state) {
  MockCancellationScope cancellation_scope(absl::Milliseconds(10));
  for (auto _ : state) {
    benchmark::DoNotOptimize(IsCancelled(kDecrement));
  }
}

BENCHMARK(BM_IsCancelled<1>);
BENCHMARK(BM_IsCancelled<2>);
BENCHMARK(BM_IsCancelled<4>);
BENCHMARK(BM_IsCancelled<8>);
BENCHMARK(BM_IsCancelled<-1>);

void BM_IsCancelled_NoCancellationContext(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(IsCancelled());
  }
}

BENCHMARK(BM_IsCancelled_NoCancellationContext);

}  // namespace
}  // namespace arolla
