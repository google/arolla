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
#include "absl//time/time.h"
#include "arolla/util/cancellation_context.h"

namespace arolla {
namespace {

template <int kDecrement>
void BM_CancellationContext_SoftCheck(benchmark::State& state) {
  const auto cancel_ctx =
      CancellationContext::Make(absl::Milliseconds(10), nullptr);
  for (auto _ : state) {
    benchmark::DoNotOptimize(cancel_ctx->SoftCheck(kDecrement));
  }
}

BENCHMARK(BM_CancellationContext_SoftCheck<1>);
BENCHMARK(BM_CancellationContext_SoftCheck<2>);
BENCHMARK(BM_CancellationContext_SoftCheck<4>);
BENCHMARK(BM_CancellationContext_SoftCheck<8>);
BENCHMARK(BM_CancellationContext_SoftCheck<-1>);

}  // namespace
}  // namespace arolla
