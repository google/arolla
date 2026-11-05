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
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/base/no_destructor.h"
#include "arolla/util/cancellation.h"

namespace arolla {
namespace {

void BM_CancellationContext_Cancelled(benchmark::State& state) {
  auto cancellation_context = CancellationContext::Make();
  for (auto _ : state) {
    benchmark::DoNotOptimize(cancellation_context->Cancelled());
  }
}

BENCHMARK(BM_CancellationContext_Cancelled);

void BM_CheckCancellation(benchmark::State& state) {
  static absl::NoDestructor cancellation_context(CancellationContext::Make());
  CancellationContext::ScopeGuard cancellation_scope(*cancellation_context);
  for (auto _ : state) {
    benchmark::DoNotOptimize(CheckCancellation());
  }
}

BENCHMARK(BM_CheckCancellation)->ThreadRange(1, 64);

void BM_CheckCancellation_NoCancellationContext(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(CheckCancellation());
  }
}

BENCHMARK(BM_CheckCancellation_NoCancellationContext);

void BM_Cancelled(benchmark::State& state) {
  static absl::NoDestructor cancellation_context(CancellationContext::Make());
  CancellationContext::ScopeGuard cancellation_scope(*cancellation_context);
  for (auto _ : state) {
    benchmark::DoNotOptimize(Cancelled());
  }
}

BENCHMARK(BM_Cancelled)->ThreadRange(1, 64);

void BM_Cancelled_NoCancellationContext(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(Cancelled());
  }
}

BENCHMARK(BM_Cancelled_NoCancellationContext);

void BM_SubscribeAndUnsubscribe(benchmark::State& state) {
  const int n = state.range(0);
  auto cancellation_context = CancellationContext::Make();
  std::vector<CancellationContext::Subscription> other_subscriptions(n);
  for (int i = 0; i < n; ++i) {
    other_subscriptions[i] = cancellation_context->Subscribe([] {});
  }
  for (auto _ : state) {
    auto subscription = cancellation_context->Subscribe([] {});
    benchmark::DoNotOptimize(subscription);
  }
}

BENCHMARK(BM_SubscribeAndUnsubscribe)->Arg(0)->Arg(1)->Arg(4)->Arg(16)->Arg(64);

void BM_SubscribeAndCancel(benchmark::State& state) {
  const int n = state.range(0);
  for (auto _ : state) {
    auto cancellation_context = CancellationContext::Make();
    std::vector<CancellationContext::Subscription> subscriptions(n);
    for (int i = 0; i < n; ++i) {
      subscriptions[i] = cancellation_context->Subscribe([] {});
    }
    cancellation_context->Cancel();
    benchmark::DoNotOptimize(subscriptions);
  }
}

BENCHMARK(BM_SubscribeAndCancel)->Arg(1)->Arg(4)->Arg(16)->Arg(64);

void BM_DeriveAndDestroy(benchmark::State& state) {
  const int n = state.range(0);
  auto parent = CancellationContext::Make();
  std::vector<CancellationContextPtr> other_children(n);
  for (int i = 0; i < n; ++i) {
    other_children[i] = CancellationContext::Derive(parent);
  }
  for (auto _ : state) {
    auto child = CancellationContext::Derive(parent);
    benchmark::DoNotOptimize(child);
  }
}

BENCHMARK(BM_DeriveAndDestroy)->Arg(0)->Arg(1)->Arg(4)->Arg(16)->Arg(64);

}  // namespace
}  // namespace arolla
