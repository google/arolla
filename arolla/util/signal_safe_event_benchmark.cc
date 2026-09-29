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
#include <atomic>
#include <thread>  // NOLINT(build/c++11)

#include "benchmark/benchmark.h"
#include "absl/base/no_destructor.h"
#include "arolla/util/signal_safe_event.h"

namespace arolla {
namespace {

void BM_Notify_NoWaiter(benchmark::State& state) {
  static absl::NoDestructor<SignalSafeEvent> event;
  for (auto _ : state) {
    event->Notify();
  }
}

BENCHMARK(BM_Notify_NoWaiter)->ThreadRange(1, 8);

void BM_TryConsume_NoEvent(benchmark::State& state) {
  SignalSafeEvent event;
  for (auto _ : state) {
    benchmark::DoNotOptimize(event.TryConsume());
  }
}

BENCHMARK(BM_TryConsume_NoEvent);

void BM_NotifyAndWaitAndConsume_NoBlocking(benchmark::State& state) {
  SignalSafeEvent event;
  for (auto _ : state) {
    event.Notify();
    event.WaitAndConsume();
  }
}

BENCHMARK(BM_NotifyAndWaitAndConsume_NoBlocking);

void BM_Notify_WithWaiter(benchmark::State& state) {
  SignalSafeEvent event;
  std::atomic<bool> stop{false};
  std::thread consumer([&] {
    while (!stop.load()) {
      event.WaitAndConsume();
    }
  });
  for (auto _ : state) {
    event.Notify();
  }
  stop.store(true);
  event.Notify();
  consumer.join();
}

BENCHMARK(BM_Notify_WithWaiter);

void BM_WaitAndConsume_WithNotifier(benchmark::State& state) {
  SignalSafeEvent event;
  std::atomic<bool> stop{false};
  std::thread producer([&] {
    while (!stop.load()) {
      event.Notify();
    }
  });
  for (auto _ : state) {
    event.WaitAndConsume();
  }
  stop.store(true);
  producer.join();
}

BENCHMARK(BM_WaitAndConsume_WithNotifier);

}  // namespace
}  // namespace arolla
