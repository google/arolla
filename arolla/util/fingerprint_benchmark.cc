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
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/strings/string_view.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace {

void BM_RandomFingerprint(benchmark::State& state) {
  for (auto _ : state) {
    auto fgpt = RandomFingerprint();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_RandomFingerprint)->Threads(1);
BENCHMARK(BM_RandomFingerprint)->Threads(2);
BENCHMARK(BM_RandomFingerprint)->Threads(4);
BENCHMARK(BM_RandomFingerprint)->Threads(8);

void BM_CombineInts(benchmark::State& state) {
  const int n = state.range(0);
  for (auto _ : state) {
    FingerprintHasher hasher("bench-salt");
    for (int i = 0; i < n; ++i) {
      hasher.Combine(i);
    }
    auto fgpt = std::move(hasher).Finish();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_CombineInts)->Arg(1)->Arg(4)->Arg(8)->Arg(16)->Arg(32);

void BM_CombineDoubles(benchmark::State& state) {
  const int n = state.range(0);
  for (auto _ : state) {
    FingerprintHasher hasher("bench-salt");
    for (int i = 0; i < n; ++i) {
      hasher.Combine(static_cast<double>(i));
    }
    auto fgpt = std::move(hasher).Finish();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_CombineDoubles)->Arg(1)->Arg(4)->Arg(8)->Arg(16)->Arg(32);

void BM_CombineFingerprints(benchmark::State& state) {
  const int n = state.range(0);
  std::vector<Fingerprint> fps(n);
  for (int i = 0; i < n; ++i) {
    fps[i] = RandomFingerprint();
  }
  for (auto _ : state) {
    FingerprintHasher hasher("bench-salt");
    for (int i = 0; i < n; ++i) {
      hasher.Combine(fps[i]);
    }
    auto fgpt = std::move(hasher).Finish();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_CombineFingerprints)->Arg(1)->Arg(4)->Arg(8)->Arg(16);

void BM_CombineRawBytes(benchmark::State& state) {
  const int size = state.range(0);
  std::string data(size, 'x');
  for (auto _ : state) {
    FingerprintHasher hasher("bench-salt");
    hasher.CombineRawBytes(data.data(), data.size());
    auto fgpt = std::move(hasher).Finish();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_CombineRawBytes)->Arg(4)->Arg(16)->Arg(64)->Arg(256)->Arg(1024);

void BM_CombineRawBytesManySmall(benchmark::State& state) {
  const int n = state.range(0);
  int32_t val = 42;
  for (auto _ : state) {
    FingerprintHasher hasher("bench-salt");
    for (int i = 0; i < n; ++i) {
      hasher.CombineRawBytes(&val, sizeof(val));
    }
    auto fgpt = std::move(hasher).Finish();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_CombineRawBytesManySmall)
    ->Arg(1)
    ->Arg(4)
    ->Arg(8)
    ->Arg(16)
    ->Arg(32);

// Benchmarks Combine with a mix of types — simulates a typical struct
// fingerprint (int, int, double, string_view, bool).
void BM_CombineMixedTypes(benchmark::State& state) {
  int x = 10;
  int y = 20;
  double radius = 3.14;
  absl::string_view name = "example";
  bool flag = true;
  for (auto _ : state) {
    FingerprintHasher hasher("bench-salt");
    hasher.Combine(x, y, radius, name, flag);
    auto fgpt = std::move(hasher).Finish();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_CombineMixedTypes);

void BM_FingerprintOfString(benchmark::State& state) {
  const int size = state.range(0);
  std::string data(size, 'a');
  for (auto _ : state) {
    auto fgpt = FingerprintOfString(data);
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_FingerprintOfString)->Arg(8)->Arg(64)->Arg(256)->Arg(1024);

void BM_FingerprintOfBytes(benchmark::State& state) {
  const int size = state.range(0);
  std::string data(size, 'b');
  for (auto _ : state) {
    auto fgpt = FingerprintOfBytes(data.data(), data.size());
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_FingerprintOfBytes)->Arg(8)->Arg(64)->Arg(256)->Arg(1024);

}  // namespace
}  // namespace arolla
