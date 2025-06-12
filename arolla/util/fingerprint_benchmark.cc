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
#include "benchmark/benchmark.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace {

void BM_RandomFingerprint(benchmark::State& state) {
  for (auto s : state) {
    auto fgpt = RandomFingerprint();
    benchmark::DoNotOptimize(fgpt);
  }
}

BENCHMARK(BM_RandomFingerprint)->Threads(1);
BENCHMARK(BM_RandomFingerprint)->Threads(2);
BENCHMARK(BM_RandomFingerprint)->Threads(4);
BENCHMARK(BM_RandomFingerprint)->Threads(8);

}  // namespace
}  // namespace arolla
