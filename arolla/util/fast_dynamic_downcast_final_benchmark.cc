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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <typeinfo>
#include <vector>

#include "benchmark/benchmark.h"
#include "arolla/util/fast_dynamic_downcast_final.h"

namespace arolla {
namespace {

// Helper classes with non-trivial names.
class FastDynamicDownCastFinalTestClassA {
 public:
  virtual ~FastDynamicDownCastFinalTestClassA() = default;
};

class FastDynamicDownCastFinalTestClassB final
    : public FastDynamicDownCastFinalTestClassA {};
class FastDynamicDownCastFinalTestClassC
    : public FastDynamicDownCastFinalTestClassA {};
class FastDynamicDownCastFinalTestClassD final
    : public FastDynamicDownCastFinalTestClassC {};

using A = FastDynamicDownCastFinalTestClassA;
using B = FastDynamicDownCastFinalTestClassB;
using C = FastDynamicDownCastFinalTestClassC;
using D = FastDynamicDownCastFinalTestClassD;

std::vector<std::unique_ptr<A>> MixABCDs(size_t size) {
  auto gen = std::mt19937(34);
  std::vector<std::unique_ptr<A>> result(size);
  std::uniform_int_distribution<int> int03(0, 3);
  for (auto& a : result) {
    switch (int03(gen)) {
      case 0:
        a = std::make_unique<A>();
        break;
      case 1:
        a = std::make_unique<B>();
        break;
      case 2:
        a = std::make_unique<C>();
        break;
      case 3:
        a = std::make_unique<D>();
        break;
    }
  }
  return result;
}

void BM_CheckStdDynamicCast(benchmark::State& state) {
  auto inputs = MixABCDs(1 << 16);
  uint16_t i = 0;
  int sum = 0;
  for (auto _ : state) {
    auto* const input = inputs[i++].get();
    if (dynamic_cast<B*>(input) != nullptr) {
      sum += 1;
    } else if (dynamic_cast<D*>(input) != nullptr) {
      sum += 2;
    }
  }
  benchmark::DoNotOptimize(sum);
}

void BM_CheckStdTypeId(benchmark::State& state) {
  auto inputs = MixABCDs(1 << 16);
  uint16_t i = 0;
  int sum = 0;
  for (auto _ : state) {
    auto* const input = inputs[i++].get();
    auto& input_id = typeid(*input);
    if (input_id == typeid(B)) {
      sum += 1;
    } else if (input_id == typeid(D)) {
      sum += 2;
    }
  }
  benchmark::DoNotOptimize(sum);
}

void BM_CheckArollaFastDynamicDownCastFinal(benchmark::State& state) {
  auto inputs = MixABCDs(1 << 16);
  uint16_t i = 0;
  int sum = 0;
  for (auto _ : state) {
    auto* const input = inputs[i++].get();
    if (fast_dynamic_downcast_final<B*>(input) != nullptr) {
      sum += 1;
    } else if (fast_dynamic_downcast_final<D*>(input) != nullptr) {
      sum += 2;
    }
  }
  benchmark::DoNotOptimize(sum);
}

BENCHMARK(BM_CheckStdDynamicCast);
BENCHMARK(BM_CheckStdTypeId);
BENCHMARK(BM_CheckArollaFastDynamicDownCastFinal);

}  // namespace
}  // namespace arolla
