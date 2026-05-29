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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

#include "benchmark/benchmark.h"
#include "arolla/util/class_info.h"
#include "arolla/util/fast_dynamic_downcast_final.h"

namespace arolla {
namespace {

class Base {
 public:
  virtual ~Base() = default;

 protected:
  explicit Base(ClassInfo class_info) : class_info_(class_info) {}

 private:
  ClassInfo class_info_;

  AROLLA_DECLARE_ROOT_CLASS_INFO(Base, class_info_);
};

class Afinal final : public Base {
 public:
  Afinal() : Base(GetClassInfo<Afinal>()) {}

  AROLLA_DECLARE_SUBCLASS_INFO(Afinal, Base);
};

class B : public Base {
 protected:
  explicit B(ClassInfo class_info) : Base(class_info) {}

  AROLLA_DECLARE_SUBCLASS_INFO(B, Base);
};

class C : public B {
 protected:
  explicit C(ClassInfo class_info) : B(class_info) {}

  AROLLA_DECLARE_SUBCLASS_INFO(C, B);
};

class Dfinal final : public C {
 public:
  Dfinal() : C(GetClassInfo<Dfinal>()) {}

  AROLLA_DECLARE_SUBCLASS_INFO(Dfinal, C);
};

std::vector<std::unique_ptr<Base>> MixInputs(size_t size) {
  auto gen = std::mt19937(34);
  std::vector<std::unique_ptr<Base>> result(size);
  std::uniform_int_distribution<int> int01(0, 1);
  for (auto& x : result) {
    switch (int01(gen)) {
      case 0:
        x = std::make_unique<Afinal>();
        break;
      case 1:
        x = std::make_unique<Dfinal>();
        break;
    }
  }
  return result;
}

template <typename T>
void BM_FastDowncastTo(benchmark::State& state) {
  auto inputs = MixInputs(1 << 16);
  uint16_t i = 0;
  for (auto _ : state) {
    auto* input = inputs[i++].get();
    auto* output = FastDowncast<T>(input);
    benchmark::DoNotOptimize(output);
  }
}

template <typename T>
void BM_DynamicCastTo(benchmark::State& state) {
  auto inputs = MixInputs(1 << 16);
  uint16_t i = 0;
  for (auto _ : state) {
    auto* input = inputs[i++].get();
    auto* output = dynamic_cast<T*>(input);
    benchmark::DoNotOptimize(output);
  }
}

template <typename T>
void BM_FastDynamicDowncastFinalTo(benchmark::State& state) {
  auto inputs = MixInputs(1 << 16);
  uint16_t i = 0;
  for (auto _ : state) {
    auto* input = inputs[i++].get();
    auto* output = fast_dynamic_downcast_final<T*>(input);
    benchmark::DoNotOptimize(output);
  }
}

BENCHMARK_TEMPLATE(BM_FastDowncastTo, Base);
BENCHMARK_TEMPLATE(BM_FastDowncastTo, Afinal);
BENCHMARK_TEMPLATE(BM_FastDowncastTo, B);
BENCHMARK_TEMPLATE(BM_FastDowncastTo, C);
BENCHMARK_TEMPLATE(BM_FastDowncastTo, Dfinal);

BENCHMARK_TEMPLATE(BM_DynamicCastTo, Base);
BENCHMARK_TEMPLATE(BM_DynamicCastTo, Afinal);
BENCHMARK_TEMPLATE(BM_DynamicCastTo, B);
BENCHMARK_TEMPLATE(BM_DynamicCastTo, C);
BENCHMARK_TEMPLATE(BM_DynamicCastTo, Dfinal);

BENCHMARK_TEMPLATE(BM_FastDynamicDowncastFinalTo, Afinal);
BENCHMARK_TEMPLATE(BM_FastDynamicDowncastFinalTo, Dfinal);

}  // namespace
}  // namespace arolla
