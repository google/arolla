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
#include <array>

#include "benchmark/benchmark.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

void BM_IsScalarQType(benchmark::State& state) {
  InitArolla();
  const std::array qtypes = {
      GetQType<int>(),
      GetOptionalQType<int>(),
      GetArrayQType<int>(),
      GetDenseArrayQType<int>(),
  };
  int i = 0;
  for (auto _ : state) {
    auto input = qtypes[i];
    benchmark::DoNotOptimize(input);
    auto output = IsScalarQType(input);
    benchmark::DoNotOptimize(output);
    i = (i + 1) % qtypes.size();
  }
}

void BM_GetShapeQType(benchmark::State& state) {
  InitArolla();
  const std::array qtypes = {
      GetQType<int>(),
      GetOptionalQType<int>(),
      GetArrayQType<int>(),
      GetDenseArrayQType<int>(),
  };
  int i = 0;
  for (auto _ : state) {
    auto input = qtypes[i];
    benchmark::DoNotOptimize(input);
    auto output = GetShapeQType(input);
    benchmark::DoNotOptimize(output);
    i = (i + 1) % qtypes.size();
  }
}

void BM_GetScalarQType(benchmark::State& state) {
  InitArolla();
  const std::array qtypes = {
      GetQType<int>(),
      GetOptionalQType<int>(),
      GetArrayQType<int>(),
      GetDenseArrayQType<int>(),
  };
  int i = 0;
  for (auto _ : state) {
    auto input = qtypes[i];
    benchmark::DoNotOptimize(input);
    auto output = GetScalarQType(input);
    benchmark::DoNotOptimize(output);
    i = (i + 1) % qtypes.size();
  }
}

void BM_WithScalarQType(benchmark::State& state) {
  InitArolla();
  std::array qtypes = {
      *GetShapeQType(GetQType<int>()),
      *GetShapeQType(GetOptionalQType<int>()),
      *GetShapeQType(GetArrayQType<int>()),
      *GetShapeQType(GetDenseArrayQType<int>()),
  };
  const auto int_qtype = GetQType<int>();
  int i = 0;
  for (auto _ : state) {
    auto input_1 = qtypes[i];
    auto input_2 = int_qtype;
    benchmark::DoNotOptimize(input_1);
    benchmark::DoNotOptimize(input_2);
    auto output = input_1->WithValueQType(input_2);
    benchmark::DoNotOptimize(output);
    i = (i + 1) % qtypes.size();
  }
}

void BM_CommonQType(benchmark::State& state) {
  InitArolla();
  const std::array qtypes = {
      GetQType<int>(),
      GetOptionalQType<int>(),
      GetArrayQType<int>(),
      GetDenseArrayQType<int>(),
  };
  int i = 0;
  for (auto _ : state) {
    auto input_1 = qtypes[i];
    auto input_2 = qtypes[(i + 1) % qtypes.size()];
    benchmark::DoNotOptimize(input_1);
    benchmark::DoNotOptimize(input_2);
    auto output = CommonQType(input_1, input_2, true);
    benchmark::DoNotOptimize(output);
    i = (i + 1) % qtypes.size();
  }
}

BENCHMARK(BM_IsScalarQType);
BENCHMARK(BM_GetShapeQType);
BENCHMARK(BM_GetScalarQType);
BENCHMARK(BM_WithScalarQType);
BENCHMARK(BM_CommonQType);

}  // namespace
}  // namespace arolla
