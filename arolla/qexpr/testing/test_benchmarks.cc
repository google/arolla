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
#include <cstdint>

#include "benchmark/benchmark.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/testing/benchmarks.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::testing {
namespace {

void BM_SmokeTest(benchmark::State& state) {
  auto add_op = QExprOperatorFromFunction([](int32_t a, int32_t b) {
                  return a + b;
                }).value();
  testing::BenchmarkBinaryOperator(*add_op, /*num_inputs=*/57,
                                   /*input_value=*/TypedValue::FromValue(1),
                                   /*common_inputs=*/{},
                                   /*shuffle=*/true, state);
}

BENCHMARK(BM_SmokeTest);

}  // namespace
}  // namespace arolla::testing
