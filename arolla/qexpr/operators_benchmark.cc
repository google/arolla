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
#include "absl/strings/str_format.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/testing/benchmarks.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

template <typename T>
constexpr T kInitialValue = T{57.07};

// Microbenchmark for evaluation of a tree of "Add" operators.
template <typename T>
void BM_Add(benchmark::State& state) {
  InitArolla();
  int num_inputs = state.range(0);
  bool shuffle = state.range(1) != 0;
  auto qtype = GetQType<T>();
  state.SetLabel(absl::StrFormat("qtype:%s inputs:%d order:%s", qtype->name(),
                                 num_inputs, shuffle ? "shuffled" : "direct"));

  auto add_op = OperatorRegistry::GetInstance()
                    ->LookupOperator("test.add", {qtype, qtype}, qtype)
                    .value();
  auto initial_value = TypedValue::FromValue(kInitialValue<T>);
  testing::BenchmarkBinaryOperator(*add_op, num_inputs, initial_value,
                                   /*common_inputs=*/{}, shuffle, state);
}

void RunBenchmark(benchmark::internal::Benchmark* b) {
  for (int num_inputs : {2, 32, 1 << 15, 1 << 20}) {
    for (int shuffle : {0, 1}) {
      b->Args({num_inputs, shuffle});
    }
  }
}

void BM_AddFloat(benchmark::State& state) { BM_Add<float>(state); }
void BM_AddDouble(benchmark::State& state) { BM_Add<double>(state); }

BENCHMARK(BM_AddFloat)->Apply(&RunBenchmark);
BENCHMARK(BM_AddDouble)->Apply(&RunBenchmark);

}  // namespace
}  // namespace arolla
