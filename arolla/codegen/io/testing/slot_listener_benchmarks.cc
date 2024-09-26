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

#include <memory>

#include "benchmark/benchmark.h"
#include "arolla/codegen/io/testing/benchmark_nested_proto_with_extension_slot_listener.h"
#include "arolla/codegen/io/testing/benchmark_proto_slot_listener.h"
#include "arolla/codegen/io/testing/test_proto_slot_listener.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/proto/testing/benchmark_util.h"
#include "arolla/proto/testing/test.pb.h"

namespace arolla {
namespace {

void BM_WriteScalarsIntoProto(::benchmark::State& state) {
  const auto& slot_listener = ::my_namespace::GetBenchProtoSlotListener();
  testing::benchmark::WriteScalarsIntoProto(*slot_listener, state);
}

BENCHMARK(BM_WriteScalarsIntoProto);

void BM_WriteScalarsIntoProtoWithManyUnusedFields(::benchmark::State& state) {
  const auto& slot_listener = ::my_namespace::GetProtoSlotListener();
  testing::benchmark::WriteScalarsIntoProto(*slot_listener, state);
}

BENCHMARK(BM_WriteScalarsIntoProtoWithManyUnusedFields);

void BM_WriteScalarsIntoNestedProtoWithExtensions(::benchmark::State& state) {
  const auto& slot_listener =
      ::my_namespace::GetBenchNestedProtoWithExtensionsSlotListener();
  testing::benchmark::WriteScalarsIntoProto(
      *slot_listener, state,
      "/self_reference/"
      "Ext::testing_extension_namespace.BenchmarkExtension.bench_ext");
}

BENCHMARK(BM_WriteScalarsIntoNestedProtoWithExtensions);

}  // namespace
}  // namespace arolla
