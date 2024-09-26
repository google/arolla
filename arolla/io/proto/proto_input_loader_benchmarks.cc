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
#include "gmock/gmock.h"
#include "arolla/io/proto/proto_input_loader.h"
#include "arolla/io/proto/testing/benchmark_util.h"
#include "arolla/proto/testing/test.pb.h"

namespace arolla {
namespace {

void BM_LoadProtoIntoScalars(::benchmark::State& state) {
  ASSERT_OK_AND_ASSIGN(
      auto input_loader,
      ProtoFieldsLoader::Create(::testing_namespace::Root::descriptor()));
  testing::benchmark::LoadProtoIntoScalars(input_loader, state);
}

BENCHMARK(BM_LoadProtoIntoScalars);

}  // namespace
}  // namespace arolla
