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
#include <vector>

#include "benchmark/benchmark.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/proto/testing/test_extension.pb.h"

namespace {

void BM_FieldAccess(::benchmark::State& state) {
  ::testing_namespace::Root r1;
  r1.set_x(1);
  r1.set_x0(2);
  r1.set_x1(3);
  ::testing_namespace::Root r2;
  r2.set_x(1);
  while (state.KeepRunningBatch(6)) {
    for (const auto* r : {&r1, &r2}) {
      ::benchmark::DoNotOptimize(r);
      int sum = 0;
      if (r->has_x()) {
        sum += r->x();
      }
      if (r->has_x0()) {
        sum += r->x0();
      }
      if (r->has_x1()) {
        sum += r->x1();
      }
      ::benchmark::DoNotOptimize(sum);
    }
  }
}

BENCHMARK(BM_FieldAccess);

void BM_IndexAccess(::benchmark::State& state) {
  ::testing_namespace::Root r;
  r.add_ys(1);
  r.add_ys(2);
  r.add_ys(3);
  r.add_ys(4);
  while (state.KeepRunningBatch(6)) {
    ::benchmark::DoNotOptimize(r);
    int sum = 0;
    for (int i = 0; i < 6; ++i) {
      if (r.ys_size() > i) {
        sum += r.ys(i);
      } else {
        sum += i;
      }
      ::benchmark::DoNotOptimize(sum);
    }
  }
}

BENCHMARK(BM_IndexAccess);

void BM_SizeAccess(::benchmark::State& state) {
  ::testing_namespace::Root r;
  r.add_ys(1);
  r.add_ys(2);
  r.add_ys(3);
  r.add_ys(4);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(r);
    auto s = r.ys_size();
    ::benchmark::DoNotOptimize(s);
  }
}

BENCHMARK(BM_SizeAccess);

void BM_ExtensionAccess(::benchmark::State& state) {
  ::testing_namespace::Root r;
  r.mutable_inner()
      ->MutableExtension(testing_extension_namespace::InnerExtension::inner_ext)
      ->set_inner_extension_x_int32(5);
  auto& inner = *r.mutable_inner();
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(inner);
    auto x = inner
                 .GetExtension(
                     testing_extension_namespace::InnerExtension::inner_ext)
                 .inner_extension_x_int32();
    ::benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ExtensionAccess);

void BM_MapAccess(::benchmark::State& state) {
  ::testing_namespace::Root r;
  r.mutable_map_int()->insert({"a", 5});
  r.mutable_map_int()->insert({"b", 7});
  while (state.KeepRunningBatch(3)) {
    ::benchmark::DoNotOptimize(r);
    int sum = 0;
    if (r.map_int().contains("a")) {
      sum += r.map_int().at("a");
    }
    if (r.map_int().contains("b")) {
      sum += r.map_int().at("b");
    }
    if (r.map_int().contains("c")) {
      sum += r.map_int().at("c");
    }
    ::benchmark::DoNotOptimize(sum);
  }
}

BENCHMARK(BM_MapAccess);

void BM_StdVectorReservedPushBack(::benchmark::State& state) {
  std::vector<::testing_namespace::Root*> ptrs;
  ::testing_namespace::Root r;
  constexpr int kSize = 100;
  ptrs.reserve(kSize);
  while (state.KeepRunningBatch(kSize)) {
    ::benchmark::DoNotOptimize(ptrs);
    for (int i = 0; i != kSize; ++i) {
      ::benchmark::DoNotOptimize(r);
      ptrs.push_back(&r);
    }
    ptrs.clear();
  }
}

BENCHMARK(BM_StdVectorReservedPushBack);

}  // namespace
