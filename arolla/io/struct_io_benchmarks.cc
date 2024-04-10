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
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "arolla/io/struct_io.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

template<typename T, bool InverseOrder>
void BM_StructInputLoader(::benchmark::State& state) {
  constexpr size_t kBufSize = 1024;
  struct FakeStruct { char buf[kBufSize]; };
  absl::flat_hash_map<std::string, TypedSlot> struct_slots;
  absl::flat_hash_map<std::string, TypedSlot> frame_slots;
  int N = kBufSize / sizeof(T);
  FrameLayout::Builder bldr;
  for (int i = 0; i < N; ++i) {
    size_t struct_offset = (InverseOrder ? N - i - 1 : i) * sizeof(T);
    struct_slots.emplace(
        absl::StrFormat("x%d", i),
        TypedSlot::UnsafeFromOffset(GetQType<T>(), struct_offset));
    frame_slots.emplace(absl::StrFormat("x%d", i),
                         TypedSlot::FromSlot(bldr.AddSlot<T>()));
  }
  auto layout = std::move(bldr).Build();

  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       StructInputLoader<FakeStruct>::Create(struct_slots));
  ASSERT_OK_AND_ASSIGN(auto bound_loader, input_loader->Bind(frame_slots));

  MemoryAllocation alloc(&layout);
  FramePtr frame = alloc.frame();
  FakeStruct fs;
  for (size_t i = 0; i < kBufSize; ++i) {
    fs.buf[i] = i & 0xff;
  }

  while (state.KeepRunningBatch(N)) {
    benchmark::DoNotOptimize(fs);
    benchmark::DoNotOptimize(frame);
    CHECK_OK(bound_loader(fs, frame));
    benchmark::DoNotOptimize(frame);
  }
}

template<typename T>
void BM_StructInputLoader_ordered(::benchmark::State& state) {
  BM_StructInputLoader<T, false>(state);
}

template<typename T>
void BM_StructInputLoader_unordered(::benchmark::State& state) {
  BM_StructInputLoader<T, true>(state);
}

BENCHMARK(BM_StructInputLoader_ordered<bool>);
BENCHMARK(BM_StructInputLoader_ordered<int32_t>);
BENCHMARK(BM_StructInputLoader_ordered<int64_t>);
BENCHMARK(BM_StructInputLoader_ordered<OptionalValue<int32_t>>);
BENCHMARK(BM_StructInputLoader_ordered<OptionalValue<int64_t>>);

BENCHMARK(BM_StructInputLoader_unordered<bool>);
BENCHMARK(BM_StructInputLoader_unordered<int32_t>);
BENCHMARK(BM_StructInputLoader_unordered<int64_t>);
BENCHMARK(BM_StructInputLoader_unordered<OptionalValue<int32_t>>);
BENCHMARK(BM_StructInputLoader_unordered<OptionalValue<int64_t>>);

}  // namespace
}  // namespace arolla
