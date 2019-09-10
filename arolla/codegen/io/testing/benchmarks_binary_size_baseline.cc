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
// A benchmark doing some trivial operations on input loader and slot listener.
// Serves as a baseline for generated input loader / slot listener
// WeightWatchers.

#include <memory>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

void BM_ScalarLoader(::benchmark::State& state) {
  auto input_loader = CreateAccessorsInputLoader<int>("x0", [](int x) {
                        return CreateDenseArray<int>({x});
                      }).value();

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto bound_input_loader =
      input_loader->Bind({{"x0", TypedSlot::FromSlot(x0_slot)}}).value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  int v = {57};
  for (auto s : state) {
    ::benchmark::DoNotOptimize(v);
    CHECK_OK(bound_input_loader(v, frame));
  }
}

BENCHMARK(BM_ScalarLoader);

void BM_ScalarListener(::benchmark::State& state) {
  auto slot_listener =
      CreateAccessorsSlotListener<int>("x0", [](const DenseArray<int>& x,
                                                int* result) {
        *result = x[0].present ? x[0].value : -1;
      }).value();

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto bound_slot_listener =
      slot_listener->Bind({{"x0", TypedSlot::FromSlot(x0_slot)}}).value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  int v;
  frame.Set(x0_slot, CreateDenseArray<int>({57}));
  for (auto s : state) {
    ::benchmark::DoNotOptimize(v);
    CHECK_OK(bound_slot_listener(frame, &v));
  }
}

BENCHMARK(BM_ScalarListener);

}  // namespace
}  // namespace arolla
