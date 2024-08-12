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
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {
namespace {

void BM_LoadAccessorsFromStruct(::benchmark::State& state) {
  struct StructInput {
    float x0;
    float x1;
    float x2;
    float x3;
    float x4;
  };

  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       CreateAccessorsInputLoader<StructInput>(
                           "x0", [](const auto& i) { return i.x0; },  //
                           "x1", [](const auto& i) { return i.x1; },  //
                           "x2", [](const auto& i) { return i.x2; },  //
                           "x3", [](const auto& i) { return i.x3; },  //
                           "x4", [](const auto& i) { return i.x4; }));

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<float>();
  auto x1_slot = layout_builder.AddSlot<float>();
  auto x2_slot = layout_builder.AddSlot<float>();
  auto x3_slot = layout_builder.AddSlot<float>();
  auto x4_slot = layout_builder.AddSlot<float>();

  auto bound_input_loader = input_loader
                                ->Bind({
                                    {"x0", TypedSlot::FromSlot(x0_slot)},
                                    {"x1", TypedSlot::FromSlot(x1_slot)},
                                    {"x2", TypedSlot::FromSlot(x2_slot)},
                                    {"x3", TypedSlot::FromSlot(x3_slot)},
                                    {"x4", TypedSlot::FromSlot(x4_slot)},
                                })
                                .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  StructInput r{0.0, 0.1, 0.2, 0.3, 0.4};
  while (state.KeepRunningBatch(5)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

BENCHMARK(BM_LoadAccessorsFromStruct);

void BM_LoadAccessorsFromMap(::benchmark::State& state) {
  using Input = absl::flat_hash_map<std::string, float>;

  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       CreateAccessorsInputLoader<Input>(
                           "x0",
                           [](const auto& i) {
                             auto it = i.find("x0");
                             return it == i.end() ? 0.0f : it->second;
                           },  //
                           "x1",
                           [](const auto& i) {
                             auto it = i.find("x1");
                             return it == i.end() ? 0.0f : it->second;
                           },  //
                           "x2",
                           [](const auto& i) {
                             auto it = i.find("x2");
                             return it == i.end() ? 0.0f : it->second;
                           },  //
                           "x3",
                           [](const auto& i) {
                             auto it = i.find("x3");
                             return it == i.end() ? 0.0f : it->second;
                           },  //
                           "x4",
                           [](const auto& i) {
                             auto it = i.find("x4");
                             return it == i.end() ? 0.0f : it->second;
                           }));

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<float>();
  auto x1_slot = layout_builder.AddSlot<float>();
  auto x2_slot = layout_builder.AddSlot<float>();
  auto x3_slot = layout_builder.AddSlot<float>();
  auto x4_slot = layout_builder.AddSlot<float>();

  auto bound_input_loader = input_loader
                                ->Bind({
                                    {"x0", TypedSlot::FromSlot(x0_slot)},
                                    {"x1", TypedSlot::FromSlot(x1_slot)},
                                    {"x2", TypedSlot::FromSlot(x2_slot)},
                                    {"x3", TypedSlot::FromSlot(x3_slot)},
                                    {"x4", TypedSlot::FromSlot(x4_slot)},
                                })
                                .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  Input r{{"x0", 0.0}, {"x1", 0.1}, {"x2", 0.2}, {"x3", 0.3}, {"x4", 0.4}};
  while (state.KeepRunningBatch(5)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

BENCHMARK(BM_LoadAccessorsFromMap);

void BM_LoadWildcardFromMap(::benchmark::State& state) {
  using Input = absl::flat_hash_map<std::string, float>;

  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       WildcardInputLoader<Input>::Build(
                           [](const auto& i, const std::string& k) {
                             auto it = i.find(k);
                             return it == i.end() ? 0.0f : it->second;
                           }));

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<float>();
  auto x1_slot = layout_builder.AddSlot<float>();
  auto x2_slot = layout_builder.AddSlot<float>();
  auto x3_slot = layout_builder.AddSlot<float>();
  auto x4_slot = layout_builder.AddSlot<float>();

  auto bound_input_loader = input_loader
                                ->Bind({
                                    {"x0", TypedSlot::FromSlot(x0_slot)},
                                    {"x1", TypedSlot::FromSlot(x1_slot)},
                                    {"x2", TypedSlot::FromSlot(x2_slot)},
                                    {"x3", TypedSlot::FromSlot(x3_slot)},
                                    {"x4", TypedSlot::FromSlot(x4_slot)},
                                })
                                .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  Input r{{"x0", 0.0}, {"x1", 0.1}, {"x2", 0.2}, {"x3", 0.3}, {"x4", 0.4}};
  while (state.KeepRunningBatch(5)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

BENCHMARK(BM_LoadWildcardFromMap);

}  // namespace
}  // namespace arolla
