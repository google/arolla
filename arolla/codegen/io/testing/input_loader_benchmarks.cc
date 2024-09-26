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
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "arolla/codegen/io/testing/benchmark_array_input_loader.h"
#include "arolla/codegen/io/testing/benchmark_dense_array_nested_proto_with_extension_input_loader.h"
#include "arolla/codegen/io/testing/benchmark_nested_proto_input_loader.h"
#include "arolla/codegen/io/testing/benchmark_nested_proto_with_extension_input_loader.h"
#include "arolla/codegen/io/testing/benchmark_proto_input_loader.h"
#include "arolla/codegen/io/testing/benchmark_proto_string_input_loader.h"
#include "arolla/codegen/io/testing/test_dense_array_repeated_proto_input_loader.h"
#include "arolla/codegen/io/testing/test_descriptor_input_loader.h"
#include "arolla/codegen/io/testing/test_repeated_proto_input_loader_with_parent_intermediate_node_collected.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/inplace_slot_builder.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/proto/testing/benchmark_util.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"

namespace arolla {
namespace {

struct BenchStruct {
  int x0;
  int x1;
  int x2;
  int x3;
  int x4;
  int x5;
  int x6;
  int x7;
  int x8;
  int x9;
};

absl::flat_hash_map<std::string, TypedSlot> GetInplaceSlots(
    FrameLayout::Slot<BenchStruct> struct_slot) {
  InplaceSlotBuilder<BenchStruct> builder;
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x0, "x0"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x1, "x1"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x2, "x2"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x3, "x3"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x4, "x4"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x5, "x5"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x6, "x6"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x7, "x7"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x8, "x8"));
  CHECK_OK(AROLLA_ADD_INPLACE_SLOT_FIELD(builder, x9, "x9"));
  return builder.OutputSlots(struct_slot);
}

void BM_ScalarInplaceLoader(::benchmark::State& state) {
  FrameLayout::Builder layout_builder;
  auto struct_slot = layout_builder.AddSlot<BenchStruct>();
  auto slots = GetInplaceSlots(struct_slot);
  CHECK_OK(RegisterUnsafeSlotsMap(slots, &layout_builder));
  auto x0_slot = slots.find("x0")->second.ToSlot<int>().value();

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  BenchStruct r = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  while (state.KeepRunningBatch(10)) {
    ::benchmark::DoNotOptimize(r);
    frame.Set(struct_slot, r);
    auto x = frame.Get(x0_slot);
    ::benchmark::DoNotOptimize(x);
  }
}

BENCHMARK(BM_ScalarInplaceLoader);

void BM_ScalarArrayLoader(::benchmark::State& state) {
  const auto& input_loader = ::my_namespace::GetBenchArrayProtoLoader();

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<int>();
  auto x1_slot = layout_builder.AddSlot<int>();
  auto x2_slot = layout_builder.AddSlot<int>();
  auto x3_slot = layout_builder.AddSlot<int>();
  auto x4_slot = layout_builder.AddSlot<int>();
  auto x5_slot = layout_builder.AddSlot<int>();
  auto x6_slot = layout_builder.AddSlot<int>();
  auto x7_slot = layout_builder.AddSlot<int>();
  auto x8_slot = layout_builder.AddSlot<int>();
  auto x9_slot = layout_builder.AddSlot<int>();
  auto bound_input_loader = input_loader
                                ->Bind({
                                    {"x0", TypedSlot::FromSlot(x0_slot)},
                                    {"x1", TypedSlot::FromSlot(x1_slot)},
                                    {"x2", TypedSlot::FromSlot(x2_slot)},
                                    {"x3", TypedSlot::FromSlot(x3_slot)},
                                    {"x4", TypedSlot::FromSlot(x4_slot)},
                                    {"x5", TypedSlot::FromSlot(x5_slot)},
                                    {"x6", TypedSlot::FromSlot(x6_slot)},
                                    {"x7", TypedSlot::FromSlot(x7_slot)},
                                    {"x8", TypedSlot::FromSlot(x8_slot)},
                                    {"x9", TypedSlot::FromSlot(x9_slot)},
                                })
                                .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  std::array<int, 10> r = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  while (state.KeepRunningBatch(10)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

BENCHMARK(BM_ScalarArrayLoader);

void BM_LoadProtoIntoScalars(::benchmark::State& state) {
  std::unique_ptr<InputLoader<testing_namespace::Root>> input_loader =
      ::my_namespace::GetBenchProtoLoader();
  testing::benchmark::LoadProtoIntoScalars(input_loader, state);
}

BENCHMARK(BM_LoadProtoIntoScalars);

void BM_LoadProtoIntoScalarsWithManyUnusedFields(::benchmark::State& state) {
  std::unique_ptr<InputLoader<testing_namespace::Root>> input_loader =
      ::my_namespace::GetDescriptorBasedLoader();
  testing::benchmark::LoadProtoIntoScalars(input_loader, state);
}

BENCHMARK(BM_LoadProtoIntoScalarsWithManyUnusedFields);

void BM_LoadNestedDepth4ProtoIntoScalars(::benchmark::State& state) {
  auto input_loader = ::my_namespace::GetBenchNestedProtoLoader();
  testing::benchmark::LoadNestedDepth4ProtoIntoScalars(input_loader, state);
}

BENCHMARK(BM_LoadNestedDepth4ProtoIntoScalars);

void BM_LoadNestedWithExtensionProtoIntoScalars(::benchmark::State& state) {
  auto input_loader = ::my_namespace::GetBenchNestedProtoWithExtensionsLoader();
  testing::benchmark::LoadNestedWithExtensionProtoIntoScalars(input_loader,
                                                              state);
}

BENCHMARK(BM_LoadNestedWithExtensionProtoIntoScalars);

void BM_LoadProtoStringIntoScalars(::benchmark::State& state) {
  using OBytes = ::arolla::OptionalValue<Bytes>;
  const auto& input_loader = ::my_namespace::GetBenchProtoStringLoader();

  FrameLayout::Builder layout_builder;
  auto str_slot = layout_builder.AddSlot<OBytes>();
  auto raw_bytes_slot = layout_builder.AddSlot<OBytes>();
  auto bound_input_loader =
      input_loader
          ->Bind({
              {"/str", TypedSlot::FromSlot(str_slot)},
              {"/raw_bytes", TypedSlot::FromSlot(raw_bytes_slot)},
          })
          .value();

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.set_str("abc");
  r.set_raw_bytes("cba");
  while (state.KeepRunningBatch(2)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

BENCHMARK(BM_LoadProtoStringIntoScalars);

void BM_LoadProtoIntoDenseArray(::benchmark::State& state) {
  testing::benchmark::LoadProtoIntoArrays<DenseArray<int>>(
      std::unique_ptr<InputLoader<testing_namespace::Root>>(
          ::my_namespace::GetDenseArrayRepeatedProtoLoader()),
      state);
}

BENCHMARK(BM_LoadProtoIntoDenseArray);

void BM_LoadProtoIntoDenseArrayWithArena(::benchmark::State& state) {
  UnsafeArenaBufferFactory arena(100000);
  testing::benchmark::LoadProtoIntoArrays<DenseArray<int>>(
      std::unique_ptr<InputLoader<testing_namespace::Root>>(
          ::my_namespace::GetDenseArrayRepeatedProtoLoader()),
      state, &arena);
}

BENCHMARK(BM_LoadProtoIntoDenseArrayWithArena);

void BM_LoadNestedBatchWithExtensionProtoIntoDenseArrays(
    ::benchmark::State& state) {
  size_t batch_size = state.range(0);
  size_t sparsity_percent = state.range(1);
  const auto& input_loader =
      ::my_namespace::GetBenchBatchNestedProtoWithExtensionsLoader();
  testing::benchmark::LoadNestedWithExtensionProtoIntoArrays(
      input_loader, batch_size, sparsity_percent, state);
}

BENCHMARK(BM_LoadNestedBatchWithExtensionProtoIntoDenseArrays)
    ->ArgPair(5, 100)
    ->ArgPair(10, 100)
    ->ArgPair(100, 100)
    ->ArgPair(5, 0)
    ->ArgPair(10, 0)
    ->ArgPair(100, 0)
    ->ArgPair(5, 70)
    ->ArgPair(10, 70)
    ->ArgPair(100, 70);

void BM_LoadNestedBatchWithExtensionProtoIntoDenseArraysWithArena(
    ::benchmark::State& state) {
  size_t batch_size = state.range(0);
  size_t sparsity_percent = state.range(1);
  const auto& input_loader =
      ::my_namespace::GetBenchBatchNestedProtoWithExtensionsLoader();
  UnsafeArenaBufferFactory arena(100000);
  testing::benchmark::LoadNestedWithExtensionProtoIntoArrays(
      input_loader, batch_size, sparsity_percent, state, &arena);
}

BENCHMARK(BM_LoadNestedBatchWithExtensionProtoIntoDenseArraysWithArena)
    ->ArgPair(5, 100)
    ->ArgPair(10, 100)
    ->ArgPair(100, 100)
    ->ArgPair(5, 0)
    ->ArgPair(10, 0)
    ->ArgPair(100, 0)
    ->ArgPair(5, 70)
    ->ArgPair(10, 70)
    ->ArgPair(100, 70);

// Benchmark demonstrating that sometimes it is faster to avoid collect
// intermediate results and process the same path several times.
void BM_LoadWithPotentiallyUselessIntermediateResults(
    ::benchmark::State& state) {
  size_t batch_size = state.range(0);
  const auto& input_loader = ::my_namespace::
      GetRepeatedProtoLoaderWithParentIntermediateNodeCollection();
  FrameLayout::Builder layout_builder;
  auto inner_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inner_a0_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners0_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners0_a0_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners1_a_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto inners1_a0_slot = layout_builder.AddSlot<DenseArray<int>>();
  auto bound_input_loader =
      input_loader
          ->Bind({
              {"inners/rr/inner/a", TypedSlot::FromSlot(inner_a_slot)},
              {"inners/rr/inner/a0", TypedSlot::FromSlot(inner_a0_slot)},
              {"inners/rr/inners0/a", TypedSlot::FromSlot(inners0_a_slot)},
              {"inners/rr/inners0/a0", TypedSlot::FromSlot(inners0_a0_slot)},
              {"inners/rr/inners1/a", TypedSlot::FromSlot(inners1_a_slot)},
              {"inners/rr/inners1/a0", TypedSlot::FromSlot(inners1_a0_slot)},
          })
          .value();

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();
  ::testing_namespace::Root r;
  for (int i = 0; i != batch_size; ++i) {
    auto* rr = r.add_inners()->mutable_root_reference();
    {
      auto* inner = rr->mutable_inner();
      inner->set_a(5);
      inner->add_as(7);
    }
    for (int j = 0; j != 2; ++j) {
      auto* inner = rr->add_inners();
      inner->set_a(5);
      inner->add_as(7);
    }
  }
  while (state.KeepRunningBatch(batch_size * 6)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

BENCHMARK(BM_LoadWithPotentiallyUselessIntermediateResults)
    ->Arg(5)
    ->Arg(10)
    ->Arg(100);

}  // namespace
}  // namespace arolla
