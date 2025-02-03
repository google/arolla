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
#ifndef AROLLA_IO_PROTO_TESTING_BENCHMARK_UTIL_H_
#define AROLLA_IO_PROTO_TESTING_BENCHMARK_UTIL_H_

#include <cstddef>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/qtype.h"

namespace arolla::testing {
namespace benchmark {

// Uses the given InputLoader to load fields x0, x1... x9 of
// proto/test.proto into scalar slots of a FrameLayout for a benchmark test.
template <class T>
void LoadProtoIntoScalars(const InputLoaderPtr<T>& input_loader,
                          ::benchmark::State& state) {
  using oint = ::arolla::OptionalValue<int>;

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<oint>();
  auto x1_slot = layout_builder.AddSlot<oint>();
  auto x2_slot = layout_builder.AddSlot<oint>();
  auto x3_slot = layout_builder.AddSlot<oint>();
  auto x4_slot = layout_builder.AddSlot<oint>();
  auto x5_slot = layout_builder.AddSlot<oint>();
  auto x6_slot = layout_builder.AddSlot<oint>();
  auto x7_slot = layout_builder.AddSlot<oint>();
  auto x8_slot = layout_builder.AddSlot<oint>();
  auto x9_slot = layout_builder.AddSlot<oint>();
  auto bound_input_loader = input_loader
                                ->Bind({
                                    {"/x0", TypedSlot::FromSlot(x0_slot)},
                                    {"/x1", TypedSlot::FromSlot(x1_slot)},
                                    {"/x2", TypedSlot::FromSlot(x2_slot)},
                                    {"/x3", TypedSlot::FromSlot(x3_slot)},
                                    {"/x4", TypedSlot::FromSlot(x4_slot)},
                                    {"/x5", TypedSlot::FromSlot(x5_slot)},
                                    {"/x6", TypedSlot::FromSlot(x6_slot)},
                                    {"/x7", TypedSlot::FromSlot(x7_slot)},
                                    {"/x8", TypedSlot::FromSlot(x8_slot)},
                                    {"/x9", TypedSlot::FromSlot(x9_slot)},
                                })
                                .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.set_x0(0);
  r.set_x1(1);
  r.set_x2(2);
  r.set_x3(3);
  r.set_x4(4);
  r.set_x5(5);
  r.set_x6(6);
  r.set_x7(7);
  r.set_x8(8);
  r.set_x9(9);

  while (state.KeepRunningBatch(10)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame));
  }
}

// Uses the given SlotListener to write scalar slots into fields x0, x1... x9 of
// proto/test.proto for a benchmark test.
void WriteScalarsIntoProto(
    const SlotListener<::testing_namespace::Root>& slot_listener,
    ::benchmark::State& state, absl::string_view name_prefix = "");

// Uses the given InputLoader to load fields under inner/inner2/root_reference
// x0, x1... x9 of proto/test.proto into scalar slots of a FrameLayout
// for a benchmark test.
void LoadNestedDepth4ProtoIntoScalars(
    const InputLoaderPtr<::testing_namespace::Root>& input_loader,
    ::benchmark::State& state);

// Uses the given InputLoader to load fields under
// Ext::testing_extension_namespace.BenchmarkExtension.bench_ext
// x0, x1... x9 of proto/test.proto into scalar slots of a FrameLayout
// for a benchmark test.
void LoadNestedWithExtensionProtoIntoScalars(
    const InputLoaderPtr<::testing_namespace::Root>& input_loader,
    ::benchmark::State& state);

// Uses the given InputLoader to load fields under
// Ext::testing_extension_namespace.BenchmarkExtension.bench_ext
// x0, x1... x9 of proto/test.proto into DenseArray slots of a FrameLayout
// for a benchmark test.
void LoadNestedWithExtensionProtoIntoArrays(
    const InputLoaderPtr<absl::Span<const ::testing_namespace::Root>>&
        input_loader,
    size_t batch_size, size_t sparsity_percent, ::benchmark::State& state,
    RawBufferFactory* buffer_factory = GetHeapBufferFactory());

// Uses the given InputLoader to load some nested proto and repeated fields of
// proto/test.proto into array slots of a FrameLayout for a benchmark test.
template <class Array, class T>
void LoadProtoIntoArrays(
    const InputLoaderPtr<T>& input_loader, ::benchmark::State& state,
    RawBufferFactory* buffer_factory = GetHeapBufferFactory()) {
  FrameLayout::Builder layout_builder;
  auto ys_slot = layout_builder.AddSlot<Array>();
  auto inner_as_slot = layout_builder.AddSlot<Array>();
  auto inners_as_slot = layout_builder.AddSlot<Array>();
  auto inners_a_slot = layout_builder.AddSlot<Array>();
  auto inners_z_slot = layout_builder.AddSlot<Array>();
  auto bound_input_loader =
      input_loader
          ->Bind({
              {"ys", TypedSlot::FromSlot(ys_slot)},
              {"inner__as", TypedSlot::FromSlot(inner_as_slot)},
              {"inners__as", TypedSlot::FromSlot(inners_as_slot)},
              {"inners__a", TypedSlot::FromSlot(inners_a_slot)},
              {"inners__inner2__z", TypedSlot::FromSlot(inners_z_slot)},
          })
          .value();

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root r;
  r.add_ys(5);
  r.add_ys(7);

  r.mutable_inner()->add_as(3);
  r.mutable_inner()->add_as(5);
  r.mutable_inner()->add_as(7);

  auto* inners0 = r.add_inners();
  inners0->add_as(5);
  inners0->set_a(3);
  inners0->mutable_inner2()->set_z(5);
  auto* inners1 = r.add_inners();
  inners1->add_as(7);
  inners1->add_as(9);
  inners1->set_a(7);
  inners1->mutable_inner2()->set_z(7);

  while (state.KeepRunningBatch(5)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_input_loader(r, frame, buffer_factory));
    {
      auto x = frame.Get(ys_slot);
      ::benchmark::DoNotOptimize(x);
    }
    {
      auto x = frame.Get(inner_as_slot);
      ::benchmark::DoNotOptimize(x);
    }
    {
      auto x = frame.Get(inners_as_slot);
      ::benchmark::DoNotOptimize(x);
    }
    {
      auto x = frame.Get(inners_a_slot);
      ::benchmark::DoNotOptimize(x);
    }
    {
      auto x = frame.Get(inners_z_slot);
      ::benchmark::DoNotOptimize(x);
    }
  }
}

}  // namespace benchmark
}  // namespace arolla::testing

#endif  // AROLLA_IO_PROTO_TESTING_BENCHMARK_UTIL_H_
