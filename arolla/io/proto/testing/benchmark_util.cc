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
#include "arolla/io/proto/testing/benchmark_util.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/proto/test_benchmark_extension.pb.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::testing {
namespace benchmark {

void LoadNestedDepth4ProtoIntoScalars(
    const InputLoaderPtr<::testing_namespace::Root>& input_loader,
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
  std::string prefix = "/inner/inner2/root_reference/";
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {prefix + "x0", TypedSlot::FromSlot(x0_slot)},
                           {prefix + "x1", TypedSlot::FromSlot(x1_slot)},
                           {prefix + "x2", TypedSlot::FromSlot(x2_slot)},
                           {prefix + "x3", TypedSlot::FromSlot(x3_slot)},
                           {prefix + "x4", TypedSlot::FromSlot(x4_slot)},
                           {prefix + "x5", TypedSlot::FromSlot(x5_slot)},
                           {prefix + "x6", TypedSlot::FromSlot(x6_slot)},
                           {prefix + "x7", TypedSlot::FromSlot(x7_slot)},
                           {prefix + "x8", TypedSlot::FromSlot(x8_slot)},
                           {prefix + "x9", TypedSlot::FromSlot(x9_slot)},
                       }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root root;
  ::testing_namespace::Root& r =
      *root.mutable_inner()->mutable_inner2()->mutable_root_reference();
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
    ::benchmark::DoNotOptimize(root);
    CHECK_OK(bound_input_loader(root, frame));
  }
}

void LoadNestedWithExtensionProtoIntoScalars(
    const InputLoaderPtr<::testing_namespace::Root>& input_loader,
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
  std::string prefix =
      "/Ext::testing_extension_namespace.BenchmarkExtension.bench_ext/";
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {prefix + "x0", TypedSlot::FromSlot(x0_slot)},
                           {prefix + "x1", TypedSlot::FromSlot(x1_slot)},
                           {prefix + "x2", TypedSlot::FromSlot(x2_slot)},
                           {prefix + "x3", TypedSlot::FromSlot(x3_slot)},
                           {prefix + "x4", TypedSlot::FromSlot(x4_slot)},
                           {prefix + "x5", TypedSlot::FromSlot(x5_slot)},
                           {prefix + "x6", TypedSlot::FromSlot(x6_slot)},
                           {prefix + "x7", TypedSlot::FromSlot(x7_slot)},
                           {prefix + "x8", TypedSlot::FromSlot(x8_slot)},
                           {prefix + "x9", TypedSlot::FromSlot(x9_slot)},
                       }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root root;
  auto& r = *root.MutableExtension(
      ::testing_extension_namespace::BenchmarkExtension::bench_ext);
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
    ::benchmark::DoNotOptimize(root);
    CHECK_OK(bound_input_loader(root, frame));
  }
}

void LoadNestedWithExtensionProtoIntoArrays(
    const InputLoaderPtr<absl::Span<const ::testing_namespace::Root>>&
        input_loader,
    size_t batch_size, size_t sparsity_percent, ::benchmark::State& state,
    RawBufferFactory* buffer_factory) {
  using Array = ::arolla::DenseArray<int>;

  FrameLayout::Builder layout_builder;
  auto x0_slot = layout_builder.AddSlot<Array>();
  auto x1_slot = layout_builder.AddSlot<Array>();
  auto x2_slot = layout_builder.AddSlot<Array>();
  auto x3_slot = layout_builder.AddSlot<Array>();
  auto x4_slot = layout_builder.AddSlot<Array>();
  auto x5_slot = layout_builder.AddSlot<Array>();
  auto x6_slot = layout_builder.AddSlot<Array>();
  auto x7_slot = layout_builder.AddSlot<Array>();
  auto x8_slot = layout_builder.AddSlot<Array>();
  auto x9_slot = layout_builder.AddSlot<Array>();
  std::string prefix =
      "/Ext::testing_extension_namespace.BenchmarkExtension.bench_ext/";
  ASSERT_OK_AND_ASSIGN(auto bound_input_loader,
                       input_loader->Bind({
                           {prefix + "x0", TypedSlot::FromSlot(x0_slot)},
                           {prefix + "x1", TypedSlot::FromSlot(x1_slot)},
                           {prefix + "x2", TypedSlot::FromSlot(x2_slot)},
                           {prefix + "x3", TypedSlot::FromSlot(x3_slot)},
                           {prefix + "x4", TypedSlot::FromSlot(x4_slot)},
                           {prefix + "x5", TypedSlot::FromSlot(x5_slot)},
                           {prefix + "x6", TypedSlot::FromSlot(x6_slot)},
                           {prefix + "x7", TypedSlot::FromSlot(x7_slot)},
                           {prefix + "x8", TypedSlot::FromSlot(x8_slot)},
                           {prefix + "x9", TypedSlot::FromSlot(x9_slot)},
                       }));
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();

  ::testing_namespace::Root root;
  auto& r = *root.MutableExtension(
      ::testing_extension_namespace::BenchmarkExtension::bench_ext);
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

  std::vector<::testing_namespace::Root> input(batch_size, root);
  for (size_t i = 0; i < batch_size; ++i) {
    if ((i + 1) * 7919 % 100 <= sparsity_percent) {
      input[i] = ::testing_namespace::Root();
    }
  }

  while (state.KeepRunningBatch(10 * batch_size)) {
    ::benchmark::DoNotOptimize(root);
    CHECK_OK(bound_input_loader(input, frame, buffer_factory));
  }
}

void WriteScalarsIntoProto(
    const SlotListener<::testing_namespace::Root>& slot_listener,
    ::benchmark::State& state, absl::string_view name_prefix) {
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
  auto bound_slot_listener =
      slot_listener
          .Bind({
              {absl::StrCat(name_prefix, "/x0"), TypedSlot::FromSlot(x0_slot)},
              {absl::StrCat(name_prefix, "/x1"), TypedSlot::FromSlot(x1_slot)},
              {absl::StrCat(name_prefix, "/x2"), TypedSlot::FromSlot(x2_slot)},
              {absl::StrCat(name_prefix, "/x3"), TypedSlot::FromSlot(x3_slot)},
              {absl::StrCat(name_prefix, "/x4"), TypedSlot::FromSlot(x4_slot)},
              {absl::StrCat(name_prefix, "/x5"), TypedSlot::FromSlot(x5_slot)},
              {absl::StrCat(name_prefix, "/x6"), TypedSlot::FromSlot(x6_slot)},
              {absl::StrCat(name_prefix, "/x7"), TypedSlot::FromSlot(x7_slot)},
              {absl::StrCat(name_prefix, "/x8"), TypedSlot::FromSlot(x8_slot)},
              {absl::StrCat(name_prefix, "/x9"), TypedSlot::FromSlot(x9_slot)},
          })
          .value();
  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();
  frame.Set(x0_slot, 0);
  frame.Set(x1_slot, 1);
  frame.Set(x2_slot, 2);
  frame.Set(x3_slot, 3);
  frame.Set(x4_slot, 4);
  frame.Set(x5_slot, 5);
  frame.Set(x6_slot, 6);
  frame.Set(x7_slot, 7);
  frame.Set(x8_slot, 8);
  frame.Set(x9_slot, 9);

  ::testing_namespace::Root r;
  CHECK_OK(bound_slot_listener(frame, &r));
  // Check that we wrote something
  CHECK_NE(r.ByteSizeLong(), 0);

  while (state.KeepRunningBatch(10)) {
    ::benchmark::DoNotOptimize(r);
    CHECK_OK(bound_slot_listener(frame, &r));
  }
}

}  // namespace benchmark
}  // namespace arolla::testing
