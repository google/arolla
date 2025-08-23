// Copyright 2025 Google LLC
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
// This test only makes sense in opt mode.
#ifdef NDEBUG

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_slot.h"
#include "py/arolla/codegen/testing/scalars/many_inputs_and_side_outputs.h"

#include <pthread.h>

namespace arolla {
namespace {

using absl_testing::IsOk;

TEST(StackTest, StackLimitedInit) {
  static constexpr auto fn = [] {
    FrameLayout::Builder layout_builder;
    absl::flat_hash_map<std::string, TypedSlot> inputs;
    for (int64_t i = 0; i != 1000; ++i) {
      inputs.emplace(absl::StrCat("input_", i),
                     TypedSlot::FromSlot(layout_builder.AddSlot<float>()));
    }
    absl::StatusOr<std::unique_ptr<BoundExpr>> executable =
        ::test_namespace::GetManyInputsAndSideOutputs().Bind(
            &layout_builder, inputs,
            TypedSlot::FromSlot(layout_builder.AddSlot<float>()));
    ASSERT_THAT(executable.status(), IsOk());
    FrameLayout memory_layout = std::move(layout_builder).Build();
    MemoryAllocation alloc(&memory_layout);
    ASSERT_THAT((**executable).InitializeLiterals(alloc.frame()), IsOk());
  };

  // NOTE: stack size has page granularity (4096 bytes on x86), and
  // the thread creation code adds TLS data size to the stack size.
  // As a result, asking for X bytes results in some amount from [X, X+4095].
  // The code here uses much less stack, but we have a loose
  // limit to avoid limiting underlying libraries such as tcmalloc or profiling.

  pthread_attr_t pthread_attr;
  ASSERT_EQ(pthread_attr_init(&pthread_attr), 0);
  ASSERT_EQ(pthread_attr_setstacksize(&pthread_attr, 16384), 0);
  pthread_t pthread;
  ASSERT_EQ(pthread_create(
                &pthread, &pthread_attr,
                [](void*) -> void* {
                  fn();
                  return nullptr;
                },
                nullptr),
            0);
  ASSERT_EQ(pthread_join(pthread, nullptr), 0);
}

}  // namespace
}  // namespace arolla

#endif  // NDEBUG
