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
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//log/check.h"
#include "absl//status/status_matchers.h"
#include "absl//status/statusor.h"
#include "absl//types/span.h"
#include "py/arolla/codegen/testing/fully_batch_expr_for_hybrid.h"
#include "py/arolla/codegen/testing/hybrid_batch_expr.h"
#include "py/arolla/codegen/testing/hybrid_pointwise_expr.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::IsNan;
using ::testing::Not;

struct Input {
  float a;
  float b;
  float c;
  float d;
  float e;
  float q;
  float w;
  float r;
  float t;
  // Cache
  float x;
  float y;
};

Input SampleInput(float id) {
  return Input{
      .a = id + 0.f,
      .b = id + 1.f,
      .c = id + 2.f,
      .d = id + 3.f,
      .e = id + 4.f,
      .q = id + 5.f,
      .w = id + 6.f,
      .r = id + 7.f,
      .t = id + 8.f,
      .x = -1.f,
      .y = -1.f,
  };
}

std::unique_ptr<InputLoader<Input>> CreatePointwiseLoader() {
  return CreateAccessorsInputLoader<Input>(
             "a", [](const Input& in) { return in.a; },  //
             "b", [](const Input& in) { return in.b; },  //
             "c", [](const Input& in) { return in.c; },  //
             "d", [](const Input& in) { return in.d; },  //
             "e", [](const Input& in) { return in.e; },  //
             "q", [](const Input& in) { return in.q; },  //
             "w", [](const Input& in) { return in.w; },  //
             "r", [](const Input& in) { return in.r; },  //
             "t", [](const Input& in) { return in.t; })
      .value();
}

std::unique_ptr<SlotListener<Input>> CreatePointwiseSlotLister() {
  return CreateAccessorsSlotListener<Input>(
             "x", [](float x, Input* o) { o->x = x; },  //
             "y", [](float y, Input* o) { o->y = y; })
      .value();
}

#define DENSE_ARRAY_ACCESSOR(field_name)                         \
  [](absl::Span<const Input> inputs) -> DenseArray<float> {      \
    bitmap::AlmostFullBuilder bitmap_builder(inputs.size());     \
    Buffer<float>::Builder values_builder(inputs.size());        \
    auto values_inserter = values_builder.GetInserter();         \
    for (const Input& input : inputs) {                          \
      values_inserter.Add(input.field_name);                     \
    }                                                            \
    return DenseArray<float>{std::move(values_builder).Build(),  \
                             std::move(bitmap_builder).Build()}; \
  }

std::unique_ptr<InputLoader<absl::Span<const Input>>> CreateBatchLoader() {
  return CreateAccessorsInputLoader<absl::Span<const Input>>(
             "a", DENSE_ARRAY_ACCESSOR(a),  //
             "b", DENSE_ARRAY_ACCESSOR(b),  //
             "c", DENSE_ARRAY_ACCESSOR(c),  //
             "d", DENSE_ARRAY_ACCESSOR(d),  //
             "e", DENSE_ARRAY_ACCESSOR(e),  //
             "q", DENSE_ARRAY_ACCESSOR(q),  //
             "w", DENSE_ARRAY_ACCESSOR(w),  //
             "r", DENSE_ARRAY_ACCESSOR(r),  //
             "t", DENSE_ARRAY_ACCESSOR(t),  //
             "x", DENSE_ARRAY_ACCESSOR(x),  //
             "y", DENSE_ARRAY_ACCESSOR(y))  //
      .value();
}

using EvalFn = std::function<absl::StatusOr<float>(absl::Span<Input>)>;

EvalFn CompileBatchModel() {
  auto const_span_fn =
      ExprCompiler<absl::Span<const Input>, float>()
          .SetInputLoader(CreateBatchLoader())
          .Compile(test_namespace::GetCompiledFullyBatchForHybrid())
          .value();
  return [const_span_fn](absl::Span<Input> inputs) {
    return const_span_fn(absl::MakeSpan(inputs));
  };
}

// Hybrid evaluation operating with pointwise and batch models.
// The computation happen in two steps:
// 1. Pointwise model executed on each element of `Span` and store intermediate
//    values directly in the `Input` struct.
// 2. Batch evaluation reading from intermediate values and computing rest of
//    the model.
EvalFn CompileHybridModel() {
  auto pointwise =
      ExprCompiler<Input, OptionalUnit, Input>()
          .SetInputLoader(CreatePointwiseLoader())
          .SetSlotListener(CreatePointwiseSlotLister())
          .Compile(test_namespace::GetCompiledHybridPointwisePart())
          .value();
  auto batch = ExprCompiler<absl::Span<const Input>, float>()
                   .SetInputLoader(CreateBatchLoader())
                   .Compile(test_namespace::GetCompiledHybridBatchPart())
                   .value();
  return [pointwise, batch](absl::Span<Input> inputs) -> absl::StatusOr<float> {
    for (auto& input : inputs) {
      RETURN_IF_ERROR(pointwise(input, &input).status());
    }
    return batch(inputs);
  };
}

TEST(CodegenHybridTest, BatchVsHybrid) {
  auto batch_fn = CompileBatchModel();
  auto hybrid_fn = CompileHybridModel();
  std::vector<Input> inputs = {SampleInput(0), SampleInput(1)};
  EXPECT_THAT(batch_fn(absl::MakeSpan(inputs)), IsOkAndHolds(Not(IsNan())));
  EXPECT_THAT(hybrid_fn(absl::MakeSpan(inputs)), IsOkAndHolds(Not(IsNan())));
  EXPECT_EQ(batch_fn(absl::MakeSpan(inputs)),
            hybrid_fn(absl::MakeSpan(inputs)));
}

std::vector<Input> SampleInputs(int64_t count) {
  std::vector<Input> inputs;
  inputs.reserve(count);
  for (int64_t i = 0; i != count; ++i) {
    inputs.push_back(SampleInput(i));
  }
  return inputs;
}

void BM_Batch(benchmark::State& state) {
  int64_t count = state.range(0);
  std::vector<Input> inputs = SampleInputs(count);
  auto batch_fn = CompileBatchModel();
  while (state.KeepRunningBatch(count)) {
    benchmark::DoNotOptimize(inputs);
    CHECK_OK(batch_fn(absl::MakeSpan(inputs)));
  }
}

BENCHMARK(BM_Batch)->Range(1, 1000);

void BM_Hybrid(benchmark::State& state) {
  int64_t count = state.range(0);
  std::vector<Input> inputs = SampleInputs(count);
  auto batch_fn = CompileHybridModel();
  while (state.KeepRunningBatch(count)) {
    benchmark::DoNotOptimize(inputs);
    CHECK_OK(batch_fn(absl::MakeSpan(inputs)));
  }
}

BENCHMARK(BM_Hybrid)->Range(1, 1000);

}  // namespace
}  // namespace arolla
