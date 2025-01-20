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
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl//log/check.h"
#include "absl//random/random.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/testing/bound_operators.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/meta.h"

namespace arolla::testing {

#define SIZES Arg(10)->Arg(32)->Arg(100)->Arg(320)->Arg(3200)

template <int flags, typename Fn>
void RunDenseOpBenchmark(benchmark::State& state, Fn fn, bool full,
                         bool bit_offset) {
  int64_t item_count = state.range(0);
  auto arrays =
      RandomDenseArrays(typename meta::function_traits<Fn>::arg_types(),
                        item_count, full, bit_offset);
  auto shallow_arrays = AsUnownedDenseArrays(arrays);

  constexpr size_t batch_size = 100;
  UnsafeArenaBufferFactory buf_factory(item_count * 5 * batch_size);
  auto op = CreateDenseOp<flags>(fn, &buf_factory);
  while (state.KeepRunningBatch(batch_size)) {
    buf_factory.Reset();
    for (size_t i = 0; i != batch_size; ++i) {
      auto x = std::apply(op, shallow_arrays);
      ::benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

template <int flags, typename Fn>
void RunUniversalDenseOpBenchmark(benchmark::State& state, Fn fn, bool full,
                                  bool bit_offset) {
  int64_t item_count = state.range(0);
  auto arrays =
      RandomDenseArrays(typename meta::function_traits<Fn>::arg_types(),
                        item_count, full, bit_offset);
  auto shallow_arrays = AsUnownedDenseArrays(arrays);

  constexpr size_t batch_size = 100;
  UnsafeArenaBufferFactory buf_factory(item_count * 5 * batch_size);
  constexpr bool expensive = !(flags & DenseOpFlags::kRunOnMissing);
  constexpr bool ignore_offset = flags & DenseOpFlags::kNoBitmapOffset;
  auto op = dense_ops_internal::UniversalDenseOp<
      Fn, dense_ops_internal::result_base_t<Fn>, expensive, ignore_offset>(
      fn, &buf_factory);
  while (state.KeepRunningBatch(batch_size)) {
    buf_factory.Reset();
    for (size_t i = 0; i != batch_size; ++i) {
      auto x = std::apply(op, shallow_arrays);
      ::benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

template <typename CreateOpFn>
void RunBoundOperatorBenchmarks(benchmark::State& state, CreateOpFn fn) {
  int64_t item_count = state.range(0);
  constexpr size_t batch_size = 100;
  UnsafeArenaBufferFactory buf_factory(item_count * 5 * batch_size);

  FrameLayout::Builder bldr;
  auto arg1 = bldr.AddSlot<DenseArray<float>>();
  auto arg2 = bldr.AddSlot<DenseArray<float>>();
  auto result = bldr.AddSlot<DenseArray<float>>();
  auto layout = std::move(bldr).Build();
  MemoryAllocation alloc(&layout);
  EvaluationContext ctx;
  auto frame = alloc.frame();

  absl::BitGen gen;
  auto ar1 = RandomDenseArray<float>(item_count, true, 0, gen);
  auto ar2 = RandomDenseArray<float>(item_count, true, 0, gen);
  frame.Set(arg1, AsUnownedDenseArray(ar1));
  frame.Set(arg2, AsUnownedDenseArray(ar2));

  std::unique_ptr<BoundOperator> op = fn(arg1, arg2, result);
  while (state.KeepRunningBatch(batch_size)) {
    buf_factory.Reset();
    for (size_t i = 0; i != batch_size; ++i) {
      op->Run(&ctx, frame);
      CHECK_OK(ctx.status());
      auto x = frame.Get(result);
      ::benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

struct AddFn {
  float operator()(float a, float b) const { return a + b; }
};

struct UnionAddFn {
  OptionalValue<float> operator()(OptionalValue<float> a,
                                  OptionalValue<float> b) const {
    OptionalValue<float> res;
    res.present = a.present || b.present;
    res.value = (a.present ? a.value : 0.f) + (b.present ? b.value : 0.f);
    return res;
  }
};

void BM_DenseBoundOp_AddFull(benchmark::State& state) {
  RunBoundOperatorBenchmarks(state, DenseArrayAddOperator);
}

void BM_DenseBoundOp_EigenAddFull(benchmark::State& state) {
  RunBoundOperatorBenchmarks(state, DenseArrayEigenAddOperator);
}

void BM_DenseBoundOp_UnionAddFull(benchmark::State& state) {
  RunBoundOperatorBenchmarks(state, DenseArrayUnionAddOperator);
}

BENCHMARK(BM_DenseBoundOp_AddFull)->SIZES;
BENCHMARK(BM_DenseBoundOp_EigenAddFull)->SIZES;
BENCHMARK(BM_DenseBoundOp_UnionAddFull)->SIZES;

void BM_DenseOp_Unary(benchmark::State& state) {
  RunDenseOpBenchmark<DenseOpFlags::kRunOnMissing>(
      state, [](float a) { return a * 2; }, false, false);
}

void BM_DenseOp_AddFull(benchmark::State& state) {
  RunDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                      DenseOpFlags::kNoBitmapOffset |
                      DenseOpFlags::kNoSizeValidation>(state, AddFn(), true,
                                                       false);
}

void BM_DenseOp_AddFullWithSizeValidation(benchmark::State& state) {
  RunDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                      DenseOpFlags::kNoBitmapOffset>(state, AddFn(), true,
                                                     false);
}

void BM_DenseOp_AddDense(benchmark::State& state) {
  RunDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                      DenseOpFlags::kNoBitmapOffset |
                      DenseOpFlags::kNoSizeValidation>(state, AddFn(), false,
                                                       false);
}

void BM_DenseOp_AddDenseWithOffset(benchmark::State& state) {
  RunDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                      DenseOpFlags::kNoSizeValidation>(state, AddFn(), false,
                                                       true);
}

void BM_UniversalDenseOp_AddFull(benchmark::State& state) {
  RunUniversalDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                               DenseOpFlags::kNoBitmapOffset |
                               DenseOpFlags::kNoSizeValidation>(state, AddFn(),
                                                                true, false);
}

void BM_UniversalDenseOp_AddDense(benchmark::State& state) {
  RunUniversalDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                               DenseOpFlags::kNoBitmapOffset |
                               DenseOpFlags::kNoSizeValidation>(state, AddFn(),
                                                                false, false);
}

void BM_UniversalDenseOp_AddDenseWithOffset(benchmark::State& state) {
  RunUniversalDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                               DenseOpFlags::kNoSizeValidation>(state, AddFn(),
                                                                false, true);
}

void BM_UniversalDenseOp_AddDenseWithOffset_SkipMissed(
    benchmark::State& state) {
  RunUniversalDenseOpBenchmark<DenseOpFlags::kNoSizeValidation>(state, AddFn(),
                                                                false, true);
}

void BM_UniversalDenseOp_UnionAddDense(benchmark::State& state) {
  RunUniversalDenseOpBenchmark<DenseOpFlags::kRunOnMissing |
                               DenseOpFlags::kNoBitmapOffset |
                               DenseOpFlags::kNoSizeValidation>(
      state, UnionAddFn(), true, false);
}

BENCHMARK(BM_DenseOp_Unary)->SIZES;
BENCHMARK(BM_DenseOp_AddFull)->SIZES;
BENCHMARK(BM_DenseOp_AddFullWithSizeValidation)->SIZES;
BENCHMARK(BM_DenseOp_AddDense)->SIZES;
BENCHMARK(BM_DenseOp_AddDenseWithOffset)->SIZES;

BENCHMARK(BM_UniversalDenseOp_AddFull)->SIZES;
BENCHMARK(BM_UniversalDenseOp_AddDense)->SIZES;
BENCHMARK(BM_UniversalDenseOp_AddDenseWithOffset)->SIZES;
BENCHMARK(BM_UniversalDenseOp_AddDenseWithOffset_SkipMissed)->SIZES;
BENCHMARK(BM_UniversalDenseOp_UnionAddDense)->SIZES;

// *** Vectors

void BM_Vector_AddFull(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, true, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, true, 0, gen);
  std::vector<float> v1(item_count), v2(item_count);
  for (int64_t i = 0; i < item_count; ++i) {
    v1[i] = b1.values[i];
    v2[i] = b2.values[i];
  }
  std::vector<float> res(item_count);
  for (auto _ : state) {
    for (int64_t i = 0; i < item_count; ++i) {
      res[i] = v1[i] + v2[i];
    }
    ::benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

void BM_VectorAbslOpt_AddDense(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, false, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, false, 0, gen);
  auto v1 = ToVectorOptional(b1);
  auto v2 = ToVectorOptional(b2);
  std::vector<std::optional<float>> res(item_count);
  for (auto _ : state) {
    for (int64_t i = 0; i < item_count; ++i) {
      const auto& a = v1[i];
      const auto& b = v2[i];
      if (a.has_value() && b.has_value()) {
        res[i] = *a + *b;
      }
    }
    ::benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

void BM_VectorAbslOpt_UnionAddDense(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, false, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, false, 0, gen);
  auto v1 = ToVectorOptional(b1);
  auto v2 = ToVectorOptional(b2);
  std::vector<std::optional<float>> res(item_count);
  for (auto _ : state) {
    for (int64_t i = 0; i < item_count; ++i) {
      const auto& a = v1[i];
      const auto& b = v2[i];
      if (a.has_value() || b.has_value()) {
        res[i] = a.value_or(0) + b.value_or(0);
      }
    }
    ::benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

void BM_VectorRlOpt_UnionAddDense(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, false, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, false, 0, gen);
  std::vector<OptionalValue<float>> v1(item_count), v2(item_count);
  for (int64_t i = 0; i < item_count; ++i) {
    v1[i] = b1[i];
    v2[i] = b2[i];
  }
  std::vector<OptionalValue<float>> res(item_count);
  for (auto _ : state) {
    auto op = UnionAddFn();
    for (int64_t i = 0; i < item_count; ++i) {
      res[i] = op(v1[i], v2[i]);
    }
    ::benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

BENCHMARK(BM_Vector_AddFull)->SIZES;
BENCHMARK(BM_VectorAbslOpt_AddDense)->SIZES;
BENCHMARK(BM_VectorAbslOpt_UnionAddDense)->SIZES;
BENCHMARK(BM_VectorRlOpt_UnionAddDense)->SIZES;

// *** Compare buffer factories

void BM_AddFull_UnsafeArena(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, true, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, true, 0, gen);
  constexpr size_t batch_size = 100;
  UnsafeArenaBufferFactory buf_factory(item_count * 5 * batch_size);
  while (state.KeepRunningBatch(batch_size)) {
    buf_factory.Reset();
    for (size_t i = 0; i != batch_size; ++i) {
      auto op = CreateDenseOp(AddFn(), &buf_factory);
      auto x = op(b1, b2);
      ::benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

void BM_AddFull_UnsafeArenaNotEnoughMemory(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, true, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, true, 0, gen);
  constexpr size_t batch_size = 100;
  // three or four pages will be created on the first iteration and will
  // be reused for the future. 3-4 SlowAlloc would be called, which will reuse
  // the next page.
  UnsafeArenaBufferFactory buf_factory(item_count * 5 * (batch_size / 3));
  while (state.KeepRunningBatch(batch_size)) {
    buf_factory.Reset();
    for (size_t i = 0; i != batch_size; ++i) {
      auto op = CreateDenseOp(AddFn(), &buf_factory);
      auto x = op(b1, b2);
      ::benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

void BM_AddFull_Heap(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = RandomDenseArray<float>(item_count, true, 0, gen);
  auto b2 = RandomDenseArray<float>(item_count, true, 0, gen);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(b1);
    ::benchmark::DoNotOptimize(b2);
    auto op = CreateDenseOp(AddFn());
    auto x = op(b1, b2);
    ::benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

void BM_AddEmptyFull_Heap(benchmark::State& state) {
  int64_t item_count = state.range(0);
  absl::BitGen gen;
  auto b1 = CreateEmptyDenseArray<float>(item_count);
  auto b2 = RandomDenseArray<float>(item_count, true, 0, gen);
  for (auto _ : state) {
    ::benchmark::DoNotOptimize(b1);
    ::benchmark::DoNotOptimize(b2);
    auto op = CreateDenseOp(AddFn());
    auto x = op(b1, b2);
    ::benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(item_count * state.iterations());
}

BENCHMARK(BM_AddFull_UnsafeArena)->Arg(10);
BENCHMARK(BM_AddFull_UnsafeArenaNotEnoughMemory)->Arg(10);
BENCHMARK(BM_AddFull_Heap)->Arg(10);
BENCHMARK(BM_AddEmptyFull_Heap)->Arg(10);

}  // namespace arolla::testing
