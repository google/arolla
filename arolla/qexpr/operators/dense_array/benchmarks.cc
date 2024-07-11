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
#include "arolla/qexpr/testing/benchmarks.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/core/logic_operators.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::testing {
namespace {

#define SIZES Arg(10)->Arg(32)->Arg(100)->Arg(320)->Arg(3200)

void BM_Add(benchmark::State& state, bool full) {
  InitArolla();

  int vector_size = state.range(0);
  int num_inputs = state.range(1);

  auto qtype = GetDenseArrayQType<float>();
  auto add_op = OperatorRegistry::GetInstance()
                    ->LookupOperator("test.add", {qtype, qtype}, qtype)
                    .value();
  DenseArray<float> array;
  array.values = CreateBuffer(std::vector<float>(vector_size, 5.7));
  if (!full) {
    array.bitmap = CreateBuffer(
        std::vector<uint32_t>(bitmap::BitmapSize(vector_size), 0xf0f0f0f0));
  }
  testing::BenchmarkBinaryOperator(
      *add_op, num_inputs, TypedValue::FromValue(array),
      /*common_inputs=*/{}, /*shuffle=*/false, state, /*use_arena=*/true);
  state.SetItemsProcessed(vector_size * (num_inputs - 1) * state.iterations());
}

void BM_DenseArrayAddFull(benchmark::State& state) { BM_Add(state, true); }
void BM_DenseArrayAddDense(benchmark::State& state) { BM_Add(state, false); }

BENCHMARK(BM_DenseArrayAddFull)->Apply(&testing::RunArrayBenchmark);
BENCHMARK(BM_DenseArrayAddDense)->Apply(&testing::RunArrayBenchmark);

void BM_PresenceAnd_Lifted(benchmark::State& state) {
  InitArolla();

  int64_t size = state.range(0);
  absl::BitGen gen;
  DenseArray<float> x = RandomDenseArray<float>(size, false, 0, gen);
  DenseArray<float> x_unowned = AsUnownedDenseArray(x);
  DenseArray<float> y = RandomDenseArray<float>(size, false, 0, gen);
  DenseArray<Unit> mask_unowned = {VoidBuffer(size), y.bitmap.ShallowCopy()};
  UnsafeArenaBufferFactory arena(1024 * 1024);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, &arena);
  EvaluationContext ctx(root_ctx);

  auto op =
      DenseArrayLifter<PresenceAndOp, meta::type_list<float, OptionalUnit>>();
  for (auto s : state) {
    arena.Reset();
    auto x = op(&ctx, x_unowned, mask_unowned);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_PresenceAnd_Optimized(benchmark::State& state) {
  InitArolla();

  int64_t size = state.range(0);
  absl::BitGen gen;
  DenseArray<float> x = RandomDenseArray<float>(size, false, 0, gen);
  DenseArray<float> x_unowned = AsUnownedDenseArray(x);
  DenseArray<float> y = RandomDenseArray<float>(size, false, 0, gen);
  DenseArray<Unit> mask_unowned = {VoidBuffer(size), y.bitmap.ShallowCopy()};
  UnsafeArenaBufferFactory arena(1024 * 1024);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, &arena);
  EvaluationContext ctx(root_ctx);

  auto op = DenseArrayPresenceAndOp();
  for (auto s : state) {
    arena.Reset();
    auto x = op(&ctx, x_unowned, mask_unowned);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

BENCHMARK(BM_PresenceAnd_Lifted)->SIZES;
BENCHMARK(BM_PresenceAnd_Optimized)->SIZES;

enum class ImplType { Lifted, Optimized };
enum class Sparsity { Full, Sparse, Empty };

template <ImplType Type, Sparsity XSparsity, Sparsity YSparsity>
void BM_PresenceOr(benchmark::State& state) {
  InitArolla();

  int64_t size = state.range(0);
  absl::BitGen gen;
  DenseArray<float> x;
  if (XSparsity == Sparsity::Empty) {
    x = DenseArrayBuilder<float>(size).Build();
  } else {
    x = RandomDenseArray<float>(size, XSparsity == Sparsity::Full, 0, gen);
  }
  DenseArray<float> x_unowned = AsUnownedDenseArray(x);
  DenseArray<float> y;
  if (YSparsity == Sparsity::Empty) {
    y = DenseArrayBuilder<float>(size).Build();
  } else {
    y = RandomDenseArray<float>(size, YSparsity == Sparsity::Full, 0, gen);
  }
  DenseArray<float> y_unowned = AsUnownedDenseArray(y);
  UnsafeArenaBufferFactory arena(1024 * 1024);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, &arena);
  EvaluationContext ctx(root_ctx);

  if constexpr (Type == ImplType::Optimized) {
    auto op = DenseArrayPresenceOrOp();
    for (auto s : state) {
      arena.Reset();
      auto x = op(&ctx, x_unowned, y_unowned);
      benchmark::DoNotOptimize(x);
    }
  } else {
    auto op =
        DenseArrayLifter<PresenceOrOp, meta::type_list<OptionalValue<float>,
                                                       OptionalValue<float>>>();
    for (auto s : state) {
      arena.Reset();
      auto x = op(&ctx, x_unowned, y_unowned);
      benchmark::DoNotOptimize(x);
    }
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_PresenceOr_Lifted_Sparse_Sparse(benchmark::State& state) {
  BM_PresenceOr<ImplType::Lifted, Sparsity::Sparse, Sparsity::Sparse>(state);
}
void BM_PresenceOr_Lifted_Sparse_Full(benchmark::State& state) {
  BM_PresenceOr<ImplType::Lifted, Sparsity::Sparse, Sparsity::Full>(state);
}
void BM_PresenceOr_Lifted_Full_Sparse(benchmark::State& state) {
  BM_PresenceOr<ImplType::Lifted, Sparsity::Full, Sparsity::Sparse>(state);
}
void BM_PresenceOr_Lifted_Full_Full(benchmark::State& state) {
  BM_PresenceOr<ImplType::Lifted, Sparsity::Full, Sparsity::Full>(state);
}
void BM_PresenceOr_Lifted_Empty_Sparse(benchmark::State& state) {
  BM_PresenceOr<ImplType::Lifted, Sparsity::Empty, Sparsity::Sparse>(state);
}
void BM_PresenceOr_Optimized_Sparse_Sparse(benchmark::State& state) {
  BM_PresenceOr<ImplType::Optimized, Sparsity::Sparse, Sparsity::Sparse>(state);
}
void BM_PresenceOr_Optimized_Sparse_Full(benchmark::State& state) {
  BM_PresenceOr<ImplType::Optimized, Sparsity::Sparse, Sparsity::Full>(state);
}
void BM_PresenceOr_Optimized_Full_Sparse(benchmark::State& state) {
  BM_PresenceOr<ImplType::Optimized, Sparsity::Full, Sparsity::Sparse>(state);
}
void BM_PresenceOr_Optimized_Full_Full(benchmark::State& state) {
  BM_PresenceOr<ImplType::Optimized, Sparsity::Full, Sparsity::Full>(state);
}
void BM_PresenceOr_Optimized_Empty_Sparse(benchmark::State& state) {
  BM_PresenceOr<ImplType::Optimized, Sparsity::Empty, Sparsity::Sparse>(state);
}

BENCHMARK(BM_PresenceOr_Lifted_Sparse_Sparse)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Lifted_Sparse_Full)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Lifted_Full_Sparse)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Lifted_Full_Full)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Lifted_Empty_Sparse)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Optimized_Sparse_Sparse)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Optimized_Sparse_Full)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Optimized_Full_Sparse)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Optimized_Full_Full)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Optimized_Empty_Sparse)->Arg(32)->Arg(320);

template <Sparsity XSparsity>
void BM_PresenceOr_Const(benchmark::State& state) {
  InitArolla();

  int64_t size = state.range(0);
  absl::BitGen gen;
  DenseArray<float> x;
  if (XSparsity == Sparsity::Empty) {
    x = DenseArrayBuilder<float>(size).Build();
  } else {
    x = RandomDenseArray<float>(size, XSparsity == Sparsity::Full, 0, gen);
  }
  DenseArray<float> x_unowned = AsUnownedDenseArray(x);
  OptionalValue<float> y = 5.f;
  UnsafeArenaBufferFactory arena(1024 * 1024);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, &arena);
  EvaluationContext ctx(root_ctx);

  auto op = DenseArrayPresenceOrOp();
  for (auto s : state) {
    arena.Reset();
    auto x = op(&ctx, x_unowned, y);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

BENCHMARK(BM_PresenceOr_Const<Sparsity::Empty>)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Const<Sparsity::Full>)->Arg(32)->Arg(320);
BENCHMARK(BM_PresenceOr_Const<Sparsity::Sparse>)->Arg(32)->Arg(320);

template <typename T>
void BM_Expand(benchmark::State& state, const T& value, bool sparse) {
  int64_t parent_size = state.range(0);
  int64_t group_size = state.range(1);
  auto values = CreateConstDenseArray<T>(parent_size, value);
  if (sparse) {
    Buffer<bitmap::Word>::Builder bitmap_bldr(bitmap::BitmapSize(parent_size));
    // fill with 10101010 bit pattern.
    std::fill(bitmap_bldr.GetMutableSpan().begin(),
              bitmap_bldr.GetMutableSpan().end(), 0xaaaaaaaa);
    values.bitmap = std::move(bitmap_bldr).Build();
  }
  Buffer<int64_t>::Builder split_points(parent_size + 1);
  for (int i = 0; i < parent_size + 1; ++i) {
    split_points.Set(i, i * group_size);
  }
  auto edge = DenseArrayEdge::FromSplitPoints({std::move(split_points).Build()})
                  .value();

  auto op = DenseArrayExpandOp();

  UnsafeArenaBufferFactory arena(1024 * 1024);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout, &arena);
  EvaluationContext ctx(root_ctx);

  for (auto s : state) {
    arena.Reset();
    benchmark::DoNotOptimize(values);
    benchmark::DoNotOptimize(edge);
    auto res = op(&ctx, values, edge);
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(parent_size * group_size * state.iterations());
}

void BM_ExpandInt_full(benchmark::State& state) {
  BM_Expand<int>(state, 1, /*sparse=*/false);
}
void BM_ExpandInt_sparse(benchmark::State& state) {
  BM_Expand<int>(state, 1, /*sparse=*/true);
}

void BM_ExpandText_full(benchmark::State& state) {
  BM_Expand<arolla::Text>(state, arolla::Text("Arolla should be fast"),
                          /*sparse=*/false);
}
void BM_ExpandText_sparse(benchmark::State& state) {
  BM_Expand<arolla::Text>(state, arolla::Text("Arolla should be fast"),
                          /*sparse=*/true);
}

constexpr auto kExpandArgsFn = [](auto* b) {
  b->ArgPair(1, 1)
      ->ArgPair(1, 10)
      ->ArgPair(1, 100)
      ->ArgPair(1, 1000)
      ->ArgPair(10, 1)
      ->ArgPair(10, 10)
      ->ArgPair(10, 100)
      ->ArgPair(10, 1000)
      ->ArgPair(100, 1)
      ->ArgPair(100, 10)
      ->ArgPair(100, 100);
};

BENCHMARK(BM_ExpandInt_full)->Apply(kExpandArgsFn);
BENCHMARK(BM_ExpandInt_sparse)->Apply(kExpandArgsFn);

BENCHMARK(BM_ExpandText_full)->Apply(kExpandArgsFn);
BENCHMARK(BM_ExpandText_sparse)->Apply(kExpandArgsFn);

}  // namespace
}  // namespace arolla::testing
