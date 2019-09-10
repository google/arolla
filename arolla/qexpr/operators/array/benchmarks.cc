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
#include <optional>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/array/edge_ops.h"
#include "arolla/qexpr/operators/array/logic_ops.h"
#include "arolla/util/bytes.h"
#include "arolla/util/view_types.h"

namespace arolla::testing {
namespace {

enum class Sparsity { Const, Full, Dense, Sparse, Empty };

template <typename T>
Array<T> RandomArray(size_t size, Sparsity sparsity, absl::BitGen& gen) {
  switch (sparsity) {
    case Sparsity::Empty:
      return Array<T>(size, std::nullopt);
    case Sparsity::Const:
      if constexpr (std::is_same_v<view_type_t<T>, absl::string_view>) {
        return Array<T>(size, T(absl::StrCat(absl::Uniform<float>(gen, 0, 1))));
      } else {
        return Array<T>(size, absl::Uniform<T>(gen, 0, 1));
      }
    case Sparsity::Full:
      return Array<T>(RandomDenseArray<T>(size, true, 0, gen));
    case Sparsity::Dense:
      return Array<T>(RandomDenseArray<T>(size, false, 0, gen));
    case Sparsity::Sparse: {
      Array<T> array(Array<T>(RandomDenseArray<T>(size, false, 0, gen)));
      array = array.ToSparseForm();
      DenseArray<T> new_dense_data =
          RandomDenseArray<T>(array.dense_data().size(), false, 0, gen);
      return Array<T>(size, array.id_filter(), std::move(new_dense_data));
    }
  }
  LOG(FATAL) << "Unknown sparsity: " << static_cast<int64_t>(sparsity);
  return Array<T>();
}

enum class EdgeType { SplitPoints, Mapping, SparseMapping };

ArrayEdge CreateEdge(size_t parent_size, size_t child_size, EdgeType edge_type,
                     absl::BitGen& gen) {
  ArrayEdge edge;
  switch (edge_type) {
    case EdgeType::SplitPoints: {
      Buffer<int64_t>::Builder bldr(parent_size + 1);
      for (int64_t i = 0; i < parent_size; ++i) {
        bldr.Set(i, i * (child_size / parent_size));
      }
      bldr.Set(parent_size, child_size);
      return ArrayEdge::FromSplitPoints(Array<int64_t>(std::move(bldr).Build()))
          .value();
    }
    case EdgeType::Mapping: {
      Buffer<int64_t>::Builder bldr(child_size);
      for (int64_t i = 0; i < child_size; ++i) {
        bldr.Set(i, absl::Uniform<int64_t>(gen, 0, parent_size));
      }
      return ArrayEdge::FromMapping(Array<int64_t>(std::move(bldr).Build()),
                                    parent_size)
          .value();
    }
    case EdgeType::SparseMapping: {
      DenseArrayBuilder<int64_t> bldr(child_size);
      for (int64_t i = 0; i < child_size; ++i) {
        if (absl::Uniform<int64_t>(gen, 0, 3) != 0) continue;
        bldr.Set(i, absl::Uniform<int64_t>(gen, 0, parent_size));
      }
      return ArrayEdge::FromMapping(
                 Array<int64_t>(std::move(bldr).Build()).ToSparseForm(),
                 parent_size)
          .value();
    }
  }
  LOG(FATAL) << "Unknown edge type: " << static_cast<int64_t>(edge_type);
  return ArrayEdge();
}

void BM_PresenceOr(benchmark::State& state, Sparsity XSparsity,
                   Sparsity YSparsity) {
  int64_t size = state.range(0);
  absl::BitGen gen;
  Array<float> x = RandomArray<float>(size, XSparsity, gen);
  Array<float> y = RandomArray<float>(size, YSparsity, gen);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout);
  EvaluationContext ctx(root_ctx);
  auto op = ArrayPresenceOrOp();
  for (auto s : state) {
    benchmark::DoNotOptimize(x);
    benchmark::DoNotOptimize(y);
    auto z = op(&ctx, x, y);
    benchmark::DoNotOptimize(z);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_PresenceOr_Const_Sparse(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Const, Sparsity::Sparse);
}
void BM_PresenceOr_Full_Sparse(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Full, Sparsity::Sparse);
}
void BM_PresenceOr_Dense_Sparse(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Dense, Sparsity::Sparse);
}
void BM_PresenceOr_Sparse_Sparse(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Sparse, Sparsity::Sparse);
}
void BM_PresenceOr_Empty_Sparse(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Empty, Sparsity::Sparse);
}

void BM_PresenceOr_Sparse_Const(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Sparse, Sparsity::Const);
}
void BM_PresenceOr_Dense_Const(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Dense, Sparsity::Const);
}
void BM_PresenceOr_Sparse_Full(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Sparse, Sparsity::Full);
}
void BM_PresenceOr_Sparse_Dense(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Sparse, Sparsity::Dense);
}
void BM_PresenceOr_Sparse_Empty(benchmark::State& state) {
  BM_PresenceOr(state, Sparsity::Sparse, Sparsity::Empty);
}

constexpr auto kPresenceOrSizesFn = [](auto* b) {
  b->Arg(320)->Arg(3200);
};

BENCHMARK(BM_PresenceOr_Const_Sparse)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Full_Sparse)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Dense_Sparse)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Sparse_Sparse)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Empty_Sparse)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Sparse_Const)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Dense_Const)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Sparse_Full)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Sparse_Dense)->Apply(kPresenceOrSizesFn);
BENCHMARK(BM_PresenceOr_Sparse_Empty)->Apply(kPresenceOrSizesFn);

template <typename T>
void BM_Expand(benchmark::State& state, Sparsity parent_sparsity,
               EdgeType edge_type) {
  int64_t parent_size = state.range(0);
  int64_t child_size = state.range(1);
  absl::BitGen gen;
  Array<T> parent_array = RandomArray<T>(parent_size, parent_sparsity, gen);
  ArrayEdge edge = CreateEdge(parent_size, child_size, edge_type, gen);
  FrameLayout frame_layout;
  RootEvaluationContext root_ctx(&frame_layout);
  EvaluationContext ctx(root_ctx);
  auto op = ArrayExpandOp();
  for (auto s : state) {
    benchmark::DoNotOptimize(parent_array);
    benchmark::DoNotOptimize(edge);
    auto res = op(&ctx, parent_array, edge);
    benchmark::DoNotOptimize(res);
  }
}

void BM_Expand_full_int_over_split_points(benchmark::State& state) {
  BM_Expand<int>(state, Sparsity::Full, EdgeType::SplitPoints);
}
void BM_Expand_full_bytes_over_split_points(benchmark::State& state) {
  BM_Expand<Bytes>(state, Sparsity::Full, EdgeType::SplitPoints);
}
void BM_Expand_sparse_int_over_split_points(benchmark::State& state) {
  BM_Expand<int>(state, Sparsity::Sparse, EdgeType::SplitPoints);
}
void BM_Expand_sparse_bytes_over_split_points(benchmark::State& state) {
  BM_Expand<Bytes>(state, Sparsity::Sparse, EdgeType::SplitPoints);
}
void BM_Expand_sparse_int_over_mapping(benchmark::State& state) {
  BM_Expand<int>(state, Sparsity::Sparse, EdgeType::Mapping);
}
void BM_Expand_sparse_bytes_over_mapping(benchmark::State& state) {
  BM_Expand<Bytes>(state, Sparsity::Sparse, EdgeType::Mapping);
}
void BM_Expand_sparse_int_over_sparse_mapping(benchmark::State& state) {
  BM_Expand<int>(state, Sparsity::Sparse, EdgeType::SparseMapping);
}
void BM_Expand_sparse_bytes_over_sparse_mapping(benchmark::State& state) {
  BM_Expand<Bytes>(state, Sparsity::Sparse, EdgeType::SparseMapping);
}

constexpr auto kExpandSizesFn = [](auto* b) {
  b->ArgPair(10, 100)
      ->ArgPair(10, 1000)
      ->ArgPair(100, 100)
      ->ArgPair(100, 1000)
      ->ArgPair(1000, 1000);
};

BENCHMARK(BM_Expand_full_int_over_split_points)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_full_bytes_over_split_points)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_sparse_int_over_split_points)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_sparse_bytes_over_split_points)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_sparse_int_over_mapping)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_sparse_bytes_over_mapping)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_sparse_int_over_sparse_mapping)->Apply(kExpandSizesFn);
BENCHMARK(BM_Expand_sparse_bytes_over_sparse_mapping)->Apply(kExpandSizesFn);

}  // namespace
}  // namespace arolla::testing
