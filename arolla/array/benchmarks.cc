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
#include <cmath>
#include <cstdint>
#include <optional>
#include <random>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/group_op.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/pointwise_op.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/util/meta.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

// sparsity=1 -> full
// sparsity=2 -> 50% present
// sparsity=N -> 1/N present
IdFilter RandomIdFilter(int64_t size, int sparsity, absl::BitGen& gen) {
  if (sparsity <= 1) return IdFilter::kFull;
  Buffer<int64_t>::Builder bldr(size);
  auto inserter = bldr.GetInserter();
  for (int64_t i = 0; i < size; ++i) {
    if (absl::Bernoulli(gen, 1.0 / sparsity)) inserter.Add(i);
  }
  return IdFilter(size, std::move(bldr).Build(inserter));
}

DenseArray<float> CreateRandomFullArray(absl::BitGen& gen, int64_t size) {
  typename Buffer<float>::Builder bldr(size);
  for (int64_t i = 0; i < size; ++i) {
    if (absl::Bernoulli(gen, 0.5)) {
      bldr.Set(i, 1.0);
    } else {
      bldr.Set(i, 0.0);
    }
  }
  return {std::move(bldr).Build()};
}

DenseArray<float> CreateRandomDenseArray(absl::BitGen& gen, int64_t size) {
  DenseArrayBuilder<float> bldr(size);
  for (int64_t i = 0; i < size; ++i) {
    if (absl::Bernoulli(gen, 0.1)) {
      continue;  // missing
    } else if (absl::Bernoulli(gen, 0.4)) {
      bldr.Set(i, 1.0);
    } else {
      bldr.Set(i, 0.0);
    }
  }
  return std::move(bldr).Build();
}

Array<float> CreateRandomArray(int64_t size, const IdFilter& ids,
                               OptionalValue<float> missing_id_value,
                               absl::BitGen& gen) {
  if (ids.type() == IdFilter::kEmpty) {
    return Array<float>(size, missing_id_value);
  }
  int64_t data_size = ids.type() == IdFilter::kFull ? size : ids.ids().size();
  Buffer<float>::Builder bldr(data_size);
  for (int64_t i = 0; i < data_size; ++i) {
    bldr.Set(i, absl::Uniform<float>(gen, 0, 1));
  }
  return Array<float>(size, ids, {std::move(bldr).Build()}, missing_id_value);
}

ArrayEdge GetSplitPointsEdge(int64_t parent_size, int64_t children) {
  std::vector<OptionalValue<int64_t>> split_points;
  split_points.reserve(parent_size + 1);
  for (int64_t i = 0; i <= parent_size; ++i) {
    split_points.push_back(i * children);
  }
  return ArrayEdge::FromSplitPoints(CreateArray<int64_t>(split_points)).value();
}

ArrayEdge GetMappingEdge(int64_t parent_size, int64_t children) {
  std::vector<OptionalValue<int64_t>> mapping;
  mapping.reserve(parent_size * children);
  for (int64_t i = 0; i < parent_size; ++i) {
    for (int64_t j = 0; j < children; ++j) {
      mapping.push_back(i);
    }
  }
  return ArrayEdge::FromMapping(CreateArray<int64_t>(mapping), parent_size)
      .value();
}

void BM_WithIds_SparseToSparse(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto ids = RandomIdFilter(size, 4, gen);
  auto values = CreateConstDenseArray<float>(ids.ids().size(), 1.0);
  Array<float> block(size, ids, values);
  auto new_ids = RandomIdFilter(size, 4, gen);
  for (auto s : state) {
    auto x = block.WithIds(new_ids, std::nullopt);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_WithIds_SparseToDense(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto ids = RandomIdFilter(size, 4, gen);
  auto values = CreateConstDenseArray<float>(ids.ids().size(), 1.0);
  Array<float> block(size, ids, values);
  for (auto s : state) {
    auto x = block.WithIds(IdFilter::kFull, std::nullopt);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_WithIds_FullToSparse(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto values = CreateConstDenseArray<float>(size, 1.0);
  Array<float> block(values);
  auto new_ids = RandomIdFilter(size, 4, gen);
  for (auto s : state) {
    auto x = block.WithIds(new_ids, std::nullopt);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_WithIds_DenseToSparse(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  DenseArrayBuilder<float> bldr(size);
  for (int64_t i = 0; i < size; ++i) {
    if (absl::Bernoulli(gen, 0.5)) {
      bldr.Set(i, 1.0);
    }
  }
  Array<float> block(std::move(bldr).Build());
  auto new_ids = RandomIdFilter(size, 4, gen);
  for (auto s : state) {
    auto x = block.WithIds(new_ids, std::nullopt);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseForm_FromDense(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  DenseArrayBuilder<float> bldr(size);
  for (int64_t i = 0; i < size; ++i) {
    if (absl::Bernoulli(gen, 0.5)) {
      bldr.Set(i, 1.0);
    }
  }
  Array<float> block(std::move(bldr).Build());
  for (auto s : state) {
    auto x = block.ToSparseForm();
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseForm_FromSparse(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto ids = RandomIdFilter(size, 2, gen);
  int64_t data_size = ids.type() != IdFilter::kFull ? ids.ids().size() : size;
  DenseArrayBuilder<float> bldr(data_size);
  for (int64_t i = 0; i < data_size; ++i) {
    if (absl::Bernoulli(gen, 0.5)) {
      bldr.Set(i, 1.0);
    }
  }
  Array<float> block(size, ids, std::move(bldr).Build());
  for (auto s : state) {
    auto x = block.ToSparseForm();
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseFormWithMissedIdValue_FromDense(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  Array<float> block(CreateRandomDenseArray(gen, size));
  for (auto s : state) {
    auto x = block.ToSparseForm(0);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseFormWithMissedIdValue_FromMixedSparse(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto ids = RandomIdFilter(size, 2, gen);
  int64_t data_size = ids.type() != IdFilter::kFull ? ids.ids().size() : size;
  Array<float> block(size, ids, CreateRandomDenseArray(gen, data_size), 0);
  for (auto s : state) {
    auto x = block.ToSparseForm(0);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseFormWithMissedIdValue_FromFull(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  Array<float> block(CreateRandomFullArray(gen, size));
  for (auto s : state) {
    auto x = block.ToSparseForm(0);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseFormWithMissedIdValue_FromSparse(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto ids = RandomIdFilter(size, 2, gen);
  int64_t data_size = ids.type() != IdFilter::kFull ? ids.ids().size() : size;
  Array<float> block(size, ids, CreateRandomFullArray(gen, data_size), 0);
  for (auto s : state) {
    auto x = block.ToSparseForm(0);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_ToSparseForm_FromSparseWithOtherMissedIdValue(benchmark::State& state) {
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto ids = RandomIdFilter(size, 2, gen);
  int64_t data_size = ids.type() != IdFilter::kFull ? ids.ids().size() : size;
  DenseArrayBuilder<float> bldr(data_size);
  for (int64_t i = 0; i < data_size; ++i) {
    if (absl::Bernoulli(gen, 0.5)) {
      bldr.Set(i, 1.0);
    }
  }
  Array<float> block(size, ids, std::move(bldr).Build(), 0);
  for (auto s : state) {
    auto x = block.ToSparseForm();
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

BENCHMARK(BM_WithIds_SparseToSparse);
BENCHMARK(BM_WithIds_SparseToDense);
BENCHMARK(BM_WithIds_FullToSparse);
BENCHMARK(BM_WithIds_DenseToSparse);

BENCHMARK(BM_ToSparseForm_FromDense);
BENCHMARK(BM_ToSparseForm_FromSparse);
BENCHMARK(BM_ToSparseFormWithMissedIdValue_FromFull);
BENCHMARK(BM_ToSparseFormWithMissedIdValue_FromDense);
BENCHMARK(BM_ToSparseFormWithMissedIdValue_FromMixedSparse);
BENCHMARK(BM_ToSparseFormWithMissedIdValue_FromSparse);
BENCHMARK(BM_ToSparseForm_FromSparseWithOtherMissedIdValue);

void BM_Add(benchmark::State& state) {
  int sparsity1 = state.range(0);
  int sparsity2 = state.range(1);
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto op = CreateArrayOp([](float a, float b) { return a + b; });

  auto arg1 = CreateRandomArray(size, RandomIdFilter(size, sparsity1, gen),
                                std::nullopt, gen);
  auto arg2 = CreateRandomArray(size, RandomIdFilter(size, sparsity2, gen),
                                std::nullopt, gen);

  for (auto s : state) {
    auto x = op(arg1, arg2);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_Add_Union(benchmark::State& state) {
  int sparsity1 = state.range(0);
  int sparsity2 = state.range(1);
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto op = CreateArrayOp([](float a, float b) { return a + b; });

  auto arg1 =
      CreateRandomArray(size, RandomIdFilter(size, sparsity1, gen), 2.0, gen);
  auto arg2 =
      CreateRandomArray(size, RandomIdFilter(size, sparsity2, gen), 3.0, gen);

  for (auto s : state) {
    auto x = op(arg1, arg2);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_Add_SameFilter(benchmark::State& state) {
  int sparsity = state.range(0);
  int64_t size = 1024 * 1024;
  absl::BitGen gen;
  auto op = CreateArrayOp([](float a, float b) { return a + b; });

  auto ids = RandomIdFilter(size, sparsity, gen);
  auto arg1 = CreateRandomArray(size, ids, 2.0, gen);
  auto arg2 = CreateRandomArray(size, ids, 3.0, gen);

  for (auto s : state) {
    auto x = op(arg1, arg2);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

BENCHMARK(BM_Add)
    ->ArgPair(1, 1)
    ->ArgPair(1, 4)
    ->ArgPair(1, 16)
    ->ArgPair(1, 64)
    ->ArgPair(4, 4)
    ->ArgPair(4, 16)
    ->ArgPair(4, 64)
    ->ArgPair(16, 16)
    ->ArgPair(16, 64)
    ->ArgPair(64, 64);
BENCHMARK(BM_Add_Union)
    ->ArgPair(1, 4)
    ->ArgPair(1, 16)
    ->ArgPair(1, 64)
    ->ArgPair(4, 4)
    ->ArgPair(4, 16)
    ->ArgPair(4, 64)
    ->ArgPair(16, 16)
    ->ArgPair(16, 64)
    ->ArgPair(64, 64);
BENCHMARK(BM_Add_SameFilter)->Arg(4)->Arg(16)->Arg(64);

void BM_AddFull(benchmark::State& state) {
  int64_t size = state.range(0);
  absl::BitGen gen;
  auto op = CreateArrayOp([](float a, float b) { return a + b; });

  auto arg1 =
      CreateRandomArray(size, RandomIdFilter(size, 1, gen), std::nullopt, gen);
  auto arg2 =
      CreateRandomArray(size, RandomIdFilter(size, 1, gen), std::nullopt, gen);

  for (auto s : state) {
    auto x = op(arg1, arg2);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

void BM_AddFull_WithArena(benchmark::State& state) {
  int64_t size = state.range(0);
  absl::BitGen gen;

  Buffer<float> b1, b2;
  {
    Buffer<float>::Builder bldr(size);
    for (int64_t i = 0; i < size; ++i) {
      bldr.Set(i, absl::Uniform<float>(gen, 0, 1));
    }
    b1 = std::move(bldr).Build();
  }
  {
    Buffer<float>::Builder bldr(size);
    for (int64_t i = 0; i < size; ++i) {
      bldr.Set(i, absl::Uniform<float>(gen, 0, 1));
    }
    b2 = std::move(bldr).Build();
  }
  Array<float> arg1(DenseArray<float>{b1.ShallowCopy()});
  Array<float> arg2(DenseArray<float>{b1.ShallowCopy()});

  UnsafeArenaBufferFactory arena(16 * 1024 * 1024);
  auto op = CreateArrayOp([](float a, float b) { return a + b; }, &arena);

  for (auto s : state) {
    arena.Reset();
    auto x = op(arg1, arg2);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

BENCHMARK(BM_AddFull)->Arg(32)->Arg(1024)->Arg(32 * 1024)->Arg(1024 * 1024);
BENCHMARK(BM_AddFull_WithArena)
    ->Arg(32)
    ->Arg(1024)
    ->Arg(32 * 1024)
    ->Arg(1024 * 1024);

// Compute a + sum(x * y).
class WeightedAggSumAccumulator final
    : public Accumulator<AccumulatorType::kAggregator, float,
                         meta::type_list<float>,
                         meta::type_list<float, float>> {
 public:
  void Reset(float a) final { result_ = a; }

  void Add(float x, float y) final { result_ += x * y; }
  void AddN(int64_t count, float x, float y) final { result_ += count * x * y; }

  float GetResult() final { return result_; }

 private:
  float result_;
};

void WeightedAggSumBenchmark(benchmark::State& state, bool same_id_filter,
                             bool with_default_values) {
  int64_t parent_size = 1024;
  int64_t child_size = 1024 * 1024;
  int64_t detail_sparsity = state.range(0);

  absl::BitGen gen;
  ArrayGroupOp<WeightedAggSumAccumulator> agg(GetHeapBufferFactory());
  IdFilter ids1 = RandomIdFilter(child_size, detail_sparsity, gen);
  IdFilter ids2 =
      same_id_filter ? ids1 : RandomIdFilter(child_size, detail_sparsity, gen);
  OptionalValue<float> default_value = std::nullopt;
  if (with_default_values) default_value = 3.0;
  auto arg_x = CreateRandomArray(child_size, ids1, default_value, gen);
  auto arg_y = CreateRandomArray(child_size, ids2, default_value, gen);

  auto arg_a = Array<float>(CreateRandomFullArray(gen, parent_size));

  Buffer<int64_t>::Builder splits_builder(parent_size + 1);
  for (int64_t i = 0; i < parent_size; ++i) {
    splits_builder.Set(i, child_size * i / parent_size);
  }
  splits_builder.Set(parent_size, child_size);

  ArrayEdge edge = *ArrayEdge::FromSplitPoints(
      Array<int64_t>(std::move(splits_builder).Build()));

  for (auto s : state) {
    auto x = agg.Apply(edge, arg_a, arg_x, arg_y);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(child_size * state.iterations());
}

void BM_WeightedAggSum(benchmark::State& state) {
  WeightedAggSumBenchmark(state, false, false);
}

void BM_WeightedAggSum_SameFilter(benchmark::State& state) {
  WeightedAggSumBenchmark(state, true, false);
}

void BM_WeightedAggSum_WithDefaultValue(benchmark::State& state) {
  WeightedAggSumBenchmark(state, false, true);
}

void BM_WeightedAggSum_SameFilter_WithDefaultValue(benchmark::State& state) {
  WeightedAggSumBenchmark(state, true, true);
}

BENCHMARK(BM_WeightedAggSum)->Arg(1)->Arg(4)->Arg(16)->Arg(64);
BENCHMARK(BM_WeightedAggSum_SameFilter)->Arg(1)->Arg(4)->Arg(16)->Arg(64);
BENCHMARK(BM_WeightedAggSum_WithDefaultValue)->Arg(1)->Arg(4)->Arg(16)->Arg(64);
BENCHMARK(BM_WeightedAggSum_SameFilter_WithDefaultValue)
    ->Arg(1)
    ->Arg(4)
    ->Arg(16)
    ->Arg(64);

void AggSumBenchmark(benchmark::State& state, bool mapping_edge) {
  int64_t group_size = state.range(0);
  int64_t sparsity = state.range(1);

  int64_t parent_size = 1024 * 1024;
  int64_t child_size = parent_size * group_size;

  absl::BitGen gen;
  auto arg = CreateRandomArray(
      child_size, RandomIdFilter(child_size, sparsity, gen), std::nullopt, gen);

  ArrayEdge edge;
  if (mapping_edge) {
    Buffer<int64_t>::Builder mapping_bldr(child_size);
    for (int i = 0; i < child_size; ++i) {
      mapping_bldr.Set(i, i / group_size);
    }
    auto mapping = Array<int64_t>(std::move(mapping_bldr).Build());
    edge = ArrayEdge::FromMapping(mapping, parent_size).value();
  } else {
    Buffer<int64_t>::Builder splits_bldr(parent_size + 1);
    for (int i = 0; i < parent_size + 1; ++i) {
      splits_bldr.Set(i, i * group_size);
    }
    auto splits = Array<int64_t>(std::move(splits_bldr).Build());
    edge = ArrayEdge::FromSplitPoints(splits).value();
  }

  ArrayGroupOp<SumAggregator<float>> agg(GetHeapBufferFactory());
  for (auto s : state) {
    auto x = agg.Apply(edge, arg);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(child_size / sparsity * state.iterations());
}

void BM_SparseAggSum_Mapping(benchmark::State& state) {
  AggSumBenchmark(state, /*mapping_edge=*/true);
}

void BM_SparseAggSum_SplitPoints(benchmark::State& state) {
  AggSumBenchmark(state, /*mapping_edge=*/false);
}

BENCHMARK(BM_SparseAggSum_Mapping)
    ->ArgPair(32, 1)
    ->ArgPair(32, 8)
    ->ArgPair(32, 64)
    ->ArgPair(32, 512)
    ->ArgPair(32, 2048)
    ->ArgPair(32, 16384);

BENCHMARK(BM_SparseAggSum_SplitPoints)
    ->ArgPair(32, 1)
    ->ArgPair(32, 8)
    ->ArgPair(32, 64)
    ->ArgPair(32, 512)
    ->ArgPair(32, 2048)
    ->ArgPair(32, 16384);

void BM_AggSumWithSparseMapping(benchmark::State& state) {
  int64_t parent_size = 1024;
  int64_t child_size = 1024 * 1024;
  int64_t detail_sparsity = state.range(0);
  int64_t mapping_sparsity = state.range(1);

  absl::BitGen gen;

  ArrayGroupOp<SumAggregator<float>> agg(GetHeapBufferFactory());
  IdFilter detail_ids = RandomIdFilter(child_size, detail_sparsity, gen);
  IdFilter mapping_ids = RandomIdFilter(child_size, mapping_sparsity, gen);

  auto arg = CreateRandomArray(child_size, detail_ids, std::nullopt, gen);

  int64_t mapping_present_count = mapping_ids.type() == IdFilter::kFull
                                      ? child_size
                                      : mapping_ids.ids().size();
  DenseArrayBuilder<int64_t> mapping_bldr(mapping_present_count);
  for (int64_t i = 0; i < mapping_present_count; ++i) {
    mapping_bldr.Set(i, absl::Uniform(gen, 0, parent_size));
  }
  Array<int64_t> mapping(child_size, mapping_ids,
                         std::move(mapping_bldr).Build());
  ArrayEdge edge = *ArrayEdge::FromMapping(mapping, parent_size);

  for (auto s : state) {
    auto x = agg.Apply(edge, arg);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(child_size * state.iterations());
}

BENCHMARK(BM_AggSumWithSparseMapping)
    ->ArgPair(1, 1)
    ->ArgPair(1, 4)
    ->ArgPair(1, 16)
    ->ArgPair(1, 64)
    ->ArgPair(4, 1)
    ->ArgPair(4, 4)
    ->ArgPair(4, 16)
    ->ArgPair(4, 64)
    ->ArgPair(16, 1)
    ->ArgPair(16, 4)
    ->ArgPair(16, 16)
    ->ArgPair(16, 64);

// Shortcut to DenseGroupOps is used
void BM_AggAll(benchmark::State& state) {
  int64_t size = 20000;
  ArrayGroupOp<AllAggregator> agg(GetHeapBufferFactory());
  DenseArrayBuilder<Unit> bldr(size);
  for (int64_t i = 0; i < size; i += 2) {
    bldr.Set(i, Unit{});
  }
  Array<Unit> arg(std::move(bldr).Build());
  ArrayEdge edge = *ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, size}));

  for (auto s : state) {
    auto x = agg.Apply(edge, arg);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

// This branch is used: array/ops_util.h:132
void BM_AggAllNoDenseShortcut(benchmark::State& state) {
  int64_t size = 20000;
  array_ops_internal::ArrayGroupOpImpl<
      AllAggregator, meta::type_list<>, meta::type_list<Unit>,
      /*ForwardId=*/false, /*UseDenseGroupOps=*/false>
      agg(GetHeapBufferFactory());
  DenseArrayBuilder<Unit> bldr(size);
  for (int64_t i = 0; i < size; i += 2) {
    bldr.Set(i, Unit{});
  }
  Array<Unit> arg(std::move(bldr).Build());
  ArrayEdge edge = *ArrayEdge::FromSplitPoints(CreateArray<int64_t>({0, size}));

  for (auto s : state) {
    auto x = agg.Apply(edge, arg);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

// Reference for BM_AggAll. In theory the perfromace should be the same.
// This branch is used: dense_array/ops/dense_group_ops.h:97
void BM_AggAllOnDenseArray(benchmark::State& state) {
  int64_t size = 20000;
  DenseGroupOps<AllAggregator> agg(GetHeapBufferFactory());
  DenseArrayBuilder<Unit> bldr(size);
  for (int64_t i = 0; i < size; i += 2) {
    bldr.Set(i, Unit{});
  }
  DenseArray<Unit> arg = std::move(bldr).Build();
  DenseArrayEdge edge =
      *DenseArrayEdge::FromSplitPoints(CreateDenseArray<int64_t>({0, size}));

  for (auto s : state) {
    auto x = agg.Apply(edge, arg);
    benchmark::DoNotOptimize(x);
  }
  state.SetItemsProcessed(size * state.iterations());
}

BENCHMARK(BM_AggAll);
BENCHMARK(BM_AggAllOnDenseArray);
BENCHMARK(BM_AggAllNoDenseShortcut);

void BM_ArraysAreEquivalent_DenseForm(benchmark::State& state) {
  const int size = state.range(0);
  absl::BitGen gen{std::seed_seq{}};
  const auto id_filter = RandomIdFilter(size, 2, gen);
  // Avoid initializing the arrays with the same data to avoid fast path.
  gen = absl::BitGen{std::seed_seq{}};
  Array<float> arr1{CreateRandomDenseArray(gen, size)};
  gen = absl::BitGen{std::seed_seq{}};
  Array<float> arr2{CreateRandomDenseArray(gen, size)};
  CHECK(arr1.IsDenseForm());
  CHECK(ArraysAreEquivalent(arr1, arr2));
  benchmark::DoNotOptimize(arr1);
  benchmark::DoNotOptimize(arr2);
  for (auto s : state) {
    bool equiv = ArraysAreEquivalent(arr1, arr2);
    benchmark::DoNotOptimize(equiv);
  }
}

BENCHMARK(BM_ArraysAreEquivalent_DenseForm)->Range(10, 100000);

void BM_ArraysAreEquivalent_SparseForm(benchmark::State& state) {
  const int size = state.range(0);
  absl::BitGen gen{std::seed_seq{1}};
  const auto id_filter = RandomIdFilter(size, 2, gen);
  Array<float> sparse_arr{size, id_filter,
                          CreateRandomDenseArray(gen, id_filter.ids().size())};
  auto compact_sparse_arr = sparse_arr.ToSparseForm();
  // NE if at least one value in sparse_arr's dense data is missing.
  CHECK_NE(sparse_arr.dense_data().size(),
           compact_sparse_arr.dense_data().size());
  CHECK(ArraysAreEquivalent(sparse_arr, compact_sparse_arr));
  benchmark::DoNotOptimize(sparse_arr);
  benchmark::DoNotOptimize(compact_sparse_arr);
  for (auto s : state) {
    bool equiv = ArraysAreEquivalent(sparse_arr, compact_sparse_arr);
    benchmark::DoNotOptimize(equiv);
  }
}

BENCHMARK(BM_ArraysAreEquivalent_SparseForm)->Range(10, 100000);

void BM_ArrayEdge_ComposeEdges_SplitPoints(benchmark::State& state) {
  const int num_edges = state.range(0);
  const int num_children = state.range(1);
  std::vector<ArrayEdge> edges;
  edges.reserve(num_edges);
  for (int i = 0; i < num_edges; ++i) {
    edges.push_back(
        GetSplitPointsEdge(std::pow(num_children, i), num_children));
  }
  const int span_begin = state.range(2);
  const int span_len = state.range(3);
  absl::Span<const ArrayEdge> span =
      absl::MakeSpan(edges).subspan(span_begin, span_len);
  benchmark::DoNotOptimize(span);
  for (auto _ : state) {
    auto composed_edge = ArrayEdge::ComposeEdges(span).value();
    benchmark::DoNotOptimize(composed_edge);
  }
}

BENCHMARK(BM_ArrayEdge_ComposeEdges_SplitPoints)
    // num edges, num children, span begin, span len.
    ->Args({6, 10, 0, 6})
    ->Args({6, 10, 0, 2})
    ->Args({6, 10, 2, 2})
    ->Args({6, 10, 4, 2})
    ->Args({8, 10, 6, 2});  // "Comparable" to mapping test: {6, 10, 4, 2}.

void BM_ArrayEdge_ComposeEdges_Mapping(benchmark::State& state) {
  const int num_edges = state.range(0);
  const int num_children = state.range(1);
  std::vector<ArrayEdge> edges;
  edges.reserve(num_edges);
  for (int i = 0; i < num_edges; ++i) {
    edges.push_back(GetMappingEdge(std::pow(num_children, i), num_children));
  }
  const int span_begin = state.range(2);
  const int span_len = state.range(3);
  absl::Span<const ArrayEdge> span =
      absl::MakeSpan(edges).subspan(span_begin, span_len);
  benchmark::DoNotOptimize(span);
  for (auto _ : state) {
    auto composed_edge = ArrayEdge::ComposeEdges(span).value();
    benchmark::DoNotOptimize(composed_edge);
  }
}

BENCHMARK(BM_ArrayEdge_ComposeEdges_Mapping)
    // num edges, num children, span begin, span len.
    ->Args({6, 10, 0, 6})
    ->Args({6, 10, 0, 2})
    ->Args({6, 10, 2, 2})
    ->Args({6, 10, 4, 2});

void BM_ArrayEdge_ComposeEdges_MappingAndSplitPointsTail(
    benchmark::State& state) {
  const int num_edges = state.range(0);
  const int num_children = state.range(1);
  const int num_mapping_edges = state.range(2);
  std::vector<ArrayEdge> edges;
  edges.reserve(num_edges);
  for (int i = 0; i < num_edges; ++i) {
    if (i < num_mapping_edges) {
      edges.push_back(GetMappingEdge(std::pow(num_children, i), num_children));
    } else {
      edges.push_back(
          GetSplitPointsEdge(std::pow(num_children, i), num_children));
    }
  }
  benchmark::DoNotOptimize(edges);
  for (auto _ : state) {
    auto composed_edge = ArrayEdge::ComposeEdges(edges).value();
    benchmark::DoNotOptimize(composed_edge);
  }
}

BENCHMARK(BM_ArrayEdge_ComposeEdges_MappingAndSplitPointsTail)
    // num edges, num children, num mappings.
    ->Args({6, 10, 1})
    ->Args({6, 10, 3})
    ->Args({6, 10, 6});

}  // namespace
}  // namespace arolla
