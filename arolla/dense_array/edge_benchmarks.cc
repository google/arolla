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
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"

namespace arolla {
namespace {

DenseArrayEdge GetSplitPointsEdge(int64_t parent_size, int64_t children) {
  std::vector<OptionalValue<int64_t>> split_points;
  split_points.reserve(parent_size + 1);
  for (int64_t i = 0; i <= parent_size; ++i) {
    split_points.push_back(i * children);
  }
  return DenseArrayEdge::FromSplitPoints(
             CreateDenseArray<int64_t>(split_points))
      .value();
}

DenseArrayEdge GetMappingEdge(int64_t parent_size, int64_t children) {
  std::vector<OptionalValue<int64_t>> mapping;
  mapping.reserve(parent_size * children);
  for (int64_t i = 0; i < parent_size; ++i) {
    for (int64_t j = 0; j < children; ++j) {
      mapping.push_back(i);
    }
  }
  return DenseArrayEdge::FromMapping(CreateDenseArray<int64_t>(mapping),
                                     parent_size)
      .value();
}

void BM_DenseArrayEdge_ComposeEdges_SplitPoints(benchmark::State& state) {
  const int num_edges = state.range(0);
  const int num_children = state.range(1);
  std::vector<DenseArrayEdge> edges;
  edges.reserve(num_edges);
  for (int i = 0; i < num_edges; ++i) {
    edges.push_back(
        GetSplitPointsEdge(std::pow(num_children, i), num_children));
  }
  const int span_begin = state.range(2);
  const int span_len = state.range(3);
  absl::Span<const DenseArrayEdge> span =
      absl::MakeSpan(edges).subspan(span_begin, span_len);
  benchmark::DoNotOptimize(span);
  for (auto _ : state) {
    auto composed_edge = DenseArrayEdge::ComposeEdges(span).value();
    benchmark::DoNotOptimize(composed_edge);
  }
}

BENCHMARK(BM_DenseArrayEdge_ComposeEdges_SplitPoints)
    // num edges, num children, span begin, span len.
    ->Args({6, 10, 0, 6})
    ->Args({6, 10, 0, 2})
    ->Args({6, 10, 2, 2})
    ->Args({6, 10, 4, 2})
    ->Args({8, 10, 6, 2});  // "Comparable" to mapping test: {6, 10, 4, 2}.

void BM_DenseArrayEdge_ComposeEdges_Mapping(benchmark::State& state) {
  const int num_edges = state.range(0);
  const int num_children = state.range(1);
  std::vector<DenseArrayEdge> edges;
  edges.reserve(num_edges);
  for (int i = 0; i < num_edges; ++i) {
    edges.push_back(GetMappingEdge(std::pow(num_children, i), num_children));
  }
  const int span_begin = state.range(2);
  const int span_len = state.range(3);
  absl::Span<const DenseArrayEdge> span =
      absl::MakeSpan(edges).subspan(span_begin, span_len);
  benchmark::DoNotOptimize(span);
  for (auto _ : state) {
    auto composed_edge = DenseArrayEdge::ComposeEdges(span).value();
    benchmark::DoNotOptimize(composed_edge);
  }
}

BENCHMARK(BM_DenseArrayEdge_ComposeEdges_Mapping)
    // num edges, num children, span begin, span len.
    ->Args({6, 10, 0, 6})
    ->Args({6, 10, 0, 2})
    ->Args({6, 10, 2, 2})
    ->Args({6, 10, 4, 2});

void BM_DenseArrayEdge_ComposeEdges_MappingAndSplitPointsTail(
    benchmark::State& state) {
  const int num_edges = state.range(0);
  const int num_children = state.range(1);
  const int num_mapping_edges = state.range(2);
  std::vector<DenseArrayEdge> edges;
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
    auto composed_edge = DenseArrayEdge::ComposeEdges(edges).value();
    benchmark::DoNotOptimize(composed_edge);
  }
}

BENCHMARK(BM_DenseArrayEdge_ComposeEdges_MappingAndSplitPointsTail)
    // num edges, num children, num mappings.
    ->Args({6, 10, 1})
    ->Args({6, 10, 3})
    ->Args({6, 10, 6});

}  // namespace
}  // namespace arolla
