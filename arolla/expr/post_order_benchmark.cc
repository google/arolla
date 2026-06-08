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
// Benchmarks for arolla::expr::PostOrder construction.
//
// We benchmark PostOrder on four families of expressions and, for each family,
// generate multiple random samples so that the benchmark iterates over them
// round-robin. This reduces the potential impact of lucky or unlucky hashing,
// branch prediction, and caching effects.
//
// Families
// --------
//  Linear   — a chain of binary ops: op(op(op(leaf, leaf), leaf), leaf) ...
//  Balanced — a balanced binary tree built bottom-up
//  Bushy    — a balanced tree with arity=8
//  DAG      — heavy subexpression sharing: nodes reuse earlier subtrees

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/algorithm/container.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/testing/testing.h"

namespace arolla::expr {
namespace {

using ::arolla::testing::MockExprOperator;

// Returns a new variadic operator with unique fingerprint.
ExprOperatorPtr MakeBenchOp() { return MockExprOperator::MakeNice(); }

ExprNodePtr MakeBenchOpNode(ExprOperatorPtr op,
                            absl::Span<const ExprNodePtr> deps) {
  return ExprNode::UnsafeMakeOperatorNode(
      std::move(op), std::vector<ExprNodePtr>(deps.begin(), deps.end()),
      ExprAttributes{});
}

// Linear chain of `n` leaves connected by a binary operator.
ExprNodePtr MakeLinearChain(const ExprOperatorPtr& op, size_t n) {
  DCHECK_GE(n, 1);
  ExprNodePtr cur = Leaf(absl::StrCat("v", 0));
  for (size_t i = 1; i < n; ++i) {
    cur = MakeBenchOpNode(op, {std::move(cur), Leaf(absl::StrCat("v", i))});
  }
  return cur;
}

// Balanced tree with `n` leaves and operator with `fan_out` children.
ExprNodePtr MakeBalancedTree(const ExprOperatorPtr& op, size_t n,
                             size_t fan_out) {
  DCHECK_GE(n, 1);
  std::vector<ExprNodePtr> nodes;
  nodes.reserve(n);
  for (int64_t i = 0; i < n; ++i) {
    nodes.push_back(Leaf(absl::StrCat("v", i)));
  }
  while (nodes.size() >= fan_out) {
    absl::Span<ExprNodePtr> span = absl::MakeSpan(nodes);
    size_t i = 0;
    for (; span.size() >= fan_out; span.remove_prefix(fan_out)) {
      nodes[i++] = MakeBenchOpNode(op, span.subspan(0, fan_out));
    }
    nodes.erase(absl::c_move(span, nodes.begin() + i), nodes.end());
  }
  if (nodes.size() > 1) {
    nodes[0] = MakeBenchOpNode(op, nodes);
  }
  return nodes[0];
}

// DAG with heavy sharing: we build `n` unique nodes, but later nodes reuse
// earlier ones as dependencies, specifically, node[i] depends on
// node[i-1] and node[i/2]. This creates significant subexpression sharing
// because node[i/2] is referenced by many later nodes.
ExprNodePtr MakeDAG(const ExprOperatorPtr& op, size_t n) {
  DCHECK_GE(n, 1);
  std::vector<ExprNodePtr> nodes;
  nodes.reserve(n);
  nodes.push_back(Leaf("v0"));
  if (n >= 2) {
    nodes.push_back(Leaf("v1"));
  }
  for (size_t i = 2; i < n; ++i) {
    nodes.push_back(MakeBenchOpNode(op, {nodes[i - 1], nodes[i / 2]}));
  }
  return nodes.back();
}

constexpr size_t kSampleCount = 16;

template <typename SampleFn>
void BM_PostOrder_WithSampleFn(benchmark::State& state, SampleFn sample_fn) {
  size_t n = state.range(0);
  std::vector<ExprNodePtr> samples;
  samples.reserve(kSampleCount);
  for (size_t i = 0; i < kSampleCount; ++i) {
    // Use an operator with unique fingerprint for each sample.
    samples.push_back(sample_fn(MakeBenchOp(), n));
  }
  size_t idx = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(samples[idx]);
    auto post_order = PostOrder(samples[idx]);
    benchmark::DoNotOptimize(post_order);
    idx = (idx + 1) % kSampleCount;
  }
}

void BM_PostOrder_Linear(benchmark::State& state) {
  return BM_PostOrder_WithSampleFn(state, &MakeLinearChain);
}

void BM_PostOrder_Balanced(benchmark::State& state) {
  return BM_PostOrder_WithSampleFn(
      state, [](const ExprOperatorPtr& op, size_t n) {
        return MakeBalancedTree(op, n, /*fan_out=*/2);
      });
}

void BM_PostOrder_Bushy(benchmark::State& state) {
  return BM_PostOrder_WithSampleFn(
      state, [](const ExprOperatorPtr& op, size_t n) {
        return MakeBalancedTree(op, n, /*fan_out=*/8);
      });
}

void BM_PostOrder_DAG(benchmark::State& state) {
  return BM_PostOrder_WithSampleFn(state, &MakeDAG);
}

BENCHMARK(BM_PostOrder_Linear)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_PostOrder_Balanced)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_PostOrder_Bushy)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_PostOrder_DAG)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace
}  // namespace arolla::expr
