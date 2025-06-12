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
#include "arolla/expr/optimization/peephole_optimizations/reduce.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

// Optimizes summing chains (a+b+c+...) using math._add4 operator.
absl::Status AppendAdd4Optimizations(PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  ExprNodePtr d = Placeholder("d");

  auto Add = [](auto a, auto b) { return CallOpReference("math.add", {a, b}); };

  // Patterns
  ASSIGN_OR_RETURN(ExprNodePtr pattern1, Add(Add(a, b), Add(c, d)));
  ASSIGN_OR_RETURN(ExprNodePtr pattern2, Add(Add(Add(a, b), c), d));
  ASSIGN_OR_RETURN(ExprNodePtr pattern3, Add(a, Add(b, Add(c, d))));

  // Replacement
  ASSIGN_OR_RETURN(ExprNodePtr replacement,
                   CallOpReference("math._add4", {a, b, c, d}));

  ASSIGN_OR_RETURN(
      optimizations.emplace_back(),
      PeepholeOptimization::CreatePatternOptimization(pattern1, replacement));
  ASSIGN_OR_RETURN(
      optimizations.emplace_back(),
      PeepholeOptimization::CreatePatternOptimization(pattern2, replacement));
  ASSIGN_OR_RETURN(
      optimizations.emplace_back(),
      PeepholeOptimization::CreatePatternOptimization(pattern3, replacement));
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<PeepholeOptimizationPack> ReduceOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(AppendAdd4Optimizations(optimizations));
  return optimizations;
}

}  // namespace arolla::expr
