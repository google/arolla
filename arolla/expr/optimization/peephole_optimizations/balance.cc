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
#include "arolla/expr/optimization/peephole_optimizations/balance.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

// Optimization to balance chain of binary operations.
absl::Status BinaryBalanceOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  ExprNodePtr d = Placeholder("d");
  for (std::string op :
       {"core.presence_and", "core.presence_or", "bool.logical_or",
        "bool.logical_and", "math.multiply"}) {
    ASSIGN_OR_RETURN(
        ExprNodePtr from1,
        CallOpReference(
            op, {a, CallOpReference(op, {b, CallOpReference(op, {c, d})})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr from2,
        CallOpReference(
            op, {CallOpReference(op, {CallOpReference(op, {a, b}), c}), d}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference(op, {CallOpReference(op, {a, b}),
                                          CallOpReference(op, {c, d})}));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(from1, to));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(from2, to));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<PeepholeOptimizationPack> BalanceOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(BinaryBalanceOptimizations(optimizations));
  return optimizations;
}


}  // namespace arolla::expr
