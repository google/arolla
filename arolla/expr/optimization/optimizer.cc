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
#include "arolla/expr/optimization/optimizer.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

namespace {
constexpr int kPeepholeOptimizerIterationsLimit = 100;
}  // namespace

Optimizer MakeOptimizer(std::unique_ptr<PeepholeOptimizer> peephole_optimizer) {
  return [peephole_optimizer = std::shared_ptr<PeepholeOptimizer>(
              std::move(peephole_optimizer))](
             ExprNodePtr expr) -> absl::StatusOr<ExprNodePtr> {
    ExprNodePtr previous_expr;
    int iteration = 0;
    do {
      if (++iteration > kPeepholeOptimizerIterationsLimit) {
        return absl::InternalError(absl::StrFormat(
            "too many iterations of peephole optimizer; this may indicate that "
            "the set of optimizations contains cycles, or just too big "
            "expression unsupported by the optimizer (last iterations: %s vs "
            "%s)",
            // TODO: Print diff instead of two snippets.
            GetDebugSnippet(previous_expr), GetDebugSnippet(expr)));
      }
      previous_expr = expr;
      ASSIGN_OR_RETURN(expr, peephole_optimizer->ApplyToNode(expr));
      if (expr->qtype() != previous_expr->qtype()) {
        return absl::InternalError(absl::StrFormat(
            "expression %s was optimized into %s, which changed its output "
            "type from %s to %s; this indicates incorrect optimization",
            GetDebugSnippet(previous_expr), GetDebugSnippet(expr),
            previous_expr->qtype() != nullptr ? previous_expr->qtype()->name()
                                              : "NULL",
            expr->qtype() != nullptr ? expr->qtype()->name() : "NULL"));
      }
    } while (previous_expr->fingerprint() != expr->fingerprint());
    return expr;
  };
}

}  // namespace arolla::expr
