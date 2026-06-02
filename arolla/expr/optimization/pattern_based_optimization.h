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
#ifndef AROLLA_EXPR_OPTIMIZATION_PATTERN_BASED_OPTIMIZATION_H_
#define AROLLA_EXPR_OPTIMIZATION_PATTERN_BASED_OPTIMIZATION_H_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

// Creates optimization converting `from` pattern to `to`.
// Both `from` and `to` expected to contain operators, literals
// and placeholders (but no leaves).
// Optimization is applied if `from` matching the expression.
// 1. Placeholder matching any EXPRession. Placeholders with the same key must
//    match the same expression
// 2. Literals matching only exact literals (with exact type).
// 3. Operators are matching nodes with the same operator and arguments.
//
// Placeholder in `to` must be also present in `from`.
// Application of optimization will be `to` expression with placeholders
// substituted to the matched nodes.
//
// placeholder_matchers are optional additional matchers for the placeholders.
// If matcher is not specified, we assume [](auto) { return true; }.
//
// Usage examples:
// Returns optimization converting `a ** 2` to `a * a`.
// absl::StatusOr<std::unique_ptr<PeepholeOptimization>>
// SquareA2AxAOptimization() {
//   ASSIGN_OR_RETURN(ExprNodePtr square_a,
//                    CallOp("math.pow", {Placeholder("a"), Literal(2.f)}));
//   ASSIGN_OR_RETURN(
//       ExprNodePtr axa,
//       CallOp("math.multiply", {Placeholder("a"), Placeholder("a")}));
//   return CreatePatternBasedOptimization(square_a, axa);
// }
// ASSIGN_OR_RETURN(auto optimization, SquareA2AxAOptimization());
// ASSIGN_OR_RETURN(ExprNodePtr expr,
//                  CallOp("math.pow", {Leaf("x"), Literal(2.f)}));
// ASSIGN_OR_RETURN(ExprNodePtr expected_expr,
//                  CallOp("math.multiply", {Leaf("x"), Leaf("x")}));
// EXPECT_THAT(optimization->ApplyToRoot(expr),
//             IsOkAndHolds(EqualsExpr(expected_expr)));
absl::StatusOr<std::unique_ptr<PeepholeOptimization> absl_nonnull>
CreatePatternBasedOptimization(
    ExprNodePtr absl_nonnull from, ExprNodePtr absl_nonnull to,
    absl::flat_hash_map<std::string,
                        PeepholeOptimization::NodeMatcher absl_nonnull>
        placeholder_matchers = {});

namespace pattern_based_optimization_internal {

struct PatternPlan {
  struct UnfoldOperatorNode {
    size_t mem_index;
    size_t expected_arity;
    std::string expected_op_name;
  };

  struct CheckFixedFingerprint {
    size_t mem_index;
    Fingerprint expected_fingerprint;
  };

  struct CheckSameFingerprintAs {
    size_t mem_index;
    size_t other_mem_index;
  };

  struct CheckPredicate {
    size_t mem_index;
    PeepholeOptimization::NodeMatcher predicate;
  };

  // Minimum memory required to execute the pattern plan.
  size_t capacity;

  std::vector<UnfoldOperatorNode> unfold_actions;
  std::vector<CheckFixedFingerprint> check_fixed_fingerprint_actions;
  std::vector<CheckSameFingerprintAs> check_same_fingerprint_as_actions;
  std::vector<CheckPredicate> check_predicates;
};

// Matches the pattern against the root node. `memory` is used as additional
// storage, its size MUST be at least `pattern.capacity`. Returns true if
// the pattern matched, in which case the `memory` will contain the nodes
// visited during the pattern matching.
absl::StatusOr<bool> MatchPattern(
    const PatternPlan& pattern, const ExprNode& root,
    absl::Span<const ExprNode*> /*mutable*/ memory);

// Rebuilds the expression defined by post_order with nodes substituted from
// memory.
absl::StatusOr<ExprNodePtr absl_nonnull> SubstituteNodes(
    const PostOrder& post_order,
    absl::Span<const std::pair<size_t, size_t>> post_order_mem_mapping,
    absl::Span<const ExprNode* const> memory);

// Result of compiling a pattern.
struct CompilePatternResult {
  PatternPlan pattern_plan;
  PostOrder to_post_order;
  // Pairs of (node_index in to_post_order, mem_index in pattern_plan),
  // indicating nodes that should be substituted in the output expression.
  std::vector<std::pair<size_t, size_t>> to_post_order_mem_mapping;
};

// Prepares a pattern for pattern matching optimization for efficient
// computation.
absl::StatusOr<CompilePatternResult> CompilePattern(
    ExprNodePtr absl_nonnull from, ExprNodePtr absl_nonnull to,
    absl::flat_hash_map<std::string,
                        PeepholeOptimization::NodeMatcher absl_nonnull>
        placeholder_matchers);

}  // namespace pattern_based_optimization_internal
}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_PATTERN_BASED_OPTIMIZATION_H_
