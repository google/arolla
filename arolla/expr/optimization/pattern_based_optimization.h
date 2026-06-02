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
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {
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

}  // namespace pattern_based_optimization_internal
}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_PATTERN_BASED_OPTIMIZATION_H_
