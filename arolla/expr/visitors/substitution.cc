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
#include "arolla/expr/visitors/substitution.h"

#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {
namespace {

// Substitutes a node iff predicate function result matches a key in subs.
// `key_fn` must have signature: std::optional<Key>(const ExprNodePtr&).
// `key_fn` is guaranteed to be called exactly once for all nodes in `expr`.
template <class Key, class KeyFn>
absl::StatusOr<ExprNodePtr> Substitute(
    ExprNodePtr expr, const absl::flat_hash_map<Key, ExprNodePtr>& subs,
    KeyFn key_fn) {
  return PostOrderTraverse(
      expr,
      [&](const ExprNodePtr& node, absl::Span<const ExprNodePtr* const> visits)
          -> absl::StatusOr<ExprNodePtr> {
        if (auto key = key_fn(node); key.has_value()) {
          if (auto it = subs.find(*key); it != subs.end()) {
            return it->second;
          }
        }
        return WithNewDependencies(node, DereferenceVisitPointers(visits));
      });
}

}  // namespace

absl::StatusOr<ExprNodePtr> SubstituteByName(
    ExprNodePtr expr,
    const absl::flat_hash_map<std::string, ExprNodePtr>& subs) {
  return Substitute(expr, subs,
                    [](const auto& expr) -> std::optional<std::string> {
                      if (IsNameAnnotation(expr)) {
                        return std::string(ReadNameAnnotation(expr));
                      }
                      return std::nullopt;
                    });
}

absl::StatusOr<ExprNodePtr> SubstituteLeaves(
    ExprNodePtr expr,
    const absl::flat_hash_map<std::string, ExprNodePtr>& subs) {
  return Substitute(expr, subs,
                    [](const auto& expr) -> std::optional<std::string> {
                      if (expr->is_leaf()) return expr->leaf_key();
                      return std::nullopt;
                    });
}

absl::StatusOr<ExprNodePtr> SubstitutePlaceholders(
    ExprNodePtr expr, const absl::flat_hash_map<std::string, ExprNodePtr>& subs,
    bool must_substitute_all) {
  return PostOrderTraverse(
      expr,
      [&](const ExprNodePtr& node, absl::Span<const ExprNodePtr* const> visits)
          -> absl::StatusOr<ExprNodePtr> {
        if (node->is_placeholder()) {
          if (subs.contains(node->placeholder_key())) {
            return subs.at(node->placeholder_key());
          } else if (must_substitute_all) {
            return absl::InvalidArgumentError(absl::StrFormat(
                "No value was provided for P.%s, but substitution of all "
                "placeholders was requested.",
                node->placeholder_key()));
          }
        }
        return WithNewDependencies(node, DereferenceVisitPointers(visits));
      });
}

absl::StatusOr<ExprNodePtr> SubstituteByFingerprint(
    ExprNodePtr expr,
    const absl::flat_hash_map<Fingerprint, ExprNodePtr>& subs) {
  return Substitute(expr, subs,
                    [](const auto& expr) -> std::optional<Fingerprint> {
                      return expr->fingerprint();
                    });
}

}  // namespace arolla::expr
