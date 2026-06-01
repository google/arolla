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
#include "arolla/expr/optimization/peephole_optimizer.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using MatchersMap =
    absl::flat_hash_map<std::string, PeepholeOptimization::NodeMatcher>;

bool PlaceholderMatches(absl::string_view key,
                        const MatchersMap& placeholder_matchers,
                        const ExprNodePtr& candidate) {
  if (auto matcher_it = placeholder_matchers.find(key);
      matcher_it != placeholder_matchers.end()) {
    const auto& matcher = matcher_it->second;
    return matcher(candidate);
  }
  return true;
}

// Rebuilds the expression with substituted placeholders.
absl::StatusOr<ExprNodePtr> SubstitutePlaceholders(
    const PostOrder& post_order,
    absl::Span<const std::pair<Fingerprint, const ExprNode*>> subs) {
  return TransformOnPostOrder(
      post_order, [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        if (!node->is_placeholder()) {
          return node;
        }
        for (const auto& [key, sub] : subs) {
          if (key == node->fingerprint()) {
            return ExprNodePtr::NewRef(sub);
          }
        }
        return absl::InvalidArgumentError(absl::StrFormat(
            "No value was provided for P.%s.", node->placeholder_key()));
      });
}

struct PatternOptimizationData {
  ExprNodePtr from;
  PostOrder to_post_order;
  MatchersMap placeholder_matchers;
  PeepholeOptimization::PatternKey key;
};

// Optimization that detects pattern (`from`) and transform it to `to`.
// All placeholders in the pattern need to satisfy `placeholder_matchers`
// conditions.
class PatternOptimization final : public PeepholeOptimization {
 public:
  explicit PatternOptimization(PatternOptimizationData data)
      : data_(std::move(data)) {}

  std::optional<PatternKey> GetKey() const final { return data_.key; }

  absl::StatusOr<ExprNodePtr> ApplyToRoot(
      const ExprNodePtr& root) const override {
    struct MatchingCandidate {
      // node or subnode we are trying to optimize
      const ExprNodePtr& candidate;
      // pattern we are matching (`from`)
      const ExprNodePtr& pattern;
    };

    // maps Fingerprint in `from` optimization to the corresponded node in
    // `root`.
    absl::InlinedVector<std::pair<Fingerprint, Fingerprint>, 8> opt2root;
    absl::InlinedVector<MatchingCandidate, 8> stack;
    stack.push_back({.candidate = root, .pattern = data_.from});
    // Adds candidate to stack. Returns false if a non-identical candidate
    // matching the same pattern.
    auto add_to_stack = [&](MatchingCandidate candidate) -> bool {
      for (const auto& [p_fp, c_fp] : opt2root) {
        if (p_fp == candidate.pattern->fingerprint()) {
          return c_fp == candidate.candidate->fingerprint();
        }
      }
      opt2root.push_back({candidate.pattern->fingerprint(),
                          candidate.candidate->fingerprint()});
      stack.push_back(std::move(candidate));
      return true;
    };
    absl::InlinedVector<std::pair<Fingerprint, const ExprNode*>, 4>
        placeholder_subs;
    while (!stack.empty()) {
      MatchingCandidate candidate = stack.back();
      stack.pop_back();
      if (candidate.pattern->is_literal()) {
        // verify exact match for literals
        if (!candidate.candidate->is_literal() ||
            (candidate.pattern->fingerprint() !=
             candidate.candidate->fingerprint())) {
          return root;
        }
        continue;
      }
      if (candidate.pattern->is_placeholder()) {
        if (!PlaceholderMatches(candidate.pattern->placeholder_key(),
                                data_.placeholder_matchers,
                                candidate.candidate)) {
          return root;
        }
        placeholder_subs.push_back(
            {candidate.pattern->fingerprint(), candidate.candidate.get()});
        continue;
      }
      DCHECK(candidate.pattern->is_op())
          << "Internal error: unexpected node type: "
          << ToDebugString(candidate.pattern);
      // both must be operation
      if (!candidate.pattern->is_op() || !candidate.candidate->is_op()) {
        return root;
      }
      const auto& opt_deps = candidate.pattern->node_deps();
      const auto& root_deps = candidate.candidate->node_deps();
      // number of dependencies must match
      if (root_deps.size() != opt_deps.size()) {
        return root;
      }
      // operator names must match
      // NOTE: Consider checking the name of the decayed operator instead.
      if (candidate.pattern->op()->display_name() !=
          candidate.candidate->op()->display_name()) {
        return root;
      }
#ifndef NDEBUG
      ASSIGN_OR_RETURN(auto decayed_op,
                       DecayRegisteredOperator(candidate.candidate->op()));
      if (!HasBackendExprOperatorTag(decayed_op) &&
          !HasBuiltinExprOperatorTag(decayed_op)) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "tried applying a peephole optimization to operator %s."
            " which is neither backend nor builtin. Is "
            "your peephole optimization correct?",
            candidate.candidate->op()->display_name()));
      }
#endif
      // Trying to add children to the stack.
      for (size_t i = 0; i != root_deps.size(); ++i) {
        if (!add_to_stack(
                {.candidate = root_deps[i], .pattern = opt_deps[i]})) {
          return root;
        }
      }
    }
    return SubstitutePlaceholders(data_.to_post_order, placeholder_subs);
  }

 private:
  PatternOptimizationData data_;
};

// Optimization that applies specific transformation function.
class TransformOptimization final : public PeepholeOptimization {
 public:
  explicit TransformOptimization(
      std::function<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> transform_fn)
      : transform_fn_(std::move(transform_fn)) {}

  absl::StatusOr<ExprNodePtr> ApplyToRoot(const ExprNodePtr& root) const final {
    return transform_fn_(root);
  }

 private:
  std::function<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> transform_fn_;
};

}  // namespace

ReferenceToRegisteredOperator::ReferenceToRegisteredOperator(
    absl::string_view name)
    : ExprOperator(
          name, FingerprintHasher("arolla::expr::ReferenceToRegisteredOperator")
                    .Combine(name)
                    .Finish()) {}

absl::StatusOr<std::string> ReferenceToRegisteredOperator::GetDoc() const {
  return absl::UnimplementedError(
      "ReferenceToRegisteredOperator::GetDoc is not implemented.");
}

absl::StatusOr<ExprOperatorSignaturePtr absl_nonnull>
ReferenceToRegisteredOperator::GetSignature() const {
  static const absl::NoDestructor signature(
      std::make_shared<const ExprOperatorSignature>(
          ExprOperatorSignature::MakeVariadicArgs()));
  return *signature;
}

absl::StatusOr<ExprAttributes> ReferenceToRegisteredOperator::InferAttributes(
    absl::Span<const ExprAttributes> /*inputs*/) const {
  return ExprAttributes{};
}

absl::StatusOr<ExprNodePtr> CallOpReference(
    absl::string_view op_name,
    std::initializer_list<absl::StatusOr<ExprNodePtr>> status_or_args) {
  return CallOp(std::make_shared<ReferenceToRegisteredOperator>(op_name),
                status_or_args);
}

PeepholeOptimization::PatternKey::PatternKey(const ExprNodePtr& expr) {
  if (expr->is_op()) {
    // We use only operator names for initial filtration, but later do accurate
    // matching via OperatorMatches().
    hash_ = absl::HashOf(expr->op()->display_name());
  } else {
    hash_ = absl::HashOf(expr->fingerprint());
  }
}

bool PeepholeOptimization::PatternKey::operator==(
    const PatternKey& other) const {
  return hash_ == other.hash_;
}
bool PeepholeOptimization::PatternKey::operator!=(
    const PatternKey& other) const {
  return !(*this == other);
}

absl::StatusOr<std::unique_ptr<PeepholeOptimization>>
PeepholeOptimization::CreatePatternOptimization(
    ExprNodePtr from, ExprNodePtr to,
    absl::flat_hash_map<std::string, NodeMatcher> placeholder_matchers) {
  if (from->is_placeholder()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("from EXPRession is placeholder, which would match "
                        "everything: %s -> %s",
                        ToDebugString(from), ToDebugString(to)));
  }
  if (!GetLeafKeys(from).empty() || !GetLeafKeys(to).empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("leaves are not allowed in optimizations: %s -> %s",
                        ToDebugString(from), ToDebugString(to)));
  }
  absl::flat_hash_set<std::string> from_keys_set;
  for (const auto& key : GetPlaceholderKeys(from)) {
    from_keys_set.insert(key);
  }
  std::vector<std::string> unknown_to_keys;
  for (const auto& key : GetPlaceholderKeys(to)) {
    if (!from_keys_set.contains(key)) {
      unknown_to_keys.push_back(key);
    }
  }
  if (!unknown_to_keys.empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unknown placeholder keys in to expression: %s, %s->%s",
                        absl::StrJoin(unknown_to_keys, ","),
                        ToDebugString(from), ToDebugString(to)));
  }
  std::vector<std::string> unknown_matcher_keys;
  for (const auto& [key, _] : placeholder_matchers) {
    if (!from_keys_set.contains(key)) {
      unknown_matcher_keys.push_back(key);
    }
  }
  if (!unknown_matcher_keys.empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("unknown placeholder matcher keys: %s, %s->%s",
                        absl::StrJoin(unknown_matcher_keys, ","),
                        ToDebugString(from), ToDebugString(to)));
  }
  // Replaces all ReferenceToRegisteredOperator instances with the corresponding
  // RegisteredOperator in `to`.
  ASSIGN_OR_RETURN(
      to, Transform(to, [](ExprNodePtr node) {
        if (!node->is_op() ||
            typeid(*node->op()) != typeid(ReferenceToRegisteredOperator)) {
          return node;
        }
        // NOTE: Manually constructing the RegisteredOperator and using
        // ExprNode::UnsafeMakeOperatorNode works even if the operator is not
        // yet present in the registry.
        return ExprNode::UnsafeMakeOperatorNode(
            std::make_shared<RegisteredOperator>(node->op()->display_name()),
            std::vector(node->node_deps()), {});
      }));
  auto key = PatternKey(from);
  return std::make_unique<PatternOptimization>(PatternOptimizationData{
      std::move(from), PostOrder(to), std::move(placeholder_matchers), key});
}

absl::StatusOr<std::unique_ptr<PeepholeOptimization>>
PeepholeOptimization::CreateTransformOptimization(
    std::function<absl::StatusOr<ExprNodePtr>(ExprNodePtr)> transform_fn) {
  return std::make_unique<TransformOptimization>(std::move(transform_fn));
}

struct PeepholeOptimizer::Data {
  absl::flat_hash_map<PeepholeOptimization::PatternKey,
                      std::vector<std::unique_ptr<PeepholeOptimization>>>
      pattern_optimizations;
  std::vector<std::unique_ptr<PeepholeOptimization>> transform_optimizations;
};

absl::StatusOr<ExprNodePtr> PeepholeOptimizer::ApplyToNode(
    ExprNodePtr node) const {
  const auto& pattern_optimizations = data_->pattern_optimizations;
  const auto key = PeepholeOptimization::PatternKey(node);
  if (auto it = pattern_optimizations.find(key);
      it != pattern_optimizations.end()) {
    for (const auto& optimization : it->second) {
      ASSIGN_OR_RETURN(node, optimization->ApplyToRoot(node));
      if (PeepholeOptimization::PatternKey(node) != key) {
        break;
      }
    }
  }
  for (const auto& optimization : data_->transform_optimizations) {
    ASSIGN_OR_RETURN(node, optimization->ApplyToRoot(node));
  }
  return node;
}

absl::StatusOr<ExprNodePtr> PeepholeOptimizer::Apply(ExprNodePtr root) const {
  return Transform(root,
                   [this](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
                     return ApplyToNode(node);
                   });
}

PeepholeOptimizer::~PeepholeOptimizer() = default;
PeepholeOptimizer::PeepholeOptimizer(std::unique_ptr<Data> data)
    : data_(std::move(data)) {}

absl::StatusOr<std::unique_ptr<PeepholeOptimizer>> PeepholeOptimizer::Create(
    std::vector<std::unique_ptr<PeepholeOptimization>> optimizations) {
  auto data = std::make_unique<Data>();
  for (auto& opt : optimizations) {
    if (auto key = opt->GetKey()) {
      data->pattern_optimizations[*key].push_back(std::move(opt));
    } else {
      data->transform_optimizations.push_back(std::move(opt));
    }
  }
  return absl::WrapUnique(new PeepholeOptimizer(std::move(data)));
}

absl::StatusOr<std::unique_ptr<PeepholeOptimizer>> CreatePeepholeOptimizer(
    absl::Span<const PeepholeOptimizationPackFactory>
        optimization_pack_factories) {
  PeepholeOptimizationPack optimizations;
  for (const auto& factory : optimization_pack_factories) {
    ASSIGN_OR_RETURN(PeepholeOptimizationPack pack, factory());
    optimizations.reserve(optimizations.size() + pack.size());
    std::move(pack.begin(), pack.end(), std::back_inserter(optimizations));
  }
  return PeepholeOptimizer::Create(std::move(optimizations));
}

}  // namespace arolla::expr
