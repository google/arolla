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
#include "arolla/expr/optimization/peephole_optimizer.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
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

struct MatchingCandidate {
  const ExprNodePtr& candidate;  // node or subnode we are trying to optimize
  const ExprNodePtr& pattern;    // pattern we are matching (`from`)
};

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

// Replaces all occurrences of ReferenceToRegisteredOperator with the
// corresponding RegisteredOperator and substitute placeholders.
absl::StatusOr<ExprNodePtr> DecayReferencesToRegisteredOperator(
    const PostOrder& node_visitor_order,
    const absl::flat_hash_map<std::string, ExprNodePtr>& subs) {
  return TransformOnPostOrder(
      node_visitor_order, [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        if (node->is_op() &&
            typeid(*node->op()) == typeid(ReferenceToRegisteredOperator)) {
          return BindOp(node->op()->display_name(), node->node_deps(), {});
        }
        if (node->is_placeholder()) {
          if (subs.contains(node->placeholder_key())) {
            return subs.at(node->placeholder_key());
          } else {
            return absl::InvalidArgumentError(absl::StrFormat(
                "No value was provided for P.%s.", node->placeholder_key()));
          }
        }
        return node;
      });
}

struct PatternOptimizationData {
  ExprNodePtr from;
  PostOrder to_visitor_order;
  MatchersMap placeholder_matchers;
  PeepholeOptimization::PatternKey key;
};

// Optimiziation that detects pattern (`from`) and transform it to `to`.
// All placeholders in the pattern need to satisfy `placeholder_matchers`
// conditions.
class PatternOptimization : public PeepholeOptimization {
 public:
  explicit PatternOptimization(PatternOptimizationData data)
      : data_(std::move(data)) {}

  std::optional<PeepholeOptimization::PatternKey> GetKey() const final {
    return data_.key;
  }

  absl::StatusOr<ExprNodePtr> ApplyToRoot(
      const ExprNodePtr& root) const override {
    // maps Fingerprint in `from` optimization to the corresponded node in
    // `root`.
    absl::flat_hash_map<Fingerprint, Fingerprint> opt2root;
    std::queue<MatchingCandidate> queue;
    queue.push({.candidate = root, .pattern = data_.from});
    // Adds candidate to queue. Returns false if a non-identical candidate
    // matching the same pattern.
    auto add_to_queue = [&](MatchingCandidate candidate) -> bool {
      if (auto [it, success] =
              opt2root.emplace(candidate.pattern->fingerprint(),
                               candidate.candidate->fingerprint());
          !success) {
        // Equal nodes in optimization should correspond to the equal nodes in
        // the root. E. g. L.a + L.b shouldn't be matched by pattern P.x + P.x.
        return it->second == candidate.candidate->fingerprint();
      }
      queue.push(std::move(candidate));
      return true;
    };
    absl::flat_hash_map<std::string, ExprNodePtr> placeholder_subs;
    while (!queue.empty()) {
      MatchingCandidate candidate = queue.front();
      queue.pop();
      if (candidate.pattern->is_literal()) {
        // verify exact match for literals
        if (!candidate.candidate->is_literal() ||
            (candidate.pattern->fingerprint() !=
             candidate.candidate->fingerprint())) {
          return root;
        }
        continue;
      }
      // defensive check, no leaves are expected.
      if (candidate.pattern->is_leaf()) {
        LOG(FATAL) << "Internal error: leaves are not expected.";
        return root;
      }
      if (candidate.pattern->is_placeholder()) {
        absl::string_view key = candidate.pattern->placeholder_key();
        if (!PlaceholderMatches(key, data_.placeholder_matchers,
                                candidate.candidate)) {
          return root;
        }
        auto [it, success] = placeholder_subs.emplace(key, candidate.candidate);
        DCHECK(success)
            << "Internal error: each node of the pattern with the same "
               "fingerprint must be added to the queue only once.";
        continue;
      }
      DCHECK(candidate.pattern->is_op())
          << "Internal error: unexpected node type: "
          << ToDebugString(candidate.pattern);
      // both must be operation
      if (!candidate.candidate->is_op()) {
        return root;
      }
      if (candidate.pattern->op()->display_name() !=
          candidate.candidate->op()->display_name()) {
        return root;
      }
      ASSIGN_OR_RETURN(auto decayed_op,
                       DecayRegisteredOperator(candidate.candidate->op()));
      if (!HasBackendExprOperatorTag(decayed_op) &&
          !HasBuiltinExprOperatorTag(decayed_op)) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "tried applying a peephole optimization to operator %s."
            " which is neither backend nor builtin. Is "
            "your peephole optimization correct?",
            decayed_op->display_name()));
      }

      const auto& opt_deps = candidate.pattern->node_deps();
      const auto& root_deps = candidate.candidate->node_deps();
      // number of dependencies must match
      if (opt_deps.size() != root_deps.size()) {
        return root;
      }
      // Trying to add children to the queue.
      for (int64_t dep_id = 0; dep_id != root_deps.size(); ++dep_id) {
        if (!add_to_queue({.candidate = root_deps[dep_id],
                           .pattern = opt_deps[dep_id]})) {
          return root;
        }
      }
    }
    return DecayReferencesToRegisteredOperator(data_.to_visitor_order,
                                               placeholder_subs);
  }

 private:
  PatternOptimizationData data_;
};

// Optimiziation that applies specific transformation function.
class TransformOptimization : public PeepholeOptimization {
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

absl::StatusOr<ExprOperatorSignature>
ReferenceToRegisteredOperator::GetSignature() const {
  return ExprOperatorSignature::MakeVariadicArgs();
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
    tpe_ = Type::kOperator;
    // We use only operator names for initial filtration, but later do accurate
    // matching via OperatorMatches().
    fingerprint_ =
        FingerprintHasher("").Combine(expr->op()->display_name()).Finish();
  } else if (expr->is_literal()) {
    tpe_ = Type::kLiteral;
    fingerprint_ = expr->qvalue()->GetFingerprint();
  } else {
    tpe_ = Type::kOther;
    fingerprint_ = expr->fingerprint();
  }
}

bool PeepholeOptimization::PatternKey::operator==(
    const PatternKey& other) const {
  return tpe_ == other.tpe_ && fingerprint_ == other.fingerprint_;
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
  PatternKey key(from);
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
  PeepholeOptimization::PatternKey key(node);
  if (auto it = pattern_optimizations.find(key);
      it != pattern_optimizations.end()) {
    for (const auto& optimization : it->second) {
      ASSIGN_OR_RETURN(node, optimization->ApplyToRoot(node));
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
    std::optional<PeepholeOptimization::PatternKey> key = opt->GetKey();
    if (key.has_value()) {
      auto& opt_list = data->pattern_optimizations[*key];
      opt_list.push_back(std::move(opt));
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
