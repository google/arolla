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
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

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

absl::StatusOr<ExprNodePtr> ResolveReferenceToRegisteredOperators(
    const ExprNodePtr absl_nonnull& expr) {
  return Transform(expr, [](ExprNodePtr node) {
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
  });
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
