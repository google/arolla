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
#ifndef AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZER_H_
#define AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZER_H_

#include <cstddef>
#include <functional>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"

namespace arolla::expr {

// A "placeholder" for ExprOperator: just a name, no ToLower and
// InferAttributes. The goal for such placeholders is to avoid a dependency on
// real operators when they are not needed.
class ReferenceToRegisteredOperator final : public ExprOperator {
 public:
  explicit ReferenceToRegisteredOperator(absl::string_view name);

  absl::StatusOr<std::string> GetDoc() const final;
  absl::StatusOr<ExprOperatorSignaturePtr absl_nonnull> GetSignature()
      const final;
  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;
};

// Like CallOp, but calls ReferenceToRegisteredOperator instead of the real one.
absl::StatusOr<ExprNodePtr> CallOpReference(
    absl::string_view op_name,
    std::initializer_list<absl::StatusOr<ExprNodePtr>> status_or_args);

// Replaces all `ReferenceToRegisteredOperator` instances with
// the corresponding `RegisteredOperator` in `expr`.
//
// NOTE: Uses a low-level API that does not require the operator to be
// present in the registry.
absl::StatusOr<ExprNodePtr> ResolveReferenceToRegisteredOperators(
    const ExprNodePtr absl_nonnull& expr);

// Single optimization, which able to convert one set of instructions
// to another. Generally can be used for any type of EXPRession transformations.
class PeepholeOptimization {
 public:
  // Key that can be used to split `PeepholeOptimization`s into groups for
  // quick search of applicable optimizations.
  // Only `optimization` holds following condition
  // `PeepholeOptimization::PatternKey(root) == optimization.key()` can be
  // applicable to the `root`.
  // In other words: if condition is false `ApplyToRoot(root)`
  // will return `root` unchanged.
  // PatternKey is equal for the nodes that have similar root structure, e.g.,
  // the same operator or the same literal.
  class PatternKey {
   public:
    explicit PatternKey(const ExprNodePtr& expr);
    bool operator==(const PatternKey& other) const;
    bool operator!=(const PatternKey& other) const;

    template <typename H>
    friend H AbslHashValue(H h, const PatternKey& key) {
      return H::combine(std::move(h), key.hash_);
    }

   private:
    size_t hash_;
  };

  using NodeMatcher = std::function<bool(const ExprNode&)>;


  // Returns PatternKey to filter nodes optimization applied for.
  // If nullopt, ApplyToRoot will be called for each node.
  virtual std::optional<PatternKey> GetKey() const { return std::nullopt; }

  // Try to apply optimization to the root of expression.
  // Returns root unchanged if not applicable.
  virtual absl::StatusOr<ExprNodePtr> ApplyToRoot(
      const ExprNodePtr& root) const = 0;

  virtual ~PeepholeOptimization() = default;
};

// Set of peephole optimizations.
using PeepholeOptimizationPack =
    std::vector<std::unique_ptr<PeepholeOptimization>>;

// Applies set of optimizations to the entire EXPRession.
class PeepholeOptimizer {
 public:
  // Applies optimizations to the entire EXPRession.
  // Optimizations are applied in post order traverse DFS order.
  // One or several optimizations can be applied to each node, but at the
  // end more optimizations can be applied. It can make sense to call `Apply`
  // several times while expression keep changing.
  absl::StatusOr<ExprNodePtr> Apply(ExprNodePtr root) const;

  absl::StatusOr<ExprNodePtr> ApplyToNode(ExprNodePtr node) const;

  static absl::StatusOr<std::unique_ptr<PeepholeOptimizer>> Create(
      PeepholeOptimizationPack optimizations);

  ~PeepholeOptimizer();

 private:
  struct Data;
  explicit PeepholeOptimizer(std::unique_ptr<Data> data);

  std::unique_ptr<Data> data_;
};

// A factory constructing peephole optimization pack.
using PeepholeOptimizationPackFactory =
    std::function<absl::StatusOr<PeepholeOptimizationPack>()>;

// Convenience wrapper for PeepholeOptimizer::Create. Allows to pass
// optimizations in packs, also forwards StatusOr from the arguments in case of
// errors.
absl::StatusOr<std::unique_ptr<PeepholeOptimizer>> CreatePeepholeOptimizer(
    absl::Span<const PeepholeOptimizationPackFactory>
        optimization_pack_factories);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZER_H_
