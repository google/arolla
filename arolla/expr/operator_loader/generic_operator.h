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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_H_
#define AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/generic_operator_overload_condition.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/thread_safe_shared_ptr.h"

namespace arolla::operator_loader {

// Name of the input leaf for the prepared conditions.
inline constexpr absl::string_view
    kGenericOperatorPreparedOverloadConditionLeafKey = "input_tuple_qtype";

// A generic operator.
//
// A generic operator works as a frontend to a namespace with overloads stored
// in the operator registry. The overloads have associated overload conditions
// (that must be mutually exclusive) based on which the overload selection
// happens.
class GenericOperator final
    : public ::arolla::expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  // Factory function.
  static absl::StatusOr<std::shared_ptr<GenericOperator>> Make(
      absl::string_view name, ::arolla::expr::ExprOperatorSignature signature,
      absl::string_view doc);

  // Private constructor.
  GenericOperator(PrivateConstructorTag, absl::string_view name,
                  ::arolla::expr::ExprOperatorSignature signature,
                  absl::string_view doc);

  // Returns the namespace for overloads.
  absl::string_view namespace_for_overloads() const { return display_name(); }

  absl::StatusOr<::arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const ::arolla::expr::ExprAttributes> inputs) const final;

  absl::StatusOr<::arolla::expr::ExprNodePtr> ToLowerLevel(
      const ::arolla::expr::ExprNodePtr& node) const final;

  absl::string_view py_qvalue_specialization_key() const final;

 private:
  struct SnapshotOfOverloads {
    int64_t revision_id;
    std::vector<::arolla::expr::RegisteredOperatorPtr> overloads;
    GenericOperatorOverloadConditionFn overload_condition_fn;
  };
  using SnapshotOfOverloadsPtr = std::shared_ptr<SnapshotOfOverloads>;

  // Builds a new snapshot of the overloads.
  absl::StatusOr<SnapshotOfOverloadsPtr> BuildSnapshot() const;

  // Returns a snapshot of the overloads.
  absl::StatusOr<SnapshotOfOverloadsPtr> GetSnapshot() const;

  // Returns an overload corresponding to the given inputs; returns nullptr if
  // the selection is inconclusive.
  absl::StatusOr<::arolla::expr::ExprOperatorPtr /*nullable*/> GetOverload(
      absl::Span<const ::arolla::expr::ExprAttributes> inputs) const;

  ::arolla::expr::ExprOperatorRegistry::RevisionIdFn revision_id_fn_;
  mutable ThreadSafeSharedPtr<SnapshotOfOverloads> snapshot_of_overloads_;
};

// An overload for a generic operator.
class GenericOperatorOverload final : public ::arolla::expr::ExprOperator {
  struct PrivateConstructorTag {};

 public:
  // Factory function.
  //
  // The `prepared_overload_condition_expr` should contain no placeholders and
  // should only use the L.input_tuple_qtype input. For more information, see
  // the function: _prepare_overload_condition_expr(signature, condition_expr).
  static absl::StatusOr<std::shared_ptr<GenericOperatorOverload>> Make(
      ::arolla::expr::ExprOperatorPtr base_operator,
      ::arolla::expr::ExprNodePtr prepared_overload_condition_expr);

  // Private constructor.
  GenericOperatorOverload(
      PrivateConstructorTag, ::arolla::expr::ExprOperatorPtr base_operator,
      ::arolla::expr::ExprNodePtr prepared_overload_condition_expr);

  // Returns a prepared overload condition.
  const ::arolla::expr::ExprNodePtr& prepared_overload_condition_expr() const {
    return prepared_overload_condition_expr_;
  }

  // Returns base operator.
  const ::arolla::expr::ExprOperatorPtr& base_operator() const {
    return base_operator_;
  }

  absl::StatusOr<::arolla::expr::ExprOperatorSignature> GetSignature()
      const final {
    return base_operator_->GetSignature();
  }

  absl::StatusOr<std::string> GetDoc() const final {
    return base_operator_->GetDoc();
  }

  absl::StatusOr<::arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const ::arolla::expr::ExprAttributes> inputs) const final {
    return base_operator_->InferAttributes(inputs);
  }

  absl::StatusOr<::arolla::expr::ExprNodePtr> ToLowerLevel(
      const ::arolla::expr::ExprNodePtr& node) const final;

  absl::string_view py_qvalue_specialization_key() const final;

 private:
  ::arolla::expr::ExprOperatorPtr base_operator_;
  ::arolla::expr::ExprNodePtr prepared_overload_condition_expr_;
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_H_
