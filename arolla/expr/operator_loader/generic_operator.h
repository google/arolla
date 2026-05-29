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
#include <vector>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/class_info.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/thread_safe_shared_ptr.h"

namespace arolla::operator_loader {

// Forward declaration.
class GenericOperatorOverload;
using GenericOperatorOverloadPtr =
    std::shared_ptr<const GenericOperatorOverload>;

// A generic operator.
//
// A generic operator works as a frontend to a namespace with overloads stored
// in the operator registry. The overloads have associated overload conditions
// (that must be mutually exclusive) based on which the overload selection
// happens.
class GenericOperator final
    : public arolla::expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  // Factory function for a generic operator.
  static absl::StatusOr<std::shared_ptr<GenericOperator>> Make(
      absl::string_view name, arolla::expr::ExprOperatorSignature signature,
      absl::string_view doc);

  // Private constructor.
  GenericOperator(PrivateConstructorTag, absl::string_view name,
                  arolla::expr::ExprOperatorSignature signature,
                  absl::string_view doc);

  // Returns the namespace for overloads.
  absl::string_view namespace_for_overloads() const { return display_name(); }

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;

  absl::StatusOr<arolla::expr::ExprNodePtr absl_nonnull> ToLowerLevel(
      const arolla::expr::ExprNodePtr absl_nonnull& node) const final;

  absl::string_view py_qvalue_specialization_key() const final;

 private:
  // Returns a tuple with the readiness value followed by the overload
  // condition results.
  using DispatchFn = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
      const Sequence& input_qtype_sequence) const>;

  struct SnapshotOfOverloads {
    int64_t revision_id;
    std::vector<GenericOperatorOverloadPtr absl_nonnull> overloads;
    DispatchFn absl_nonnull dispatch_fn;
  };
  using SnapshotOfOverloadsPtr = std::shared_ptr<SnapshotOfOverloads>;

  // Builds a new snapshot of the overloads.
  absl::StatusOr<SnapshotOfOverloadsPtr> BuildSnapshot() const;

  // Returns a snapshot of the overloads.
  absl::StatusOr<SnapshotOfOverloadsPtr> GetSnapshot() const;

  // Returns an overload corresponding to the given inputs; returns nullptr if
  // the selection is inconclusive.
  absl::StatusOr<GenericOperatorOverloadPtr absl_nullable> GetOverload(
      const Sequence& input_qtype_sequence) const;

  arolla::expr::ExprOperatorRegistry::RevisionIdFn revision_id_fn_;
  mutable ThreadSafeSharedPtr<SnapshotOfOverloads> snapshot_of_overloads_;

  AROLLA_DECLARE_SUBCLASS_INFO(GenericOperator, ExprOperator);
};

// An overload for a generic operator.
//
// NOTE: GenericOperatorOverload is not intended to be used directly, and
// does not implement `InferAttributes` or `ToLowerLevel` methods.
class GenericOperatorOverload final
    : public arolla::expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  // Factory function for a generic operator overload.
  //
  // NOTE: The overload's signature is fixed, while the base operator's
  // signature may change over time. While these signatures are typically
  // identical, this is not a strict requirement.
  static absl::StatusOr<std::shared_ptr<GenericOperatorOverload>> Make(
      absl::string_view name, arolla::expr::ExprOperatorSignature signature,
      arolla::expr::ExprNodePtr absl_nonnull condition_expr,
      arolla::expr::ExprOperatorPtr absl_nonnull base_operator);

  // Private constructor.
  GenericOperatorOverload(  // clang-format hint
      PrivateConstructorTag, absl::string_view name,
      arolla::expr::ExprOperatorSignature signature,
      arolla::expr::ExprNodePtr absl_nonnull condition_expr,
      arolla::expr::ExprOperatorPtr absl_nonnull base_operator,
      Fingerprint fingerprint,
      arolla::expr::ExprNodePtr absl_nonnull prepared_readiness_expr,
      arolla::expr::ExprNodePtr absl_nonnull prepared_condition_expr);

  // Returns the overload condition expression.
  const arolla::expr::ExprNodePtr absl_nonnull& condition_expr() const {
    return condition_expr_;
  }

  // Returns base operator.
  const arolla::expr::ExprOperatorPtr absl_nonnull& base_operator() const {
    return base_operator_;
  }

  // Returns a prepared readiness expression.
  const arolla::expr::ExprNodePtr absl_nonnull& prepared_readiness_expr()
      const {
    return prepared_readiness_expr_;
  }

  // Returns a prepared overload condition.
  const arolla::expr::ExprNodePtr absl_nonnull& prepared_condition_expr()
      const {
    return prepared_condition_expr_;
  }

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;

  absl::StatusOr<arolla::expr::ExprNodePtr absl_nonnull> ToLowerLevel(
      const arolla::expr::ExprNodePtr absl_nonnull& node) const final;

  absl::string_view py_qvalue_specialization_key() const final;

 private:
  arolla::expr::ExprNodePtr absl_nonnull condition_expr_;
  arolla::expr::ExprOperatorPtr absl_nonnull base_operator_;
  arolla::expr::ExprNodePtr absl_nonnull prepared_readiness_expr_;
  arolla::expr::ExprNodePtr absl_nonnull prepared_condition_expr_;

  AROLLA_DECLARE_SUBCLASS_INFO(GenericOperatorOverload, ExprOperator);
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_H_
