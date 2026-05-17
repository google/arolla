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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_DISPATCH_OPERATOR_H_
#define AROLLA_EXPR_OPERATOR_LOADER_DISPATCH_OPERATOR_H_

#include <memory>
#include <string>
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
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla::operator_loader {

// A dispatch operator.
//
// Dispatch operator is an adapter for a list of overloads.
// Dispatch operator has an explicit signature, list of overloads where each
// operator comes with the explicit qtype constraint and a default operator.
// For each set of inputs, dispatch operator would return:
//   - the only overload with passing constraint,
//   - default operator if none of the constraints passes,
//   - an Error if more than one constraint passes.
class DispatchOperator final
    : public arolla::expr::ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  struct Overload {
    std::string name;
    arolla::expr::ExprOperatorPtr absl_nonnull op;
    arolla::expr::ExprNodePtr absl_nonnull condition_expr;
  };

  // Factory function for a dispatch operator.
  static absl::StatusOr<std::shared_ptr<const DispatchOperator>> Make(
      absl::string_view name, arolla::expr::ExprOperatorSignature signature,
      absl::string_view doc, std::vector<Overload> overloads,
      arolla::expr::ExprOperatorPtr absl_nullable default_op = nullptr);

  absl::StatusOr<arolla::expr::ExprAttributes> InferAttributes(
      absl::Span<const arolla::expr::ExprAttributes> inputs) const final;

  absl::StatusOr<arolla::expr::ExprNodePtr absl_nonnull> ToLowerLevel(
      const arolla::expr::ExprNodePtr absl_nonnull& node) const final;

  ReprToken GenReprToken() const final;

  absl::string_view py_qvalue_specialization_key() const final;

  // Returns overloads
  absl::Span<const Overload> overloads() const;

  // Returns the default operator (or nullptr if there is no default operator).
  arolla::expr::ExprOperatorPtr absl_nullable default_op() const;

 public:
  // Returns a tuple with the readiness value followed by the overload
  // condition results.
  using DispatchFn = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
      const Sequence& input_qtype_sequence) const>;

  // Private constructor.
  DispatchOperator(PrivateConstructorTag, absl::string_view name,
                   arolla::expr::ExprOperatorSignature signature,
                   absl::string_view doc, std::vector<Overload> overloads,
                   arolla::expr::ExprOperatorPtr absl_nullable default_op,
                   DispatchFn absl_nonnull dispatch_fn,
                   Fingerprint fingerprint);

 private:
  // Returns a dispatching function for the given overloads.
  static absl::StatusOr<DispatchFn absl_nonnull> MakeDispatchFn(
      const arolla::expr::ExprOperatorSignature& signature,
      absl::Span<const Overload> overloads);

  // Returns the overload that fits the input qtypes; or nullptr it's not
  // sufficient information to determine the overload.
  absl::StatusOr<const Overload* absl_nullable> GetOverload(
      const Sequence& input_qtype_sequence) const;

  // List of all overloads, including the default operator if there is one.
  std::vector<Overload> all_overloads_;
  bool has_default_overload_;

  DispatchFn absl_nonnull dispatch_fn_;
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_DISPATCH_OPERATOR_H_
