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
#ifndef AROLLA_EXPR_EXPR_OPERATOR_H_
#define AROLLA_EXPR_EXPR_OPERATOR_H_

#include <memory>
#include <string>

#include "absl/base/attributes.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node_ptr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla::expr {

// Base class for expression operators.
class AROLLA_API ExprOperator {
 public:
  virtual ~ExprOperator() = default;

  // No copying.
  ExprOperator(const ExprOperator&) = delete;
  ExprOperator& operator=(const ExprOperator&) = delete;

  // Returns operator's human-readable name, used for debug strings, error
  // messages, etc. Not guaranteed to be unique.
  //
  // Name should not be used for operator comparison:
  // WRONG: op.display_name()=='math.add' ! name collision is possible !
  // CORRECT: op == M.add
  //
  // Name of a registered operator uniquely identifies it.
  absl::string_view display_name() const { return display_name_; }

  // Returns operator's fingerprint.
  const Fingerprint& fingerprint() const { return fingerprint_; }

  // Returns operator's signature.
  virtual absl::StatusOr<ExprOperatorSignature> GetSignature() const = 0;

  // Returns operator's doc-string.
  //
  // TODO: Make this method abstract.
  virtual absl::StatusOr<std::string> GetDoc() const;

  // Infers the output attributes for the given inputs.
  //
  // Contract:
  //  * If there is not enough information in `inputs` to infer the output
  //    qtype, which means that the result is inconclusive, the method should
  //    return an empty `ExprAttributes` instance.
  //  * An operator is allowed to return an inconclusive result only if one (or
  //    more) of the inputs has an unspecified qtype.
  virtual absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const = 0;

  // Given operator inputs, return an expression representing this operator's
  // translation to a lower level.
  virtual absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const;

  // Returns the "official" string representation of the operator.
  virtual ReprToken GenReprToken() const;

  // Used in python to choose a specialized wrapper for the operator.
  // Empty string means that there is no specialized wrapper.
  virtual absl::string_view py_qvalue_specialization_key() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

 protected:
  ExprOperator(absl::string_view display_name, Fingerprint fingerprint)
      : display_name_(display_name), fingerprint_(fingerprint) {}

 private:
  std::string display_name_;
  Fingerprint fingerprint_;
};

// ExprOperator pointer.
using ExprOperatorPtr = std::shared_ptr<const ExprOperator>;

// Tag for an operator that is expected to be present in backend.
//
// It's expected that a backend operator name and input qtypes uniquely
// identify what the operator computes, i.e. the "state" of the expr operator is
// irrelevant.
//
// Examples:
//   * core.map
//   * math.sum
//
// NOTE: BackendExprOperator may have a non-trivial ToLowerLevel() method,
// although it's not recommended to use it.
//
// If you need some custom logic custom ToLowerLevel(), please consider creating
// a custom derived operator that implement the logic and lowers to a backend
// operator with a trivial ToLowerLevel().
//
struct BackendExprOperatorTag {};

// Base class for operators directly supported by the evaluation backend.
//
// Examples:
//   * core.get_nth[n]
//   * derived_qtype_upcasting[T]`, `derived_qtype_downcasting[T]
//
// IMPORTANT: The name of a built-in operator may not uniquely identify it.
// Please identify such operators by fingerprint or by C++ class.
//
struct BuiltinExprOperatorTag {};

// Tag for an Annotation operator.
//
// Annotation operator provides additional information about the wrapped node.
// Any annotation operator should take wrapped node as the first input, and may
// have any number of additional inputs with annotation contents.
//
// It's expected that annotations don't affect the evaluation result. In
// particular, any annotation can be completely ignored during the evaluation,
// if, say, the evaluation backend doesn't know how to handle it.
//
struct AnnotationExprOperatorTag : BuiltinExprOperatorTag {};

// Returns true iff `op` has a backend operator tag.
inline bool HasBackendExprOperatorTag(const ExprOperatorPtr& op) {
  return dynamic_cast<const BackendExprOperatorTag*>(op.get()) != nullptr;
}

// Returns true iff `op` has a builtin operator tag.
inline bool HasBuiltinExprOperatorTag(const ExprOperatorPtr& op) {
  return dynamic_cast<const BuiltinExprOperatorTag*>(op.get()) != nullptr;
}

// Returns true iff `op` has an annotation operator tag.
inline bool HasAnnotationExprOperatorTag(const ExprOperatorPtr& op) {
  return dynamic_cast<const AnnotationExprOperatorTag*>(op.get()) != nullptr;
}

// Returns true iff `op` is a backend operator with the given name.
bool IsBackendOperator(const ExprOperatorPtr& /*nullable*/ op,
                       absl::string_view name);

}  // namespace arolla::expr

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(expr::ExprOperatorPtr);
AROLLA_DECLARE_REPR(expr::ExprOperatorPtr);
AROLLA_DECLARE_QTYPE(expr::ExprOperatorPtr);

}  // namespace arolla

#endif  // AROLLA_EXPR_EXPR_OPERATOR_H_
