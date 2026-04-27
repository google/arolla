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
#ifndef AROLLA_EXPR_EXPR_OPERATOR_H_
#define AROLLA_EXPR_EXPR_OPERATOR_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
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

// Tags for the expression operator.
//
// NOTE: Tags function as bitmasks; e.g., the kAnnotation tag implies kBuiltin.
enum class ExprOperatorTags : uint8_t {
  kNone = 0b00000000,

  // A tag indicating that the operator is directly supported by the compiler or
  // a compiler extension.
  //
  // The name of a built-in operator may not uniquely identify it. These
  // operators should be identified by their fingerprint or C++ class.
  //
  // Examples:
  //   * core.get_nth[n]
  //   * derived_qtype_upcasting[T]
  //   * derived_qtype_downcasting[T]
  kBuiltin = 0b00000001,

  // A tag indicating an Annotation operator.
  //
  // Annotation operators provide metadata about the wrapped node. Any
  // annotation operator must take the wrapped node as its first input; it
  // may have any number of additional inputs containing annotation contents.
  //
  // Annotations are expected to be side-effect free and must not affect
  // the evaluation result. Any annotation can be safely ignored during
  // evaluation if the compiler does not support it.
  kAnnotation = 0b00000011,

  // A tag indicating that the operator is provided by a backend.
  //
  // The operator name and input qtypes must uniquely identify the computation.
  // The "state" of the expression operator is irrelevant for identification.
  //
  // NOTE: Backend operators may have a non-trivial ToLowerLevel() method,
  // though using it is generally not recommended.
  //
  // If custom logic is required for ToLowerLevel(), consider creating a
  // derived operator that implements that logic and lowers to a backend
  // operator with a trivial ToLowerLevel() implementation.
  //
  // Examples:
  //   * core.map
  //   * math.add
  kBackend = 0b00000100,
};

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

  // Returns operator's tags.
  ExprOperatorTags tags() const { return tags_; }

  // Returns operator's signature.
  virtual absl::StatusOr<ExprOperatorSignaturePtr absl_nonnull> GetSignature()
      const = 0;

  // Returns operator's doc-string.
  virtual absl::StatusOr<std::string> GetDoc() const = 0;

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
  //
  // NOTE: Default implementation that only checks the number of dependencies.
  virtual absl::StatusOr<ExprNodePtr absl_nonnull> ToLowerLevel(
      const ExprNodePtr absl_nonnull& node) const;

  // Returns the "official" string representation of the operator.
  virtual ReprToken GenReprToken() const;

  // Used in python to choose a specialized wrapper for the operator.
  // Empty string means that there is no specialized wrapper.
  virtual absl::string_view py_qvalue_specialization_key() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND;

 protected:
  ExprOperator(absl::string_view display_name, Fingerprint fingerprint,
               ExprOperatorTags tags = ExprOperatorTags::kNone)
      : display_name_(display_name), fingerprint_(fingerprint), tags_(tags) {}

  // Validates the number of input dependencies for the operator node.
  // An incorrect number of dependencies indicates a broken expression DAG;
  // reports the error as a FailedPreconditionError.
  //
  // ValidateNodeDepsCount() is intended for use in the ToLowerLevel() method.
  absl::Status ValidateNodeDepsCount(const ExprNode& expr) const;

  // Validates the number of inputs provided to the operator.
  // An incompatible number of inputs is reported as an InvalidArgumentError;
  // this does not necessarily imply that the expression DAG is broken; e.g.,
  // OverloadedOperator handles this error by attempting the next candidate
  // operator.
  //
  // ValidateOpInputsCount() is intended for use in InferAttributes() and
  // GetOutputQType() methods.
  absl::Status ValidateOpInputsCount(
      absl::Span<const ExprAttributes> inputs) const;

 private:
  std::string display_name_;
  Fingerprint fingerprint_;
  ExprOperatorTags tags_;
};

// ExprOperator pointer.
using ExprOperatorPtr = std::shared_ptr<const ExprOperator>;

inline bool HasExprOperatorTag(const ExprOperatorPtr absl_nullable& op,
                               ExprOperatorTags tag) {
  return op != nullptr &&
         (static_cast<uint8_t>(op->tags()) & static_cast<uint8_t>(tag)) ==
             static_cast<uint8_t>(tag);
}

// Returns true iff `op` has a backend operator tag.
inline bool HasBackendExprOperatorTag(const ExprOperatorPtr absl_nullable& op) {
  return HasExprOperatorTag(op, ExprOperatorTags::kBackend);
}

// Returns true iff `op` has a builtin operator tag.
inline bool HasBuiltinExprOperatorTag(const ExprOperatorPtr absl_nullable& op) {
  return HasExprOperatorTag(op, ExprOperatorTags::kBuiltin);
}

// Returns true iff `op` has an annotation operator tag.
inline bool HasAnnotationExprOperatorTag(  // clang-format hint
    const ExprOperatorPtr absl_nullable& op) {
  return HasExprOperatorTag(op, ExprOperatorTags::kAnnotation);
}

// Returns true iff `op` is a backend operator with the given name.
inline bool IsBackendOperator(const ExprOperatorPtr absl_nullable& op,
                              absl::string_view name) {
  return HasBackendExprOperatorTag(op) && op->display_name() == name;
}

}  // namespace arolla::expr

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(expr::ExprOperatorPtr);
AROLLA_DECLARE_REPR(expr::ExprOperatorPtr);
AROLLA_DECLARE_QTYPE(expr::ExprOperatorPtr);

}  // namespace arolla

#endif  // AROLLA_EXPR_EXPR_OPERATOR_H_
