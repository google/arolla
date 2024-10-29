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
#ifndef AROLLA_EXPR_EXPR_OPERATOR_SIGNATURE_H_
#define AROLLA_EXPR_EXPR_OPERATOR_SIGNATURE_H_

#include <cstddef>
#include <initializer_list>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node_ptr.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

// Representation of the expression operators's calling signature (inspired by
// PEP 362).
struct ExprOperatorSignature {
  struct Parameter {
    enum class Kind {
      kPositionalOrKeyword,
      kVariadicPositional,
    };
    std::string name;  // Must be a valid python identifier.
    std::optional<TypedValue> default_value;
    Kind kind = {Kind::kPositionalOrKeyword};
  };

  // List of expression operator parameters.
  //
  // The trailing operator's parameters can have associated default values,
  // which makes them optional. The last operator's parameter can be variadic.
  std::vector<Parameter> parameters;

  // Auxiliary policy.
  //
  // NOTE: This policy only affects auxiliary functions with "aux" in their name
  // (by convention). The standard functions, such as arolla::BindArguments()
  // and arolla::BindOp(), should ignore it.
  std::string aux_policy;

  // Default constructor.
  ExprOperatorSignature() = default;

  // Convenience constructor.
  ExprOperatorSignature(std::initializer_list<Parameter> parameters)
      : parameters(parameters) {}

  // Convenience constructor.
  ExprOperatorSignature(std::initializer_list<Parameter> parameters,
                        std::string aux_policy)
      : parameters(parameters), aux_policy(std::move(aux_policy)) {}

  // Makes a simple signature: arg1, arg2, ..., argn
  static ExprOperatorSignature MakeArgsN(size_t n);

  // Makes a simple variadic positional signature: *args
  static ExprOperatorSignature MakeVariadicArgs();

  // Makes a signature from a string definition and a list of default values.
  // This function automatically validates the resulting signature.
  //
  // Example:
  //   Make("x, y=, z=, *w", default_value_for_y, default_value_for_z)
  // =>
  //   ExprOperatorSignature{
  //     Parameter{"x", nullptr, kPositionalOnly},
  //     Parameter{"y", default_value_for_y, kPositionalOnly},
  //     Parameter{"z", default_value_for_z, kPositionalOnly},
  //     Parameter{"w", nullptr, kVariadicPositional}
  //   }
  static absl::StatusOr<ExprOperatorSignature> Make(
      absl::string_view signature_spec,
      absl::Span<const TypedValue> default_values);

  // Syntactic sugar for the previous factory function.
  template <typename... DefaultValues>
  static auto Make(absl::string_view signature_spec,
                   DefaultValues&&... default_values)
      -> std::enable_if_t<!(std::is_convertible_v<
                                DefaultValues, absl::Span<const TypedValue>> &&
                            ... && (sizeof...(DefaultValues) == 1)),
                          absl::StatusOr<ExprOperatorSignature>> {
    // unused with empty DefaultValues.
    constexpr auto wrap_arg ABSL_ATTRIBUTE_UNUSED =
        [](const auto& arg) -> TypedValue {
      if constexpr (std::is_same_v<decltype(arg), const TypedValue&>) {
        return arg;
      } else {
        return TypedValue::FromValue(arg);
      }
    };
    return ExprOperatorSignature::Make(
        signature_spec,
        std::initializer_list<TypedValue>{wrap_arg(default_values)...});
  }
};

// Checks consistency of the parameters.
absl::Status ValidateSignature(const ExprOperatorSignature& signature);

// Checks that deps_count is a valid number of dependencies to pass to an
// operator with the signature. Assumes that args have been already bound and
// dependencies include defaults.
absl::Status ValidateDepsCount(const ExprOperatorSignature& signature,
                               size_t deps_count, absl::StatusCode error_code);

// Tests whether there are any variadic parameters, i.e. whether the operator's
// argument list is unbounded.
//
// Pre-condition: The function expects a valid signature as input.
bool HasVariadicParameter(const ExprOperatorSignature& signature);

// Returns arguments bound to parameters.
//
// This function checks that the provided arguments are compatible with
// the signature, and handles the parameters' default values.
// The resulting sequence (of bound arguments) is aligned with the parameters.
//
// Pre-condition: The function expects a valid signature as input.
absl::StatusOr<std::vector<ExprNodePtr>> BindArguments(
    const ExprOperatorSignature& signature, absl::Span<const ExprNodePtr> args,
    const absl::flat_hash_map<std::string, ExprNodePtr>& kwargs);

// Returns string spec of the signature.
std::string GetExprOperatorSignatureSpec(
    const ExprOperatorSignature& signature);

}  // namespace arolla::expr

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(arolla::expr::ExprOperatorSignature);

}  // namespace arolla

#endif  // AROLLA_EXPR_EXPR_OPERATOR_SIGNATURE_H_
