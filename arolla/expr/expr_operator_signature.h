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
#ifndef AROLLA_EXPR_EXPR_OPERATOR_SIGNATURE_H_
#define AROLLA_EXPR_EXPR_OPERATOR_SIGNATURE_H_

#include <cstddef>
#include <initializer_list>
#include <optional>
#include <string>
#include <type_traits>
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
  // The auxiliary policy allows for additional customization of the operator
  // in Python (and other interactive environments). This may include custom
  // rendering, boxing of non-Arolla values, and support for extended
  // parameter types (e.g., keyword-only parameters).
  //
  // Information within the auxiliary policy should only extend the signature.
  // Functions that do not support auxiliary policies (e.g.,
  // arolla::expr::BindArguments() and arolla::expr::BindOp()) should be able
  // to safely ignore any policy-specific information.
  //
  // Functions supporting auxiliary policies (which typically include "aux" in
  // their names) may use policy information to adjust their behaviour.
  // If such a function does not recognise a specific policy, it should
  // generally provide no functionality or fail with an error, rather than
  // falling back to a default behaviour.

  // Auxiliary policy name.
  std::string aux_policy_name;

  // Auxiliary policy options.
  std::string aux_policy_options;

  // Default constructor.
  ExprOperatorSignature() = default;

  // Convenience constructor.
  ExprOperatorSignature(std::initializer_list<Parameter> parameters);

  // Convenience constructor.
  ExprOperatorSignature(std::initializer_list<Parameter> parameters,
                        std::string aux_policy);

  // Makes a simple signature: arg1, arg2, ..., argn
  static ExprOperatorSignature MakeArgsN(size_t n);

  // Makes a simple variadic positional signature: *args
  static ExprOperatorSignature MakeVariadicArgs();

  // Makes a signature from a string definition and a list of default values.
  // This function automatically validates the resulting signature.
  //
  // Example:
  //   >>> Make("x, y=, z=, *w", default_value_for_y, default_value_for_z)
  //   ExprOperatorSignature{
  //       Parameter{"x", nullopt, kPositionalOrKeyword},
  //       Parameter{"y", default_value_for_y, kPositionalOrKeyword},
  //       Parameter{"z", default_value_for_z, kPositionalOrKeyword},
  //       Parameter{"w", nullopt, kVariadicPositional}
  //       aux_policy_name: "",
  //       aux_policy_options: "",
  //   }
  //
  //   >>> Make("x, y | policy:options")
  //   ExprOperatorSignature{
  //       Parameter{"x", nullopt, kPositionalOrKeyword},
  //       Parameter{"y", nullopt, kPositionalOrKeyword},
  //       aux_policy_name: "policy",
  //       aux_policy_options: "options",
  //   }
  //
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

// Validates that `deps_count` is the correct number of input dependencies for
// an operator with the given `signature`.
//
// Note: Input dependencies and arguments are distinct concepts. For example,
// an operator with the signature "x, y=" can accept one or two arguments
// (since the second parameter has a default value), but it always requires two
// inputs (one for each parameter).
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

// Returns a string with aux_policy: <aux_policy_name>[:<aux_policy_options>].
std::string GetAuxPolicy(const ExprOperatorSignature& signature);

// Returns a string with aux_policy: <aux_policy_name>[:<aux_policy_options>].
std::string GetAuxPolicy(absl::string_view aux_policy_name,
                         absl::string_view aux_policy_options);

// Returns the `aux_policy_name` form of the `aux_policy` string.
absl::string_view GetAuxPolicyName(absl::string_view aux_policy);

// Returns the `aux_policy_options` form of the `aux_policy` string.
absl::string_view GetAuxPolicyOptions(absl::string_view aux_policy);

}  // namespace arolla::expr

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(arolla::expr::ExprOperatorSignature);

}  // namespace arolla

#endif  // AROLLA_EXPR_EXPR_OPERATOR_SIGNATURE_H_
