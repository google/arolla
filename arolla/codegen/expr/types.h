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
#ifndef AROLLA_CODEGEN_EXPR_TYPES_H_
#define AROLLA_CODEGEN_EXPR_TYPES_H_

#include <functional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::codegen {

// Registers C++ type to be used for codegeneration.
// Extension point for CppTypeName and CppLiteralRepr.
// The call is overriding behavior for built-in types.
// It is required if not supported by default type is used as Literal or as
// input/output.
// Args:
//   cpp_type_name: fully qualified C++ type name
//   cpp_literal_repr: C++ code for Literal construction.
// Return error if called twice for the same type.
absl::Status RegisterCppType(
    QTypePtr qtype, absl::string_view cpp_type_name,
    std::function<absl::StatusOr<std::string>(TypedRef)> cpp_literal_repr);

// Returns C++ type name, which could be used in the generated code.
// E.g., "float", "int", "::arolla::OptionalValue<int>"
absl::StatusOr<std::string> CppTypeName(QTypePtr qtype);

// Returns C++ type construction to create QType.
// E.g., "::arolla::GetQType<int>()",
// "::arolla::MakeTupleQType({::arolla::GetQType<int>(),
//                             ::arolla::GetQType<float>()})"
absl::StatusOr<std::string> CppQTypeConstruction(QTypePtr qtype);

// Returns C++ literal representation, which can be used as rvalue to assign
// to variable with `const auto&` type.
// E.g., 1.0f, static_cast<float>(1) or lambda
// []() -> float { return 1; }()
// Different forms could be return depending on the value type.
absl::StatusOr<std::string> CppLiteralRepr(TypedRef value);

inline absl::StatusOr<std::string> CppLiteralRepr(TypedValue value) {
  return CppLiteralRepr(value.AsRef());
}

}  // namespace arolla::codegen

#endif  // AROLLA_CODEGEN_EXPR_TYPES_H_
