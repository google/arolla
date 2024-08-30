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
#ifndef AROLLA_QEXPR_OPERATOR_ERRORS_H_
#define AROLLA_QEXPR_OPERATOR_ERRORS_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {

// Returns error signalizing that operator is not implemented
// for specific combination of argument types.
absl::Status OperatorNotDefinedError(absl::string_view operator_name,
                                     absl::Span<const QTypePtr> input_types,
                                     absl::string_view extra_message = "");

// Verifies that slot types are same as expected.
//
// `operator_name` parameter are used only to construct error message. The
// difference between Input and Output versions is also only in error messages.
absl::Status VerifyInputSlotTypes(absl::Span<const TypedSlot> slots,
                                  absl::Span<const QTypePtr> expected_types);
absl::Status VerifyOutputSlotType(TypedSlot slot, QTypePtr expected_type);

// Verifies that input/output value types are same as expected.
//
// `operator_name` parameter are used only to construct error message. The
// difference between Input and Output versions is also only in error messages.
absl::Status VerifyInputValueTypes(absl::Span<const TypedValue> values,
                                   absl::Span<const QTypePtr> expected_types);
absl::Status VerifyInputValueTypes(absl::Span<const TypedRef> values,
                                   absl::Span<const QTypePtr> expected_types);
absl::Status VerifyOutputValueType(const TypedValue& value,
                                   QTypePtr expected_type);

// Guesses possible build target that contains all the operators from the
// given operator's namespace.
std::string GuessLibraryName(absl::string_view operator_name);

// Guesses possible build target that contains all the instances of the given
// operator.
std::string GuessOperatorLibraryName(absl::string_view operator_name);

// Returns a suggestion how to fix the missing backend operator issue.
std::string SuggestMissingDependency();

// Returns a suggestion of available overloads.
std::string SuggestAvailableOverloads(
    absl::string_view operator_name,
    absl::Span<const QExprOperatorSignature* const> supported_qtypes);

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATOR_ERRORS_H_
