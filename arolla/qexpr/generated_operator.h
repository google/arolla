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
#ifndef AROLLA_QEXPR_GENERATED_OPERATOR_H_
#define AROLLA_QEXPR_GENERATED_OPERATOR_H_

#include <memory>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::qexpr_impl {

using BoundOperatorFactory = std::unique_ptr<::arolla::BoundOperator> (*)(
    absl::Span<const TypedSlot>, TypedSlot);

// Registers GeneratedOperator with the given name, qtype's and bound operator
// factory's.
// Sizes of the `qtypes` and `factories` must be the same.
// This function is needed to encapsulate shared_ptr<Operator> avoiding its
// instantiations in each code-generated *.o file.
absl::Status RegisterGeneratedOperators(
    absl::string_view name,
    absl::Span<const QExprOperatorSignature* const> signatures,
    absl::Span<const BoundOperatorFactory> factories,
    bool is_individual_operator);

}  // namespace arolla::qexpr_impl

#endif  // AROLLA_QEXPR_GENERATED_OPERATOR_H_
