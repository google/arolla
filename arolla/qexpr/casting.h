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
#ifndef AROLLA_QEXPR_CASTING_H_
#define AROLLA_QEXPR_CASTING_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Tries to find operator QType that is the closest to input_types and matches
// output_type. Returns an error in case of ambiguous options, or when nothing
// compatible is found.
absl::StatusOr<const QExprOperatorSignature*> FindMatchingSignature(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type,
    absl::Span<const QExprOperatorSignature* const> supported_signatures,
    absl::string_view op_name);

}  // namespace arolla

#endif  // AROLLA_QEXPR_CASTING_H_
