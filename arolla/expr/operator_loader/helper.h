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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_HELPER_H_
#define AROLLA_EXPR_OPERATOR_LOADER_HELPER_H_

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/io/input_loader.h"
#include "arolla/qtype/qtype.h"
#include "arolla/sequence/sequence.h"

namespace arolla::operator_loader {

// Return type for ResolvePlaceholders().
struct ResolvePlaceholdersResult {
  // Expression with placeholders replaced with L.input_qtype_sequence.
  arolla::expr::ExprNodePtr absl_nonnull resolved_expr;
  // An expression that returns present if the inputs match the signature and
  // all inputs used within `resolved_expr` are known. This indicates that
  // the result of `resolved_expr` is reliable.
  arolla::expr::ExprNodePtr absl_nonnull readiness_expr;
};

// Returns an equivalent expression with placeholders replaced by getters
// derived from `L.input_qtype_sequence`.
//
// The `expr` can reference signature parameters using placeholders, or
// reference `L.input_qtype_sequence` directly for advanced use cases.
//
// If `expected_output_qtype` is provided, the function validates that the
// expression's inferred QType matches the expected output.
absl::StatusOr<ResolvePlaceholdersResult> ResolvePlaceholders(
    const arolla::expr::ExprOperatorSignature& signature,
    const arolla::expr::ExprNodePtr absl_nonnull& expr,
    QTypePtr absl_nullable expected_output_qtype = nullptr);

// Returns a sequence of input qtypes for the given inputs.
absl::StatusOr<Sequence> MakeInputQTypeSequence(
    absl::Span<const arolla::expr::ExprAttributes> inputs);

// Returns a sequence of input qtypes for the given inputs.
absl::StatusOr<Sequence> MakeInputQTypeSequence(
    absl::Span<const arolla::expr::ExprNodePtr> inputs);

// QType of the input_qtype_sequence.
QTypePtr absl_nonnull GetInputQTypeSequenceQType();

// Returns an InputLoader for L.input_qtype_sequence.
const InputLoader<const Sequence&>& GetInputQTypeSequenceLoader();

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_HELPER_H_
