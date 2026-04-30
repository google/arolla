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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_QTYPE_INFERENCE_H_
#define AROLLA_EXPR_OPERATOR_LOADER_QTYPE_INFERENCE_H_

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/qtype/qtype.h"
#include "arolla/sequence/sequence.h"

namespace arolla::operator_loader {

// Function for a qtype inference from the given input qtypes.
//
// Returns nullptr if some required arguments are missing, but none of
// the present arguments violate the qtype constraints.
using QTypeInferenceFn =
    absl::AnyInvocable<absl::StatusOr<QTypePtr absl_nullable>(
        const Sequence& input_qtype_sequence) const>;

// Compiles the given constraints and qtype inference expression.
absl::StatusOr<QTypeInferenceFn absl_nonnull> MakeQTypeInferenceFn(
    const arolla::expr::ExprOperatorSignature& expr_operator_signature,
    absl::Span<const QTypeConstraint> qtype_constraints,
    const arolla::expr::ExprNodePtr absl_nonnull& qtype_inference_expr);

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_QTYPE_INFERENCE_H_
