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
#ifndef AROLLA_DECISION_FOREST_QEXPR_OPERATOR_POINTWISE_OPERATOR_H_
#define AROLLA_DECISION_FOREST_QEXPR_OPERATOR_POINTWISE_OPERATOR_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/decision_forest/decision_forest.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Creates an operator to evaluate given decision forest.
// 'op_type' is a signature of the operator to create.
// All inputs should be OptionalValue<...>.
// Output is a tuple of non-optional floats.
// The groups argument specifies which trees should be used for each output.
// The number of groups should be equal to the size of the output tuple.
absl::StatusOr<OperatorPtr> CreatePointwiseDecisionForestOperator(
    const DecisionForestPtr& decision_forest,
    const QExprOperatorSignature* op_type, absl::Span<const TreeFilter> groups);

absl::Status ValidatePointwiseDecisionForestOutputType(const QTypePtr output,
                                                       int group_count);

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_QEXPR_OPERATOR_POINTWISE_OPERATOR_H_
