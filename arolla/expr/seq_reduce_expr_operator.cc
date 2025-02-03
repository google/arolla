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
#include "arolla/expr/seq_reduce_expr_operator.h"

#include <memory>
#include <optional>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

const ExprOperatorPtr& SeqReduceOperator::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<SeqReduceOperator>());
  return *result;
}

SeqReduceOperator::SeqReduceOperator()
    : ExprOperatorWithFixedSignature(
          "seq.reduce", ExprOperatorSignature{{"op"}, {"seq"}, {"initial"}},
          "Cumulatively applies a binary operator to sequence elements.",
          FingerprintHasher("arolla::expr::SeqReduceOperator").Finish()) {}

absl::StatusOr<ExprAttributes> SeqReduceOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  const auto& op = inputs[0];
  if (op.qtype() && op.qtype() != GetQType<ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected an operator, got op: %s", op.qtype()->name()));
  }
  const auto& seq = inputs[1];
  if (seq.qtype() && !IsSequenceQType(seq.qtype())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a sequence type, got seq: %s", seq.qtype()->name()));
  }
  const auto& initial = inputs[2];
  if (!op.qtype() || !seq.qtype() || !initial.qtype()) {
    return ExprAttributes(initial.qtype());
  }
  if (!op.qvalue()) {
    return absl::InvalidArgumentError("`op` must be a literal");
  }
  const auto& oper = op.qvalue()->UnsafeAs<ExprOperatorPtr>();
  ASSIGN_OR_RETURN(auto oper_sig, oper->GetSignature());
  if (!ValidateDepsCount(oper_sig, 2, absl::StatusCode::kInvalidArgument)
           .ok()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a binary operator, got %s", Repr(oper)));
  }
  const auto* value_qtype = seq.qtype()->value_qtype();
  DCHECK(value_qtype != nullptr);
  ASSIGN_OR_RETURN(auto output_attr,
                   oper->InferAttributes({ExprAttributes(initial.qtype()),
                                          ExprAttributes(value_qtype)}));
  auto* output_qtype = output_attr.qtype();
  if (output_qtype != initial.qtype()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected an operator that takes (%s, %s) and returns %s, got %s",
        initial.qtype()->name(), value_qtype->name(), initial.qtype()->name(),
        Repr(oper)));
  }
  return ExprAttributes(output_qtype);
}

}  // namespace arolla::expr
