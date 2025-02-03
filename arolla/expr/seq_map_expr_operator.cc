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
#include "arolla/expr/seq_map_expr_operator.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

const ExprOperatorPtr& SeqMapOperator::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<SeqMapOperator>());
  return *result;
}

SeqMapOperator::SeqMapOperator()
    : ExprOperatorWithFixedSignature(
          "seq.map",
          ExprOperatorSignature{
              {"op"},
              {"arg0"},
              {"args", std::nullopt,
               ExprOperatorSignature::Parameter::Kind::kVariadicPositional}},
          "Applies an operator to sequences of elements.",
          FingerprintHasher("arolla::expr::SeqMapOperator").Finish()) {}

absl::StatusOr<ExprAttributes> SeqMapOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  const auto& op = inputs[0];
  if (op.qtype() && op.qtype() != GetQType<ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected an operator, got op: %s; in seq.map operator",
                        op.qtype()->name()));
  }
  if (op.qtype() && !op.qvalue()) {
    return absl::InvalidArgumentError("`op` must be a literal");
  }
  if (!HasAllAttrQTypes(inputs.subspan(1, inputs.size() - 1))) {
    return ExprAttributes();
  }
  std::vector<ExprAttributes> args_expr_attributes(inputs.size() - 1);
  for (size_t i = 1; i < inputs.size(); ++i) {
    if (!IsSequenceQType(inputs[i].qtype())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected argument %d to be a sequence, got %s; in seq.map operator",
          (i + 1), inputs[i].qtype()->name()));
    }
    args_expr_attributes[i - 1] =
        ExprAttributes(inputs[i].qtype()->value_qtype());
  }
  if (!inputs[0].qtype()) {
    return ExprAttributes();
  }
  const auto& oper = op.qvalue()->UnsafeAs<ExprOperatorPtr>();
  std::vector<absl::StatusOr<ExprNodePtr>> args;
  args.reserve(args_expr_attributes.size());
  for (size_t i = 0; i < args_expr_attributes.size(); ++i) {
    args.push_back(
        CallOp(QTypeAnnotation::Make(),
               {Placeholder("x"), Literal(args_expr_attributes[i].qtype())}));
  }
  ASSIGN_OR_RETURN(auto output_expr, CallOp(oper, std::move(args)));
  auto* output_value_qtype = output_expr->qtype();
  return ExprAttributes(GetSequenceQType(output_value_qtype));
}

}  // namespace arolla::expr
