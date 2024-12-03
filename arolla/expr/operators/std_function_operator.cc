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
#include "arolla/expr/operators/std_function_operator.h"

#include <utility>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;

StdFunctionOperator::StdFunctionOperator(absl::string_view name,
                                         ExprOperatorSignature signature,
                                         absl::string_view doc,
                                         OutputQTypeFn output_qtype_fn,
                                         EvalFn eval_fn)
    : ExprOperatorWithFixedSignature(name, signature, doc, RandomFingerprint()),
      output_qtype_fn_(std::move(output_qtype_fn)),
      eval_fn_(std::move(eval_fn)) {}

absl::StatusOr<ExprAttributes> StdFunctionOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  ASSIGN_OR_RETURN(auto* output_qtype, output_qtype_fn_(GetAttrQTypes(inputs)));
  if (output_qtype == nullptr && expr::HasAllAttrQTypes(inputs)) {
    return absl::InternalError(
        "unexpected missing output qtype when all inputs are present");
  }
  return ExprAttributes(output_qtype);
}

const StdFunctionOperator::OutputQTypeFn&
StdFunctionOperator::GetOutputQTypeFn() const {
  return output_qtype_fn_;
}

const StdFunctionOperator::EvalFn& StdFunctionOperator::GetEvalFn() const {
  return eval_fn_;
}

}  // namespace arolla::expr_operators
