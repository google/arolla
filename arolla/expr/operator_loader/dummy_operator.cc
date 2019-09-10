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
#include "arolla/expr/operator_loader/dummy_operator.h"

#include <utility>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;

DummyOperator::DummyOperator(absl::string_view name,
                             ExprOperatorSignature signature,
                             absl::string_view doc, QTypePtr result_qtype)
    : ExprOperatorWithFixedSignature(
          name, signature, doc,
          FingerprintHasher("::arolla::operator_loader::DummyOperator")
              // Note that the fingerprint depends on the `result_qtype`.
              .Combine(name, signature, doc, result_qtype)
              .Finish()),
      result_qtype_(std::move(result_qtype)) {}

absl::string_view DummyOperator::py_qvalue_specialization_key() const {
  return "::arolla::operator_loader::DummyOperator";
}

absl::StatusOr<ExprAttributes> DummyOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  return ExprAttributes(result_qtype_);
}

}  // namespace arolla::operator_loader
