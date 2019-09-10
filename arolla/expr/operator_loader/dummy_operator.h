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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_DUMMY_OPERATOR_H_
#define AROLLA_EXPR_OPERATOR_LOADER_DUMMY_OPERATOR_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"

namespace arolla::operator_loader {

// A dummy operator with a fixed result qtype, but dynamic inputs.
//
// Important properties:
//  * serializable.
//  * the fingerprint of the operator instance depends on the result qtype.
class DummyOperator final : public expr::ExprOperatorWithFixedSignature {
 public:
  // NOTE(felbro): Consider allowing a qtype_inference_expr instead of
  // result_qtype to make it more versatile.
  DummyOperator(absl::string_view name, expr::ExprOperatorSignature signature,
                absl::string_view doc, QTypePtr result_qtype);

  QTypePtr GetOutputQType() const { return result_qtype_; }

  absl::string_view py_qvalue_specialization_key() const override;

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const final;

 private:
  QTypePtr result_qtype_;
};

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_DUMMY_OPERATOR_H_
