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
#include "arolla/expr/backend_wrapping_operator.h"

#include <utility>

#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

BackendWrappingOperator::BackendWrappingOperator(
    absl::string_view name, const ExprOperatorSignature& signature,
    TypeMetaEvalStrategy strategy, absl::string_view doc)
    : BasicExprOperator(
          name, signature, doc,
          FingerprintHasher("arolla::expr::BackendWrappingOperator")
              .Combine(name, signature)
              .Finish()),
      type_meta_eval_strategy_(std::move(strategy)) {}

absl::StatusOr<QTypePtr> BackendWrappingOperator::GetOutputQType(
    absl::Span<const QTypePtr> input_qtypes) const {
  return type_meta_eval_strategy_(input_qtypes);
}

absl::StatusOr<ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view name,
    BackendWrappingOperator::TypeMetaEvalStrategy strategy,
    absl::string_view doc) {
  return RegisterOperator<BackendWrappingOperator>(
      name, name, ExprOperatorSignature::MakeVariadicArgs(),
      std::move(strategy), doc);
}

absl::StatusOr<ExprOperatorPtr> RegisterBackendOperator(
    absl::string_view name, const ExprOperatorSignature& signature,
    BackendWrappingOperator::TypeMetaEvalStrategy strategy,
    absl::string_view doc) {
  return RegisterOperator<BackendWrappingOperator>(name, name, signature,
                                                   std::move(strategy), doc);
}

}  // namespace arolla::expr
