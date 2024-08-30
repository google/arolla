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
#include "arolla/optools/optools.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::optools::optools_impl {

namespace {

class QExprWrappingOperator final : public expr::BackendExprOperatorTag,
                                    public expr::BasicExprOperator {
 public:
  QExprWrappingOperator(absl::string_view name,
                        std::vector<OperatorPtr> qexpr_ops,
                        expr::ExprOperatorSignature signature,
                        absl::string_view description)
      : expr::BasicExprOperator(
            name, signature, description,
            FingerprintHasher("arolla::optools_impl::QExprWrappingOperator")
                .Combine(name, signature)
                .Finish()),
        qexpr_ops_(std::move(qexpr_ops)) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const override {
    for (const OperatorPtr& op : qexpr_ops_) {
      absl::Span<const QTypePtr> required_qtypes =
          op->signature()->input_types();
      bool match = true;
      for (size_t i = 0; i < input_qtypes.size(); ++i) {
        if (input_qtypes[i] != required_qtypes[i]) {
          match = false;
          break;
        }
      }
      if (match) {
        return op->signature()->output_type();
      }
    }

    std::string msg = "no such overload; available signatures: ";
    bool is_first = true;
    for (const auto& op : qexpr_ops_) {
      absl::StrAppend(&msg, op->signature(), NonFirstComma(is_first, ", "));
    }
    return absl::InvalidArgumentError(msg);
  }

 private:
  std::vector<OperatorPtr> qexpr_ops_;
};

}  // namespace

absl::Status RegisterFunctionAsOperatorImpl(
    absl::string_view name, std::vector<OperatorPtr> qexpr_ops,
    expr::ExprOperatorSignature signature, absl::string_view description) {
  RETURN_IF_ERROR(expr::ValidateSignature(signature));
  if (expr::HasVariadicParameter(signature)) {
    return absl::InvalidArgumentError(
        "incorrect operator signature: RegisterFunctionAsOperator doesn't "
        "support variadic args");
  }
  if (qexpr_ops.empty()) {
    return absl::InvalidArgumentError(
        "at least one qexpr operator is required");
  }
  size_t arg_count = qexpr_ops[0]->signature()->input_types().size();
  for (const OperatorPtr& op : qexpr_ops) {
    if (op->signature()->input_types().size() != arg_count) {
      return absl::InvalidArgumentError(
          "arg count must be the same for all overloads");
    }
    RETURN_IF_ERROR(
        ::arolla::OperatorRegistry::GetInstance()->RegisterOperator(name, op));
  }
  if (signature.parameters.empty()) {
    signature = expr::ExprOperatorSignature::MakeArgsN(arg_count);
  } else if (signature.parameters.size() != arg_count) {
    return absl::InvalidArgumentError(
        "operator signature doesn't match the function");
  }
  return expr::RegisterOperator<QExprWrappingOperator>(
             name, name, std::move(qexpr_ops), signature, description)
      .status();
}

}  // namespace arolla::optools::optools_impl
