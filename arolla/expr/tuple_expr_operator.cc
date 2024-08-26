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
#include "arolla/expr/tuple_expr_operator.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

ExprOperatorPtr MakeTupleOperator::Make() {
  static const absl::NoDestructor<ExprOperatorPtr> result(
      std::make_shared<MakeTupleOperator>());
  return *result;
}

MakeTupleOperator::MakeTupleOperator()
    : ExprOperatorWithFixedSignature(
          "core.make_tuple", ExprOperatorSignature::MakeVariadicArgs(),
          "Returns a tuple constructed from the given arguments.",
          FingerprintHasher("::arolla::expr::MakeTupleOperator").Finish()) {}

ExprAttributes MakeTupleOperator::StaticInferAttributes(
    absl::Span<const ExprAttributes> inputs) {
  if (!HasAllAttrQTypes(inputs)) {
    return ExprAttributes{};
  }
  return ExprAttributes(MakeTupleQType(GetAttrQTypes(inputs)));
}

absl::StatusOr<ExprAttributes> MakeTupleOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  return StaticInferAttributes(inputs);
}

absl::StatusOr<ExprOperatorPtr> GetNthOperator::Make(int64_t index) {
  if (index < 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a non-negative index, got %d", index));
  }
  return std::make_shared<GetNthOperator>(index);
}

namespace {

std::string GetNthOperatorDocstring(int64_t index) {
  if (index == 0) {
    return "Returns the first field of a compound value.";
  } else if (index == 1) {
    return "Returns the second field of a compound value.";
  } else if (index == 2) {
    return "Returns the third field of a compound value.";
  } else {
    return absl::StrFormat("Returns the %dth field of a compound value.",
                           index + 1);
  }
}

}  // namespace

GetNthOperator::GetNthOperator(int64_t index)
    : ExprOperatorWithFixedSignature(
          absl::StrFormat("get_nth[%d]", index),
          ExprOperatorSignature{{"value"}}, GetNthOperatorDocstring(index),
          FingerprintHasher("::arolla::expr::GetNthOperator")
              .Combine(index)
              .Finish()),
      index_(index) {}

absl::StatusOr<ExprAttributes> GetNthOperator::StaticInferAttributes(
    int64_t index, const ExprAttributes& input) {
  if (!input.qtype()) {
    return ExprAttributes{};
  }
  const auto& fields = input.qtype()->type_fields();
  if (fields.empty() && !IsTupleQType(input.qtype())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a compound type, got value: %s", input.qtype()->name()));
  }
  if (index < 0 || static_cast<size_t>(index) >= fields.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("index out of range: n=%d, value.field_count=%d", index,
                        fields.size()));
  }
  if (!input.qvalue()) {
    return ExprAttributes(fields[index].GetType());
  }
  return ExprAttributes(input.qvalue()->GetField(index));
}

absl::StatusOr<ExprAttributes> GetNthOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  return StaticInferAttributes(index_, inputs[0]);
}

absl::string_view GetNthOperator::py_qvalue_specialization_key() const {
  return "::arolla::expr::GetNthOperator";
}

}  // namespace arolla::expr
