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
#include "arolla/expr/operators/map_operator.h"

#include <memory>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;

// Returns the first array type from the given list. If no array found it
// returns nullptr (aka "not yet known") if one of the inputs is nullptr, or an
// error otherwise.
absl::StatusOr<const ArrayLikeShapeQType*> DeduceResultingArrayShape(
    absl::Span<const ExprAttributes> inputs) {
  const ArrayLikeQType* found_array_qtype = nullptr;
  for (const auto& input : inputs) {
    if (!IsArrayLikeQType(input.qtype())) {
      continue;
    }
    const ArrayLikeQType* array_qtype =
        static_cast<const ArrayLikeQType*>(input.qtype());
    if (found_array_qtype != nullptr) {
      if (array_qtype->shape_qtype() != found_array_qtype->shape_qtype()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "all array arguments must have compatible shapes, got %s and %s",
            array_qtype->name(), found_array_qtype->name()));
      }
    } else {
      found_array_qtype = array_qtype;
    }
  }
  if (found_array_qtype == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("at least one array required, got %s",
                        FormatTypeVector(GetAttrQTypes(inputs))));
  }
  return found_array_qtype->shape_qtype();
}

}  // namespace

MapOperator::MapOperator()
    : expr::ExprOperatorWithFixedSignature(
          "core.map",
          expr::ExprOperatorSignature{
              {"op"},
              {"first_arg"},  // core.map requires op to accept at least one
                              // argument.
              {.name = "rest_args",
               .kind = ExprOperatorSignature::Parameter::Kind::
                   kVariadicPositional}},
          "Applies an operator pointwise to the *args.\n"
          "\n"
          "Only literal ops are allowed. There has to be at least one array\n"
          "in *args and all the arrays must be of the same kind and of the\n"
          "same shape. Scalars in *args are broadcasted to match this shape.",
          FingerprintHasher("::arolla::expr_operators::MapOperator").Finish()) {
}

absl::StatusOr<ExprAttributes> MapOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  if (!HasAllAttrQTypes(inputs)) {
    return ExprAttributes{};
  }
  if (inputs[0].qtype() != GetQType<ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected an EXPR_OPERATOR, got op: %s", inputs[0].qtype()->name()));
  }
  if (!inputs[0].qvalue().has_value()) {
    return absl::InvalidArgumentError("op must be a literal");
  }
  ASSIGN_OR_RETURN(const ExprOperatorPtr& op,
                   inputs[0].qvalue()->As<ExprOperatorPtr>());
  ASSIGN_OR_RETURN(auto* result_shape_qtype,
                   DeduceResultingArrayShape(inputs.subspan(1)));

  std::vector<ExprAttributes> op_inputs;
  op_inputs.reserve(inputs.size() - 1);
  for (const auto& input : inputs.subspan(1)) {
    // Only broadcasted literal inputs can be propagated to the op, so filter
    // out the arrays.
    if (IsArrayLikeQType(input.qtype())) {
      ASSIGN_OR_RETURN(auto op_input_type,
                       ToOptionalQType(input.qtype()->value_qtype()));
      op_inputs.emplace_back(op_input_type);
    } else {
      op_inputs.emplace_back(input);
    }
  }
  ASSIGN_OR_RETURN(auto op_output, op->InferAttributes(op_inputs),
                   _ << "while deducing output type for " << op->display_name()
                     << " in core.map operator");
  if (op_output.qtype() == nullptr) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(auto result_type,
                   result_shape_qtype->WithValueQType(op_output.qtype()));
  return ExprAttributes{result_type};
}

}  // namespace arolla::expr_operators
