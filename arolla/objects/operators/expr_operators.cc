// Copyright 2025 Google LLC
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
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/objects/object_qtype.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {

using ::arolla::expr::BackendExprOperatorTag;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorWithFixedSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeLambdaOperator;

class ObjectGetObjectAttrOperator final
    : public BackendExprOperatorTag,
      public ExprOperatorWithFixedSignature {
 public:
  ObjectGetObjectAttrOperator()
      : ExprOperatorWithFixedSignature(
            "objects.get_object_attr",
            ExprOperatorSignature{{"object"}, {"attr"}, {"output_qtype"}},
            "Returns the value at `attr` with the provided `output_qtype`.",
            FingerprintHasher(
                "::arolla::expr_operators::ObjectGetObjectAttrOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const override {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& object = inputs[0];
    const auto& attr = inputs[1];
    const auto& output_qtype = inputs[2];
    if (object.qtype() && object.qtype() != GetQType<Object>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected %s, got object: %s",
                          GetQType<Object>()->name(), object.qtype()->name()));
    }
    if (attr.qtype() && attr.qtype() != GetQType<Text>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a text scalar, got attr: %s", attr.qtype()->name()));
    }
    if (attr.qtype() && !attr.qvalue()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected `attr` to be a literal"));
    }
    // Not ready yet.
    if (!output_qtype.qtype()) {
      return ExprAttributes{};
    }
    if (output_qtype.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a qtype, got output_qtype: %s",
                          output_qtype.qtype()->name()));
    }
    if (!output_qtype.qvalue()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected `output_qtype` to be a literal"));
    }
    QTypePtr qtype = output_qtype.qvalue()->UnsafeAs<QTypePtr>();
    // Early eval when possible.
    if (object.qvalue() && attr.qvalue() && output_qtype.qvalue()) {
      ASSIGN_OR_RETURN(auto output,
                       InvokeOperator("objects.get_object_attr",
                                      {*object.qvalue(), *attr.qvalue(),
                                       *output_qtype.qvalue()},
                                      qtype));
      return ExprAttributes{std::move(output)};
    }
    // Otherwise, return the output qtype.
    return ExprAttributes{qtype};
  }
};

class ObjectGetObjectAttrQTypeOperator final
    : public BackendExprOperatorTag,
      public ExprOperatorWithFixedSignature {
 public:
  ObjectGetObjectAttrQTypeOperator()
      : ExprOperatorWithFixedSignature(
            "objects.get_object_attr_qtype",
            ExprOperatorSignature{{"object"}, {"attr"}},
            "Returns the QType at `attr` or NOTHING if the attr doesn't exist.",
            FingerprintHasher(
                "::arolla::expr_operators::ObjectGetObjectAttrQTypeOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const override {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& object = inputs[0];
    const auto& attr = inputs[1];
    if (object.qtype() && object.qtype() != GetQType<Object>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected %s, got object: %s",
                          GetQType<Object>()->name(), object.qtype()->name()));
    }
    if (attr.qtype() && attr.qtype() != GetQType<Text>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a text scalar, got attr: %s", attr.qtype()->name()));
    }
    // Early eval when possible.
    if (object.qvalue() && attr.qvalue()) {
      ASSIGN_OR_RETURN(
          auto output,
          InvokeOperator("objects.get_object_attr_qtype",
                         {*object.qvalue(), *attr.qvalue()}, GetQTypeQType()));
      return ExprAttributes{GetQTypeQType(), std::move(output)};
    }
    // Otherwise, return the expected qtype.
    return ExprAttributes{GetQTypeQType()};
  }
};

AROLLA_INITIALIZER(
        .reverse_deps = {"arolla_operators/objects"},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(expr::RegisterOperator<ObjectGetObjectAttrOperator>(
                              "objects.get_object_attr")
                              .status());
          RETURN_IF_ERROR(
              expr::RegisterOperator<ObjectGetObjectAttrQTypeOperator>(
                  "objects.get_object_attr_qtype")
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator("objects.make_object_qtype",
                               MakeLambdaOperator(ExprOperatorSignature{},
                                                  Literal(GetQType<Object>())))
                  .status());
          return absl::OkStatus();
        })

}  // namespace arolla::expr_operators
