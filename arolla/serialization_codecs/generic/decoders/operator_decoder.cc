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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operator_loader/backend_operator.h"
#include "arolla/expr/operator_loader/dispatch_operator.h"
#include "arolla/expr/operator_loader/dummy_operator.h"
#include "arolla/expr/operator_loader/generic_operator.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/operator_loader/restricted_lambda_operator.h"
#include "arolla/expr/operators/while_loop/while_loop.h"
#include "arolla/expr/overloaded_expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/operator_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetNthOperator;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::MakeTupleOperator;
using ::arolla::expr::OverloadedOperator;
using ::arolla::expr::RegisteredOperator;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<TypedValue> DecodeLambdaOperator(
    const OperatorV1Proto::LambdaOperatorProto& lambda_operator_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!lambda_operator_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing lambda_operator.name; value=LAMBDA_OPERATOR");
  }
  if (!lambda_operator_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        "missing lambda_operator.signature_spec; value=LAMBDA_OPERATOR");
  }
  if (input_exprs.empty()) {
    return absl::InvalidArgumentError(
        "missing input_expr_index for lambda body; value=LAMBDA_OPERATOR");
  }
  ASSIGN_OR_RETURN(auto signature,
                   ExprOperatorSignature::Make(
                       lambda_operator_proto.signature_spec(), input_values),
                   _ << "value=LAMBDA_OPERATOR");
  ASSIGN_OR_RETURN(
      auto op,
      LambdaOperator::Make(lambda_operator_proto.name(), signature,
                           input_exprs[0], lambda_operator_proto.doc()),
      _ << "value=LAMBDA_OPERATOR");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<TypedValue> DecodeGetNthOperator(int64_t index) {
  ASSIGN_OR_RETURN(auto op, GetNthOperator::Make(index),
                   _ << "value=GET_NTH_OPERATOR_INDEX");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<TypedValue> DecodeOverloadedOperator(
    absl::string_view overloaded_operator_name,
    absl::Span<const TypedValue> input_values) {
  std::vector<ExprOperatorPtr> base_ops;
  base_ops.reserve(input_values.size());
  for (const auto& input_value : input_values) {
    if (input_value.GetType() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected %s, got a %s value as an input; "
          "value=OVERLOADED_OPERATOR",
          GetQType<ExprOperatorPtr>()->name(), input_value.GetType()->name()));
    }
    base_ops.push_back(input_value.UnsafeAs<ExprOperatorPtr>());
  }
  return TypedValue::FromValue<ExprOperatorPtr>(
      std::make_shared<OverloadedOperator>(overloaded_operator_name,
                                           std::move(base_ops)));
}

absl::StatusOr<TypedValue> DecodeWhileLoopOperator(
    const OperatorV1Proto::WhileLoopOperatorProto& while_loop_operator_proto,
    absl::Span<const TypedValue> input_values) {
  std::vector<ExprOperatorPtr> base_ops;
  if (!while_loop_operator_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing while_loop_operator.name; value=WHILE_LOOP_OPERATOR");
  }
  if (input_values.size() != 2) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 2 input values, got %d; value=WHILE_LOOP_OPERATOR",
        input_values.size()));
  }
  for (size_t i = 0; i < input_values.size(); ++i) {
    if (input_values[i].GetType() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected %s in input_values[%d], got %s; value=WHILE_LOOP_OPERATOR",
          GetQType<ExprOperatorPtr>()->name(), i,
          input_values[0].GetType()->name()));
    }
  }
  const auto& loop_condition = input_values[0].UnsafeAs<ExprOperatorPtr>();
  const auto& loop_body = input_values[1].UnsafeAs<ExprOperatorPtr>();
  if (!while_loop_operator_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        "missing while_loop_operator.signature_spec; "
        "value=WHILE_LOOP_OPERATOR");
  }
  ASSIGN_OR_RETURN(auto signature,
                   ExprOperatorSignature::Make(
                       while_loop_operator_proto.signature_spec(), {}),
                   _ << "value=WHILE_LOOP_OPERATOR");
  ASSIGN_OR_RETURN(auto op,
                   expr_operators::WhileLoopOperator::Make(
                       while_loop_operator_proto.name(), std::move(signature),
                       std::move(loop_condition), std::move(loop_body)),
                   _ << "value=WHILE_LOOP_OPERATOR");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<TypedValue> DecodeBackendOperator(
    const OperatorV1Proto::BackendOperatorProto& backend_operator_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!backend_operator_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing backend_operator.name; "
        "value=BACKEND_OPERATOR");
  }
  if (!backend_operator_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        "missing backend_operator.signature_spec; "
        "value=BACKEND_OPERATOR");
  }
  if (input_exprs.size() !=
      backend_operator_proto.qtype_constraint_error_messages_size() + 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected %d input_expr_index, got %d; "
        "value=BACKEND_OPERATOR",
        backend_operator_proto.qtype_constraint_error_messages_size() + 1,
        input_exprs.size()));
  }
  ASSIGN_OR_RETURN(auto signature,
                   ExprOperatorSignature::Make(
                       backend_operator_proto.signature_spec(), input_values),
                   _ << "value=BACKEND_OPERATOR");
  std::vector<arolla::operator_loader::QTypeConstraint> qtype_constraints;
  qtype_constraints.reserve(
      backend_operator_proto.qtype_constraint_error_messages_size());
  for (int i = 0;
       i < backend_operator_proto.qtype_constraint_error_messages_size(); ++i) {
    qtype_constraints.push_back(
        {input_exprs[i],
         backend_operator_proto.qtype_constraint_error_messages(i)});
  }
  ASSIGN_OR_RETURN(
      auto op,
      arolla::operator_loader::BackendOperator::Make(
          backend_operator_proto.name(), std::move(signature),
          backend_operator_proto.doc(), std::move(qtype_constraints),
          input_exprs[input_exprs.size() - 1]),
      _ << "value=BACKEND_OPERATOR");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<TypedValue> DecodeRestrictedLambdaOperator(
    const OperatorV1Proto::RestrictedLambdaOperatorProto&
        restricted_lambda_operator_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (input_values.empty()) {
    return absl::InvalidArgumentError(
        "missing input_value_index for base lambda "
        "operator; "
        "value=RESTRICTED_LAMBDA_OPERATOR");
  }
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected 1 input_value_index, got %d; "
                        "value=RESTRICTED_LAMBDA_OPERATOR",
                        input_values.size()));
  }
  if (input_values[0].GetType() != GetQType<ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s as input value, got %s; "
                        "value=RESTRICTED_LAMBDA_OPERATOR",
                        GetQType<ExprOperatorPtr>()->name(),
                        input_values[0].GetType()->name()));
  }
  const auto& op = input_values[0].UnsafeAs<ExprOperatorPtr>();
  auto base_lambda_op = std::dynamic_pointer_cast<const LambdaOperator>(op);
  if (base_lambda_op == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected lambda operator as input value, got %s; "
                        "value=RESTRICTED_LAMBDA_OPERATOR",
                        Repr(op)));
  }
  if (input_exprs.size() !=
      restricted_lambda_operator_proto.qtype_constraint_error_messages_size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected %d input_expr_index, got %d; "
        "value=RESTRICTED_LAMBDA_OPERATOR",
        restricted_lambda_operator_proto.qtype_constraint_error_messages_size(),
        input_exprs.size()));
  }
  std::vector<arolla::operator_loader::QTypeConstraint> qtype_constraints;
  const auto& qtype_constraint_error_messages =
      restricted_lambda_operator_proto.qtype_constraint_error_messages();
  qtype_constraints.reserve(qtype_constraint_error_messages.size());
  for (int i = 0; i < qtype_constraint_error_messages.size(); ++i) {
    qtype_constraints.push_back(
        {input_exprs[i], qtype_constraint_error_messages[i]});
  }
  ASSIGN_OR_RETURN(auto result,
                   arolla::operator_loader::RestrictedLambdaOperator::Make(
                       std::move(base_lambda_op), std::move(qtype_constraints)),
                   _ << "value=RESTRICTED_LAMBDA_OPERATOR");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(result));
}

absl::StatusOr<TypedValue> DecodeDispatchOperator(
    const OperatorV1Proto::DispatchOperatorProto& dispatch_operator_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!dispatch_operator_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing dispatch_operator.name; value=DISPATCH_OPERATOR");
  }
  if (!dispatch_operator_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        "missing dispatch_operator.signature_spec; value=DISPATCH_OPERATOR");
  }
  ASSIGN_OR_RETURN(auto signature,
                   ExprOperatorSignature::Make(
                       dispatch_operator_proto.signature_spec(), {}));
  if (input_values.empty()) {
    return absl::InvalidArgumentError(
        "missing overloads; value=DISPATCH_OPERATOR");
  }
  if (input_values.size() != dispatch_operator_proto.overload_names_size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected input_values.size() == "
        "dispatch_operator_proto.overload_names_size(), got %d and %d; "
        "value=DISPATCH_OPERATOR",
        input_values.size(), dispatch_operator_proto.overload_names_size()));
  }
  if (input_values.size() + 1 != input_exprs.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected input_values.size() + 1 == input_exprs.size()"
                        ", got %d and %d; value=DISPATCH_OPERATOR",
                        input_values.size(), input_exprs.size()));
  }
  for (size_t i = 0; i < input_values.size(); ++i) {
    if (input_values[i].GetType() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected %s as %d-th input value, got %s; "
                          "value=DISPATCH_OPERATOR",
                          GetQType<ExprOperatorPtr>()->name(), i,
                          input_values[i].GetType()->name()));
    }
  }
  std::vector<operator_loader::DispatchOperator::Overload> overloads(
      input_exprs.size() - 1);
  for (size_t i = 0; i < input_exprs.size() - 1; ++i) {
    overloads[i] = {.name = dispatch_operator_proto.overload_names(i),
                    .op = input_values[i].UnsafeAs<expr::ExprOperatorPtr>(),
                    .condition = input_exprs[i]};
  }
  auto dispatch_readiness_condition = input_exprs.back();
  ASSIGN_OR_RETURN(auto result,
                   arolla::operator_loader::DispatchOperator::Make(
                       dispatch_operator_proto.name(), signature, overloads,
                       dispatch_readiness_condition),
                   _ << "; value = DISPATCH_OPERATOR");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(result));
}

absl::StatusOr<TypedValue> DecodeDummyOperator(
    const OperatorV1Proto::DummyOperatorProto& dummy_operator_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!dummy_operator_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing dummy_operator.name; "
        "value=DUMMY_OPERATOR");
  }
  if (!dummy_operator_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        "missing dummy_operator.signature_spec; "
        "value=DUMMY_OPERATOR");
  }
  if (input_values.empty()) {
    return absl::InvalidArgumentError(
        "expected at least one input_value_index, got 0; "
        "value=DUMMY_OPERATOR");
  }
  if (input_values.back().GetType() != GetQTypeQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected the last input_value_index to be a "
                        "QType, "
                        "got %s; value=DUMMY_OPERATOR",
                        input_values.back().GetType()->name()));
  }
  ASSIGN_OR_RETURN(
      auto signature,
      ExprOperatorSignature::Make(dummy_operator_proto.signature_spec(),
                                  input_values.first(input_values.size() - 1)),
      _ << "value=DUMMY_OPERATOR");
  auto op = std::make_shared<arolla::operator_loader::DummyOperator>(
      dummy_operator_proto.name(), std::move(signature),
      dummy_operator_proto.doc(), input_values.back().UnsafeAs<QTypePtr>());
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<TypedValue> DecodeGenericOperator(
    const OperatorV1Proto::GenericOperatorProto& generic_operator_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!generic_operator_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing generic_operator.name; value=GENERIC_OPERATOR");
  }
  if (!generic_operator_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        "missing generic_operator.signature_spec; value=GENERIC_OPERATOR");
  }
  ASSIGN_OR_RETURN(auto signature,
                   ExprOperatorSignature::Make(
                       generic_operator_proto.signature_spec(), input_values),
                   _ << "value=GENERIC_OPERATOR");
  ASSIGN_OR_RETURN(auto op,
                   arolla::operator_loader::GenericOperator::Make(
                       generic_operator_proto.name(), signature,
                       generic_operator_proto.doc()),
                   _ << "value=GENERIC_OPERATOR");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<TypedValue> DecodeGenericOperatorOverload(
    const OperatorV1Proto::GenericOperatorOverloadProto&,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 input value, got %d; value=GENERIC_OPERATOR_OVERLOAD",
        input_values.size()));
  }
  if (input_values[0].GetType() != GetQType<ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s as input value, got %s; "
                        "value=GENERIC_OPERATOR_OVERLOAD",
                        GetQType<ExprOperatorPtr>()->name(),
                        input_values[0].GetType()->name()));
  }
  const auto& basic_op = input_values[0].UnsafeAs<ExprOperatorPtr>();
  if (input_exprs.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 input expr, got %d; value=GENERIC_OPERATOR_OVERLOAD",
        input_exprs.size()));
  }
  const auto& prepared_overload_condition_expr = input_exprs[0];
  ASSIGN_OR_RETURN(auto op,
                   arolla::operator_loader::GenericOperatorOverload::Make(
                       basic_op, prepared_overload_condition_expr),
                   _ << "value=GENERIC_OPERATOR_OVERLOAD");
  return TypedValue::FromValue<ExprOperatorPtr>(std::move(op));
}

absl::StatusOr<ValueDecoderResult> DecodeOperator(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(OperatorV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& operator_proto =
      value_proto.GetExtension(OperatorV1Proto::extension);
  switch (operator_proto.value_case()) {
    case OperatorV1Proto::kRegisteredOperatorName:
      return TypedValue::FromValue<ExprOperatorPtr>(
          std::make_shared<RegisteredOperator>(
              operator_proto.registered_operator_name()));
    case OperatorV1Proto::kLambdaOperator:
      return DecodeLambdaOperator(operator_proto.lambda_operator(),
                                  input_values, input_exprs);
    case OperatorV1Proto::kMakeTupleOperator:
      return TypedValue::FromValue<ExprOperatorPtr>(MakeTupleOperator::Make());
    case OperatorV1Proto::kGetNthOperatorIndex:
      return DecodeGetNthOperator(operator_proto.get_nth_operator_index());

    case OperatorV1Proto::kOverloadedOperatorName:
      return DecodeOverloadedOperator(operator_proto.overloaded_operator_name(),
                                      input_values);
    case OperatorV1Proto::kWhileLoopOperator:
      return DecodeWhileLoopOperator(operator_proto.while_loop_operator(),
                                     input_values);
    case OperatorV1Proto::kBackendOperator:
      return DecodeBackendOperator(operator_proto.backend_operator(),
                                   input_values, input_exprs);
    case OperatorV1Proto::kRestrictedLambdaOperator:
      return DecodeRestrictedLambdaOperator(
          operator_proto.restricted_lambda_operator(), input_values,
          input_exprs);
    case OperatorV1Proto::kDummyOperator:
      return DecodeDummyOperator(operator_proto.dummy_operator(), input_values,
                                 input_exprs);
    case OperatorV1Proto::kDispatchOperator:
      return DecodeDispatchOperator(operator_proto.dispatch_operator(),
                                    input_values, input_exprs);
    case OperatorV1Proto::kGenericOperator:
      return DecodeGenericOperator(operator_proto.generic_operator(),
                                   input_values, input_exprs);
    case OperatorV1Proto::kGenericOperatorOverload:
      return DecodeGenericOperatorOverload(
          operator_proto.generic_operator_overload(), input_values,
          input_exprs);
    case OperatorV1Proto::kOperatorQtype:
      return TypedValue::FromValue(GetQType<ExprOperatorPtr>());
    case OperatorV1Proto::VALUE_NOT_SET: {
      return absl::InvalidArgumentError("missing value");
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(operator_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kOperatorV1Codec, DecodeOperator);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
