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
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
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
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/operator_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::GetNthOperator;
using ::arolla::expr::LambdaOperator;
using ::arolla::expr::MakeTupleOperator;
using ::arolla::expr::OverloadedOperator;
using ::arolla::expr::RegisteredOperator;
using ::arolla::operator_loader::BackendOperator;
using ::arolla::operator_loader::DispatchOperator;
using ::arolla::operator_loader::DummyOperator;
using ::arolla::operator_loader::GenericOperator;
using ::arolla::operator_loader::GenericOperatorOverload;
using ::arolla::operator_loader::RestrictedLambdaOperator;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kOperatorV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeRegisteredOperator(
    const RegisteredOperator& op, Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  const auto& name = op.display_name();
  value_proto.MutableExtension(OperatorV1Proto::extension)
      ->set_registered_operator_name(name.data(), name.size());
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeLambdaOperator(const LambdaOperator& op,
                                                Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* lambda_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_lambda_operator();
  const auto& name = op.display_name();
  lambda_operator_proto->set_name(name.data(), name.size());
  lambda_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  if (!op.doc().empty()) {
    lambda_operator_proto->set_doc(op.doc());
  }
  for (const auto& param : op.signature().parameters) {
    if (param.default_value.has_value()) {
      ASSIGN_OR_RETURN(auto value_index,
                       encoder.EncodeValue(*param.default_value));
      value_proto.add_input_value_indices(value_index);
    }
  }
  ASSIGN_OR_RETURN(auto expr_index, encoder.EncodeExpr(op.lambda_body()));
  value_proto.add_input_expr_indices(expr_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeMakeTupleOperator(const MakeTupleOperator&,
                                                   Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OperatorV1Proto::extension)
      ->set_make_tuple_operator(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeGetNthOperator(const GetNthOperator& op,
                                                Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OperatorV1Proto::extension)
      ->set_get_nth_operator_index(op.index());
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOverloadedOperator(
    const OverloadedOperator& op, Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  const auto& name = op.display_name();
  value_proto.MutableExtension(OperatorV1Proto::extension)
      ->set_overloaded_operator_name(name.data(), name.size());
  for (const auto& base_op : op.base_ops()) {
    ASSIGN_OR_RETURN(auto value_index,
                     encoder.EncodeValue(TypedValue::FromValue(base_op)));
    value_proto.add_input_value_indices(value_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeWhileLoopOperator(
    const expr_operators::WhileLoopOperator& op, Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* while_loop_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_while_loop_operator();
  while_loop_operator_proto->set_name(std::string(op.display_name()));
  while_loop_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  for (const auto& param : op.signature().parameters) {
    if (param.default_value.has_value()) {
      return absl::UnimplementedError(absl::StrFormat(
          "%s does not support default values in while_loop "
          "operator's signature, got \"%s\"",
          kOperatorV1Codec, while_loop_operator_proto->signature_spec()));
    }
  }
  ASSIGN_OR_RETURN(auto condition_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(op.condition())));
  value_proto.add_input_value_indices(condition_value_index);
  ASSIGN_OR_RETURN(auto body_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(op.body())));
  value_proto.add_input_value_indices(body_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeBackendOperator(const BackendOperator& op,
                                                 Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* backend_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_backend_operator();
  const auto& name = op.display_name();
  backend_operator_proto->set_name(name.data(), name.size());
  backend_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  if (!op.doc().empty()) {
    backend_operator_proto->set_doc(op.doc());
  }
  for (const auto& param : op.signature().parameters) {
    if (param.default_value.has_value()) {
      ASSIGN_OR_RETURN(auto value_index,
                       encoder.EncodeValue(*param.default_value));
      value_proto.add_input_value_indices(value_index);
    }
  }
  for (const auto& qtype_constraint : op.qtype_constraints()) {
    backend_operator_proto->add_qtype_constraint_error_messages(
        qtype_constraint.error_message);
    ASSIGN_OR_RETURN(auto expr_index,
                     encoder.EncodeExpr(qtype_constraint.predicate_expr));
    value_proto.add_input_expr_indices(expr_index);
  }
  ASSIGN_OR_RETURN(auto expr_index,
                   encoder.EncodeExpr(op.qtype_inference_expr()));
  value_proto.add_input_expr_indices(expr_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeRestrictedLambdaOperator(
    const RestrictedLambdaOperator& op, Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* restricted_lambda_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_restricted_lambda_operator();
  ASSIGN_OR_RETURN(auto value_index,
                   encoder.EncodeValue(TypedValue::FromValue<ExprOperatorPtr>(
                       op.base_lambda_operator())));
  value_proto.add_input_value_indices(value_index);
  for (const auto& qtype_constraint : op.qtype_constraints()) {
    restricted_lambda_operator_proto->add_qtype_constraint_error_messages(
        qtype_constraint.error_message);
    ASSIGN_OR_RETURN(auto expr_index,
                     encoder.EncodeExpr(qtype_constraint.predicate_expr));
    value_proto.add_input_expr_indices(expr_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDispatchOperator(const DispatchOperator& op,
                                                  Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* dispatch_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_dispatch_operator();
  dispatch_operator_proto->set_name(std::string(op.display_name()));
  dispatch_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  const auto& overloads = op.overloads();
  for (size_t i = 0; i < overloads.size(); ++i) {
    dispatch_operator_proto->add_overload_names(overloads[i].name);
    ASSIGN_OR_RETURN(auto value_index,
                     encoder.EncodeValue(TypedValue::FromValue<ExprOperatorPtr>(
                         overloads[i].op)));
    value_proto.add_input_value_indices(value_index);
    ASSIGN_OR_RETURN(auto expr_index,
                     encoder.EncodeExpr(overloads[i].condition));
    value_proto.add_input_expr_indices(expr_index);
  }
  ASSIGN_OR_RETURN(auto readiness_expr_index,
                   encoder.EncodeExpr(op.dispatch_readiness_condition()));
  value_proto.add_input_expr_indices(readiness_expr_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDummyOperator(const DummyOperator& op,
                                               Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* dummy_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_dummy_operator();
  const auto& name = op.display_name();
  dummy_operator_proto->set_name(name.data(), name.size());
  dummy_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  if (!op.doc().empty()) {
    dummy_operator_proto->set_doc(op.doc());
  }
  for (const auto& param : op.signature().parameters) {
    if (param.default_value.has_value()) {
      ASSIGN_OR_RETURN(auto value_index,
                       encoder.EncodeValue(*param.default_value));
      value_proto.add_input_value_indices(value_index);
    }
  }
  ASSIGN_OR_RETURN(
      auto value_index,
      encoder.EncodeValue(TypedValue::FromValue(op.GetOutputQType())));
  value_proto.add_input_value_indices(value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeGenericOperator(const GenericOperator& op,
                                                 Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* generic_operator_proto =
      value_proto.MutableExtension(OperatorV1Proto::extension)
          ->mutable_generic_operator();
  generic_operator_proto->set_name(std::string(op.display_name()));
  generic_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  generic_operator_proto->set_signature_spec(
      GetExprOperatorSignatureSpec(op.signature()));
  if (!op.doc().empty()) {
    generic_operator_proto->set_doc(op.doc());
  }
  for (const auto& param : op.signature().parameters) {
    if (param.default_value.has_value()) {
      ASSIGN_OR_RETURN(auto value_index,
                       encoder.EncodeValue(*param.default_value));
      value_proto.add_input_value_indices(value_index);
    }
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeGenericOperatorOverload(
    const GenericOperatorOverload& op, Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OperatorV1Proto::extension)
      ->mutable_generic_operator_overload();
  ASSIGN_OR_RETURN(
      auto value_index,
      encoder.EncodeValue(TypedValue::FromValue(op.base_operator())));
  value_proto.add_input_value_indices(value_index);
  ASSIGN_OR_RETURN(auto expr_index,
                   encoder.EncodeExpr(op.prepared_overload_condition_expr()));
  value_proto.add_input_expr_indices(expr_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOperatorQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OperatorV1Proto::extension)
      ->set_operator_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOperator(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    const auto& qtype_value = value.UnsafeAs<QTypePtr>();
    if (qtype_value == GetQType<ExprOperatorPtr>()) {
      return EncodeOperatorQType(encoder);
    }

  } else if (value.GetType() == GetQType<ExprOperatorPtr>()) {
    const auto& op_value = *value.UnsafeAs<ExprOperatorPtr>();
    if (typeid(op_value) == typeid(RegisteredOperator)) {
      return EncodeRegisteredOperator(
          static_cast<const RegisteredOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(LambdaOperator)) {
      return EncodeLambdaOperator(static_cast<const LambdaOperator&>(op_value),
                                  encoder);
    } else if (typeid(op_value) == typeid(MakeTupleOperator)) {
      return EncodeMakeTupleOperator(
          static_cast<const MakeTupleOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(GetNthOperator)) {
      return EncodeGetNthOperator(static_cast<const GetNthOperator&>(op_value),
                                  encoder);
    } else if (typeid(op_value) == typeid(OverloadedOperator)) {
      return EncodeOverloadedOperator(
          static_cast<const OverloadedOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(expr_operators::WhileLoopOperator)) {
      return EncodeWhileLoopOperator(
          static_cast<const expr_operators::WhileLoopOperator&>(op_value),
          encoder);
    } else if (typeid(op_value) == typeid(BackendOperator)) {
      return EncodeBackendOperator(
          static_cast<const BackendOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(RestrictedLambdaOperator)) {
      return EncodeRestrictedLambdaOperator(
          static_cast<const RestrictedLambdaOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(DummyOperator)) {
      return EncodeDummyOperator(static_cast<const DummyOperator&>(op_value),
                                 encoder);
    } else if (typeid(op_value) == typeid(DispatchOperator)) {
      return EncodeDispatchOperator(
          static_cast<const DispatchOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(GenericOperator)) {
      return EncodeGenericOperator(
          static_cast<const GenericOperator&>(op_value), encoder);
    } else if (typeid(op_value) == typeid(GenericOperatorOverload)) {
      return EncodeGenericOperatorOverload(
          static_cast<const GenericOperatorOverload&>(op_value), encoder);
    }
  }
  return absl::UnimplementedError(absl::StrFormat(
      "%s does not support serialization of %s: %s; this may indicate a "
      "missing BUILD dependency on the encoder for this operator",
      kOperatorV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n}, .init_fn = [] {
          return RegisterValueEncoderByQType(GetQType<ExprOperatorPtr>(),
                                             EncodeOperator);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
