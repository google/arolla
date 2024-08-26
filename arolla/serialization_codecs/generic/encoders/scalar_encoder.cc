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
#include <cstdint>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/scalar_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprQuote;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kScalarV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

#define GEN_ENCODE(NAME, T, FIELD, ...)                                   \
  absl::StatusOr<ValueProto> Encode##NAME##Value(TypedRef value,          \
                                                 Encoder& encoder) {      \
    /* It's safe because we dispatch based on qtype in EncodeScalar(). */ \
    const auto& x = value.UnsafeAs<T>();                                  \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));           \
    value_proto.MutableExtension(ScalarV1Proto::extension)                \
        ->set_##FIELD##_value(__VA_ARGS__);                               \
    return value_proto;                                                   \
  }                                                                       \
                                                                          \
  absl::StatusOr<ValueProto> Encode##NAME##QType(Encoder& encoder) {      \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));           \
    value_proto.MutableExtension(ScalarV1Proto::extension)                \
        ->set_##FIELD##_qtype(true);                                      \
    return value_proto;                                                   \
  }

GEN_ENCODE(Unit, Unit, unit, (sizeof(x) > 0))
GEN_ENCODE(Boolean, bool, boolean, x)
GEN_ENCODE(Bytes, Bytes, bytes, x.data(), x.size())
GEN_ENCODE(Text, Text, text, x.view().data(), x.view().size())
GEN_ENCODE(Int32, int32_t, int32, x)
GEN_ENCODE(Int64, int64_t, int64, x)
GEN_ENCODE(UInt64, uint64_t, uint64, x)
GEN_ENCODE(Float32, float, float32, x)
GEN_ENCODE(Float64, double, float64, x)
GEN_ENCODE(WeakFloat, double, weak_float, x)

#undef GEN_ENCODE_VALUE

absl::StatusOr<ValueProto> EncodeQTypeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)->set_qtype_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeNothingQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_qtype_qtype(false);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeScalarToScalarEdgeValue(TypedRef value,
                                                         Encoder& encoder) {
  DCHECK_EQ(value.GetType(), GetQType<ScalarToScalarEdge>());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_scalar_to_scalar_edge_value(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeScalarShapeValue(TypedRef value,
                                                  Encoder& encoder) {
  DCHECK_EQ(value.GetType(), GetQType<ScalarShape>());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_scalar_shape_value(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeUnspecifiedValue(TypedRef value,
                                                  Encoder& encoder) {
  DCHECK_EQ(value.GetType(), GetUnspecifiedQType());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_unspecified_value(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeScalarToScalarEdgeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_scalar_to_scalar_edge_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeScalarShapeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_scalar_shape_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeUnspecifiedQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_unspecified_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeExprQuoteQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_expr_quote_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeExprQuoteValue(TypedRef value,
                                                Encoder& encoder) {
  DCHECK_EQ(value.GetType(), GetQType<ExprQuote>());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  const ExprQuote& quote = value.UnsafeAs<ExprQuote>();
  ASSIGN_OR_RETURN(ExprNodePtr expr, quote.expr());
  ASSIGN_OR_RETURN(int64_t encoded_id, encoder.EncodeExpr(expr));
  value_proto.add_input_expr_indices(encoded_id);
  value_proto.MutableExtension(ScalarV1Proto::extension)
      ->set_expr_quote_value(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeScalar(TypedRef value, Encoder& encoder) {
  using QTypeEncoder = absl::StatusOr<ValueProto> (*)(Encoder&);
  using ValueEncoder = absl::StatusOr<ValueProto> (*)(TypedRef, Encoder&);
  using QTypeEncoders = absl::flat_hash_map<QTypePtr, QTypeEncoder>;
  using ValueEncoders = absl::flat_hash_map<QTypePtr, ValueEncoder>;
  static const absl::NoDestructor<QTypeEncoders> kQTypeEncoders(QTypeEncoders{
      {GetQType<Unit>(), &EncodeUnitQType},
      {GetQType<bool>(), &EncodeBooleanQType},
      {GetQType<Bytes>(), &EncodeBytesQType},
      {GetQType<Text>(), &EncodeTextQType},
      {GetQType<int32_t>(), &EncodeInt32QType},
      {GetQType<int64_t>(), &EncodeInt64QType},
      {GetQType<uint64_t>(), &EncodeUInt64QType},
      {GetQType<float>(), &EncodeFloat32QType},
      {GetQType<double>(), &EncodeFloat64QType},
      {GetWeakFloatQType(), &EncodeWeakFloatQType},
      {GetQType<ScalarToScalarEdge>(), &EncodeScalarToScalarEdgeQType},
      {GetQType<ScalarShape>(), &EncodeScalarShapeQType},
      {GetUnspecifiedQType(), &EncodeUnspecifiedQType},
      {GetQTypeQType(), &EncodeQTypeQType},
      {GetNothingQType(), &EncodeNothingQType},
      {GetQType<ExprQuote>(), &EncodeExprQuoteQType},
  });
  static const absl::NoDestructor<ValueEncoders> kValueEncoders(ValueEncoders{
      {GetQType<Unit>(), &EncodeUnitValue},
      {GetQType<bool>(), &EncodeBooleanValue},
      {GetQType<Bytes>(), &EncodeBytesValue},
      {GetQType<Text>(), &EncodeTextValue},
      {GetQType<int32_t>(), &EncodeInt32Value},
      {GetQType<int64_t>(), &EncodeInt64Value},
      {GetQType<uint64_t>(), &EncodeUInt64Value},
      {GetQType<float>(), &EncodeFloat32Value},
      {GetQType<double>(), &EncodeFloat64Value},
      {GetWeakFloatQType(), &EncodeWeakFloatValue},
      {GetQType<ScalarToScalarEdge>(), &EncodeScalarToScalarEdgeValue},
      {GetQType<ScalarShape>(), &EncodeScalarShapeValue},
      {GetUnspecifiedQType(), &EncodeUnspecifiedValue},
      {GetQType<ExprQuote>(), &EncodeExprQuoteValue},
  });
  if (value.GetType() == GetQType<QTypePtr>()) {
    const auto& qtype_value = value.UnsafeAs<QTypePtr>();
    auto it = kQTypeEncoders->find(qtype_value);
    if (it != kQTypeEncoders->end()) {
      return it->second(encoder);
    }
  } else {
    auto it = kValueEncoders->find(value.GetType());
    if (it != kValueEncoders->end()) {
      return it->second(value, encoder);
    }
  }
  return absl::UnimplementedError(absl::StrFormat(
      "%s does not support serialization of %s: %s; this may indicate a "
      "missing BUILD dependency on the encoder for this qtype",
      kScalarV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetQTypeQType(), EncodeScalar));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<ScalarToScalarEdge>(), EncodeScalar));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(GetQType<ScalarShape>(),
                                                      EncodeScalar));
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetNothingQType(), EncodeScalar));
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetWeakFloatQType(), EncodeScalar));
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetUnspecifiedQType(), EncodeScalar));
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetQType<ExprQuote>(), EncodeScalar));
          absl::Status status;
          meta::foreach_type<ScalarTypes>([&](auto meta_type) {
            if (status.ok()) {
              status = RegisterValueEncoderByQType(
                  GetQType<typename decltype(meta_type)::type>(), EncodeScalar);
            }
          });
          return status;
        })

}  // namespace
}  // namespace arolla::serialization_codecs
