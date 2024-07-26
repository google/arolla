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

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"  // IWYU pragma: keep
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/array/array_codec.pb.h"
#include "arolla/serialization_codecs/array/codec_name.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kArrayV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

template <typename T>
absl::Status EncodeArrayValueImpl(ArrayV1Proto::ArrayProto& array_proto,
                                  TypedRef value, Encoder& encoder,
                                  ValueProto& value_proto) {
  /* It's safe because we dispatch based on qtype in EncodeArray(). */
  const auto& array = value.UnsafeAs<Array<T>>();
  array_proto.set_size(array.size());
  if (array.size() > 0) {
    ASSIGN_OR_RETURN(
        auto dense_data_value_index,
        encoder.EncodeValue(TypedValue::FromValue(array.dense_data())));
    value_proto.add_input_value_indices(dense_data_value_index);
    if (array.dense_data().size() == array.size()) {
      DCHECK_EQ(array.id_filter().type(), IdFilter::Type::kFull);
    } else {
      DCHECK_EQ(array.id_filter().ids().size(), array.dense_data().size());
      array_proto.mutable_ids()->Add(array.id_filter().ids().begin(),
                                     array.id_filter().ids().end());
      for (auto& id : *array_proto.mutable_ids()) {
        id -= array.id_filter().ids_offset();
      }
      ASSIGN_OR_RETURN(
          auto missing_id_value_index,
          encoder.EncodeValue(TypedValue::FromValue(array.missing_id_value())));
      value_proto.add_input_value_indices(missing_id_value_index);
    }
  }
  return absl::OkStatus();
}

#define GEN_ENCODE_ARRAY(NAME, T, FIELD)                                      \
  absl::StatusOr<ValueProto> EncodeArray##NAME##QType(Encoder& encoder) {     \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));               \
    value_proto.MutableExtension(ArrayV1Proto::extension)                     \
        ->set_##FIELD##_qtype(true);                                          \
    return value_proto;                                                       \
  }                                                                           \
                                                                              \
  absl::StatusOr<ValueProto> EncodeArray##NAME##Value(TypedRef value,         \
                                                      Encoder& encoder) {     \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));               \
    auto* array_proto = value_proto.MutableExtension(ArrayV1Proto::extension) \
                            ->mutable_##FIELD##_value();                      \
    RETURN_IF_ERROR(                                                          \
        EncodeArrayValueImpl<T>(*array_proto, value, encoder, value_proto));  \
    return value_proto;                                                       \
  }

GEN_ENCODE_ARRAY(Unit, Unit, array_unit)
GEN_ENCODE_ARRAY(Bytes, Bytes, array_bytes)
GEN_ENCODE_ARRAY(Text, Text, array_text)
GEN_ENCODE_ARRAY(Boolean, bool, array_boolean)
GEN_ENCODE_ARRAY(Int32, int32_t, array_int32)
GEN_ENCODE_ARRAY(Int64, int64_t, array_int64)
GEN_ENCODE_ARRAY(UInt64, uint64_t, array_uint64)
GEN_ENCODE_ARRAY(Float32, float, array_float32)
GEN_ENCODE_ARRAY(Float64, double, array_float64)

#undef GEN_ENCODE_ARRAY

absl::StatusOr<ValueProto> EncodeArrayEdgeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ArrayV1Proto::extension)
      ->set_array_edge_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeArrayEdgeValue(TypedRef value,
                                                Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* array_edge_proto =
      value_proto.MutableExtension(ArrayV1Proto::extension)
          ->mutable_array_edge_value();

  /* It's safe because we dispatch based on qtype in EncodeArray(). */
  const auto& array_edge = value.UnsafeAs<ArrayEdge>();
  ASSIGN_OR_RETURN(
      auto array_value_index,
      encoder.EncodeValue(TypedValue::FromValue(array_edge.edge_values())));
  value_proto.add_input_value_indices(array_value_index);

  switch (array_edge.edge_type()) {
    case ArrayEdge::EdgeType::MAPPING:
      array_edge_proto->set_edge_type(ArrayV1Proto::ArrayEdgeProto::MAPPING);
      array_edge_proto->set_parent_size(array_edge.parent_size());
      return value_proto;

    case ArrayEdge::EdgeType::SPLIT_POINTS:
      array_edge_proto->set_edge_type(
          ArrayV1Proto::ArrayEdgeProto::SPLIT_POINTS);
      return value_proto;
  }
  return absl::InternalError(
      absl::StrCat("unknown ArrayEdge edge type: ", array_edge.edge_type()));
}

absl::StatusOr<ValueProto> EncodeArrayToScalarEdgeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ArrayV1Proto::extension)
      ->set_array_to_scalar_edge_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeArrayToScalarEdgeValue(TypedRef value,
                                                        Encoder& encoder) {
  /* It's safe because we dispatch based on qtype in EncodeArrayEdge(). */
  const auto& array_to_scalar_edge = value.UnsafeAs<ArrayGroupScalarEdge>();
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ArrayV1Proto::extension)
      ->set_array_to_scalar_edge_value(array_to_scalar_edge.child_size());
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeArrayShapeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ArrayV1Proto::extension)
      ->set_array_shape_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeArrayShapeValue(TypedRef value,
                                                 Encoder& encoder) {
  /* It's safe because we dispatch based on qtype in EncodeArray(). */
  const auto& array_shape = value.UnsafeAs<ArrayShape>();
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(ArrayV1Proto::extension)
      ->set_array_shape_value(array_shape.size);
  return value_proto;
}

}  // namespace

absl::StatusOr<ValueProto> EncodeArray(TypedRef value, Encoder& encoder) {
  using QTypeEncoder = absl::StatusOr<ValueProto> (*)(Encoder&);
  using ValueEncoder = absl::StatusOr<ValueProto> (*)(TypedRef, Encoder&);
  using QTypeEncoders = absl::flat_hash_map<QTypePtr, QTypeEncoder>;
  using ValueEncoders = absl::flat_hash_map<QTypePtr, ValueEncoder>;
  static const Indestructible<QTypeEncoders> kQTypeEncoders(QTypeEncoders{
      {GetArrayQType<Unit>(), &EncodeArrayUnitQType},
      {GetArrayQType<bool>(), &EncodeArrayBooleanQType},
      {GetArrayQType<Bytes>(), &EncodeArrayBytesQType},
      {GetArrayQType<Text>(), &EncodeArrayTextQType},
      {GetArrayQType<int32_t>(), &EncodeArrayInt32QType},
      {GetArrayQType<int64_t>(), &EncodeArrayInt64QType},
      {GetArrayQType<uint64_t>(), &EncodeArrayUInt64QType},
      {GetArrayQType<float>(), &EncodeArrayFloat32QType},
      {GetArrayQType<double>(), &EncodeArrayFloat64QType},
      {GetQType<ArrayEdge>(), &EncodeArrayEdgeQType},
      {GetQType<ArrayGroupScalarEdge>(), &EncodeArrayToScalarEdgeQType},
      {GetQType<ArrayShape>(), &EncodeArrayShapeQType},
  });
  static const Indestructible<ValueEncoders> kValueEncoders(ValueEncoders{
      {GetArrayQType<Unit>(), &EncodeArrayUnitValue},
      {GetArrayQType<bool>(), &EncodeArrayBooleanValue},
      {GetArrayQType<Bytes>(), &EncodeArrayBytesValue},
      {GetArrayQType<Text>(), &EncodeArrayTextValue},
      {GetArrayQType<int32_t>(), &EncodeArrayInt32Value},
      {GetArrayQType<int64_t>(), &EncodeArrayInt64Value},
      {GetArrayQType<uint64_t>(), &EncodeArrayUInt64Value},
      {GetArrayQType<float>(), &EncodeArrayFloat32Value},
      {GetArrayQType<double>(), &EncodeArrayFloat64Value},
      {GetQType<ArrayEdge>(), &EncodeArrayEdgeValue},
      {GetQType<ArrayGroupScalarEdge>(), &EncodeArrayToScalarEdgeValue},
      {GetQType<ArrayShape>(), &EncodeArrayShapeValue},
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
      kArrayV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetQType<ArrayEdge>(), EncodeArray));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<ArrayGroupScalarEdge>(), EncodeArray));
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetQType<ArrayShape>(), EncodeArray));
          absl::Status status;
          arolla::meta::foreach_type<ScalarTypes>([&](auto meta_type) {
            if (status.ok()) {
              status = RegisterValueEncoderByQType(
                  GetArrayQType<typename decltype(meta_type)::type>(),
                  EncodeArray);
            }
          });
          return status;
        })

}  // namespace arolla::serialization_codecs
