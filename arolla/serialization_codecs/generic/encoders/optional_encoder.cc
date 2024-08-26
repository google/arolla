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
#include "absl/strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/optional_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
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
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kOptionalV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOptionalUnitValue(TypedRef value,
                                                   Encoder& encoder) {
  DCHECK(value.GetType() == GetQType<OptionalUnit>());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OptionalV1Proto::extension)
      ->set_optional_unit_value(
          static_cast<const OptionalUnit*>(value.GetRawPointer())->present);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOptionalUnitQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OptionalV1Proto::extension)
      ->set_optional_unit_qtype(true);
  return value_proto;
}

#define GEN_ENCODE_OPTIONAL(NAME, T, FIELD, ACTION)                          \
  absl::StatusOr<ValueProto> EncodeOptional##NAME##Value(TypedRef value,     \
                                                         Encoder& encoder) { \
    /* It's safe because we dispatch based on qtype in EncodeOptional(). */  \
    const auto& y = value.UnsafeAs<OptionalValue<T>>();                      \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));              \
    auto field = value_proto.MutableExtension(OptionalV1Proto::extension)    \
                     ->mutable_##FIELD##_value();                            \
    if (y.present) {                                                         \
      const auto& x = y.value;                                               \
      field->ACTION;                                                         \
    }                                                                        \
    return value_proto;                                                      \
  }                                                                          \
                                                                             \
  absl::StatusOr<ValueProto> EncodeOptional##NAME##QType(Encoder& encoder) { \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));              \
    value_proto.MutableExtension(OptionalV1Proto::extension)                 \
        ->set_##FIELD##_qtype(true);                                         \
    return value_proto;                                                      \
  }

GEN_ENCODE_OPTIONAL(Boolean, bool, optional_boolean, set_value(x))
GEN_ENCODE_OPTIONAL(Bytes, Bytes, optional_bytes,
                    set_value(absl::string_view(x).data(),
                              absl::string_view(x).size()))
GEN_ENCODE_OPTIONAL(Text, Text, optional_text,
                    set_value(absl::string_view(x).data(),
                              absl::string_view(x).size()))
GEN_ENCODE_OPTIONAL(Int32, int32_t, optional_int32, set_value(x))
GEN_ENCODE_OPTIONAL(Int64, int64_t, optional_int64, set_value(x))
GEN_ENCODE_OPTIONAL(UInt64, uint64_t, optional_uint64, set_value(x))
GEN_ENCODE_OPTIONAL(Float32, float, optional_float32, set_value(x))
GEN_ENCODE_OPTIONAL(Float64, double, optional_float64, set_value(x))
GEN_ENCODE_OPTIONAL(WeakFloat, double, optional_weak_float, set_value(x))

#undef GEN_ENCODE_OPTIONAL

absl::StatusOr<ValueProto> EncodeOptionalShapeValue(TypedRef value,
                                                    Encoder& encoder) {
  DCHECK(value.GetType() == GetQType<OptionalScalarShape>());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OptionalV1Proto::extension)
      ->set_optional_shape_value(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOptionalShapeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(OptionalV1Proto::extension)
      ->set_optional_shape_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeOptional(TypedRef value, Encoder& encoder) {
  using QTypeEncoder = absl::StatusOr<ValueProto> (*)(Encoder&);
  using ValueEncoder = absl::StatusOr<ValueProto> (*)(TypedRef, Encoder&);
  using QTypeEncoders = absl::flat_hash_map<QTypePtr, QTypeEncoder>;
  using ValueEncoders = absl::flat_hash_map<QTypePtr, ValueEncoder>;
  static const absl::NoDestructor<QTypeEncoders> kQTypeEncoders(QTypeEncoders{
      {GetOptionalQType<Unit>(), &EncodeOptionalUnitQType},
      {GetOptionalQType<bool>(), &EncodeOptionalBooleanQType},
      {GetOptionalQType<Bytes>(), &EncodeOptionalBytesQType},
      {GetOptionalQType<Text>(), &EncodeOptionalTextQType},
      {GetOptionalQType<int32_t>(), &EncodeOptionalInt32QType},
      {GetOptionalQType<int64_t>(), &EncodeOptionalInt64QType},
      {GetOptionalQType<uint64_t>(), &EncodeOptionalUInt64QType},
      {GetOptionalQType<float>(), &EncodeOptionalFloat32QType},
      {GetOptionalQType<double>(), &EncodeOptionalFloat64QType},
      {GetOptionalWeakFloatQType(), &EncodeOptionalWeakFloatQType},
      {GetQType<OptionalScalarShape>(), &EncodeOptionalShapeQType},
  });
  static const absl::NoDestructor<ValueEncoders> kValueEncoders(ValueEncoders{
      {GetOptionalQType<Unit>(), &EncodeOptionalUnitValue},
      {GetOptionalQType<bool>(), &EncodeOptionalBooleanValue},
      {GetOptionalQType<Bytes>(), &EncodeOptionalBytesValue},
      {GetOptionalQType<Text>(), &EncodeOptionalTextValue},
      {GetOptionalQType<int32_t>(), &EncodeOptionalInt32Value},
      {GetOptionalQType<int64_t>(), &EncodeOptionalInt64Value},
      {GetOptionalQType<uint64_t>(), &EncodeOptionalUInt64Value},
      {GetOptionalQType<float>(), &EncodeOptionalFloat32Value},
      {GetOptionalQType<double>(), &EncodeOptionalFloat64Value},
      {GetOptionalWeakFloatQType(), &EncodeOptionalWeakFloatValue},
      {GetQType<OptionalScalarShape>(), &EncodeOptionalShapeValue},
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
      kOptionalV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<OptionalScalarShape>(), EncodeOptional));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetOptionalWeakFloatQType(), EncodeOptional));
          absl::Status status;
          meta::foreach_type<ScalarTypes>([&](auto meta_type) {
            if (status.ok()) {
              status = RegisterValueEncoderByQType(
                  GetOptionalQType<typename decltype(meta_type)::type>(),
                  EncodeOptional);
            }
          });
          return status;
        })

}  // namespace
}  // namespace arolla::serialization_codecs
