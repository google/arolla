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
#include <type_traits>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/dense_array/codec_name.h"
#include "arolla/serialization_codecs/dense_array/dense_array_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

namespace bm = ::arolla::bitmap;

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

using BitmapProto = std::decay_t<std::remove_const_t<
    decltype(std::declval<DenseArrayV1Proto::DenseArrayUnitProto>().bitmap())>>;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kDenseArrayV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

BitmapProto GenBitmapProto(const bm::Bitmap& bitmap, int offset, int64_t size) {
  BitmapProto result;
  if (bm::CountBits(bitmap, offset, size) == size) {
    return result;
  }
  const int64_t bitmapSize = bm::BitmapSize(size);
  result.Resize(bitmapSize, 0);
  for (int64_t i = 0; i < bitmapSize; ++i) {
    result[i] = bm::GetWordWithOffset(bitmap, i, offset);
  }
  if (int last_word_usage = size % bm::kWordBitCount) {
    result[bitmapSize - 1] &= (1U << last_word_usage) - 1;
  }
  return result;
}

absl::StatusOr<ValueProto> EncodeDenseArrayUnitValue(TypedRef value,
                                                     Encoder& encoder) {
  DCHECK(value.GetType() == GetQType<DenseArray<Unit>>());
  const auto& dense_array = value.UnsafeAs<DenseArray<Unit>>();
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* dense_array_unit_proto =
      value_proto.MutableExtension(DenseArrayV1Proto::extension)
          ->mutable_dense_array_unit_value();
  dense_array_unit_proto->set_size(dense_array.size());
  *dense_array_unit_proto->mutable_bitmap() = GenBitmapProto(
      dense_array.bitmap, dense_array.bitmap_bit_offset, dense_array.size());
  return value_proto;
}

#define GEN_ENCODE_DENSE_ARRAY_QTYPE(NAME, FIELD)                              \
  absl::StatusOr<ValueProto> EncodeDenseArray##NAME##QType(Encoder& encoder) { \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));                \
    value_proto.MutableExtension(DenseArrayV1Proto::extension)                 \
        ->set_##FIELD##_qtype(true);                                           \
    return value_proto;                                                        \
  }

GEN_ENCODE_DENSE_ARRAY_QTYPE(Unit, dense_array_unit)

#define GEN_ENCODE_DENSE_ARRAY_VALUE(NAME, T, FIELD)                           \
  absl::StatusOr<ValueProto> EncodeDenseArray##NAME##Value(TypedRef value,     \
                                                           Encoder& encoder) { \
    /* It's safe because we dispatch based on qtype in EncodeDenseArray(). */  \
    const auto& dense_array = value.UnsafeAs<DenseArray<T>>();                 \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));                \
    auto* dense_array_value_proto =                                            \
        value_proto.MutableExtension(DenseArrayV1Proto::extension)             \
            ->mutable_##FIELD##_value();                                       \
    dense_array_value_proto->set_size(dense_array.size());                     \
    *dense_array_value_proto->mutable_bitmap() =                               \
        GenBitmapProto(dense_array.bitmap, dense_array.bitmap_bit_offset,      \
                       dense_array.size());                                    \
    dense_array.ForEach([&](int64_t, bool present, const T& value) {           \
      if (present) {                                                           \
        dense_array_value_proto->add_values(value);                            \
      }                                                                        \
    });                                                                        \
    return value_proto;                                                        \
  }                                                                            \
  GEN_ENCODE_DENSE_ARRAY_QTYPE(NAME, FIELD)

GEN_ENCODE_DENSE_ARRAY_VALUE(Boolean, bool, dense_array_boolean)
GEN_ENCODE_DENSE_ARRAY_VALUE(Int32, int32_t, dense_array_int32)
GEN_ENCODE_DENSE_ARRAY_VALUE(Int64, int64_t, dense_array_int64)
GEN_ENCODE_DENSE_ARRAY_VALUE(UInt64, uint64_t, dense_array_uint64)
GEN_ENCODE_DENSE_ARRAY_VALUE(Float32, float, dense_array_float32)
GEN_ENCODE_DENSE_ARRAY_VALUE(Float64, double, dense_array_float64)

#undef GEN_ENCODE_DENSE_ARRAY_VALUE

#define GEN_ENCODE_DENSE_ARRAY_STRING_VALUE(NAME, T, FIELD)                    \
  absl::StatusOr<ValueProto> EncodeDenseArray##NAME##Value(TypedRef value,     \
                                                           Encoder& encoder) { \
    /* It's safe because we dispatch based on qtype in EncodeDenseArray(). */  \
    const auto& dense_array = value.UnsafeAs<DenseArray<T>>();                 \
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));                \
    auto* dense_array_value_proto =                                            \
        value_proto.MutableExtension(DenseArrayV1Proto::extension)             \
            ->mutable_##FIELD##_value();                                       \
    dense_array_value_proto->set_size(dense_array.size());                     \
    *dense_array_value_proto->mutable_bitmap() =                               \
        GenBitmapProto(dense_array.bitmap, dense_array.bitmap_bit_offset,      \
                       dense_array.size());                                    \
    dense_array_value_proto->set_characters(                                   \
        dense_array.values.characters().span().data(),                         \
        dense_array.values.characters().span().size());                        \
    for (size_t i = 0; i < dense_array.size(); ++i) {                          \
      if (dense_array.present(i)) {                                            \
        const auto& offset = dense_array.values.offsets()[i];                  \
        dense_array_value_proto->add_value_offset_starts(                      \
            offset.start - dense_array.values.base_offset());                  \
        dense_array_value_proto->add_value_offset_ends(                        \
            offset.end - dense_array.values.base_offset());                    \
      }                                                                        \
    }                                                                          \
    return value_proto;                                                        \
  }                                                                            \
  GEN_ENCODE_DENSE_ARRAY_QTYPE(NAME, FIELD)

GEN_ENCODE_DENSE_ARRAY_STRING_VALUE(Bytes, Bytes, dense_array_bytes)
GEN_ENCODE_DENSE_ARRAY_STRING_VALUE(Text, Text, dense_array_text)

#undef GEN_ENCODE_DENSE_ARRAY_STRING_VALUE
#undef GEN_ENCODE_DENSE_ARRAY_QTYPE

absl::StatusOr<ValueProto> EncodeDenseArrayEdgeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DenseArrayV1Proto::extension)
      ->set_dense_array_edge_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDenseArrayEdgeValue(TypedRef value,
                                                     Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* dense_array_edge_proto =
      value_proto.MutableExtension(DenseArrayV1Proto::extension)
          ->mutable_dense_array_edge_value();

  /* It's safe because we dispatch based on qtype in EncodeDenseArray(). */
  const auto& dense_array_edge = value.UnsafeAs<DenseArrayEdge>();
  ASSIGN_OR_RETURN(auto dense_array_value_index,
                   encoder.EncodeValue(
                       TypedValue::FromValue(dense_array_edge.edge_values())));
  value_proto.add_input_value_indices(dense_array_value_index);

  switch (dense_array_edge.edge_type()) {
    case DenseArrayEdge::EdgeType::MAPPING:
      dense_array_edge_proto->set_edge_type(
          DenseArrayV1Proto::DenseArrayEdgeProto::MAPPING);
      dense_array_edge_proto->set_parent_size(dense_array_edge.parent_size());
      return value_proto;

    case DenseArrayEdge::EdgeType::SPLIT_POINTS:
      dense_array_edge_proto->set_edge_type(
          DenseArrayV1Proto::DenseArrayEdgeProto::SPLIT_POINTS);
      return value_proto;
  }
  return absl::InternalError(absl::StrCat("unknown DesnseArrayEdge edge type: ",
                                          dense_array_edge.edge_type()));
}

absl::StatusOr<ValueProto> EncodeDenseArrayToScalarEdgeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DenseArrayV1Proto::extension)
      ->set_dense_array_to_scalar_edge_qtype(true);
  return value_proto;
}
absl::StatusOr<ValueProto> EncodeDenseArrayToScalarEdgeValue(TypedRef value,
                                                             Encoder& encoder) {
  /* It's safe because we dispatch based on qtype in EncodeDenseArray(). */
  const auto& dense_array_to_scalar_edge =
      value.UnsafeAs<DenseArrayGroupScalarEdge>();
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DenseArrayV1Proto::extension)
      ->set_dense_array_to_scalar_edge_value(
          dense_array_to_scalar_edge.child_size());
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDenseArrayShapeQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DenseArrayV1Proto::extension)
      ->set_dense_array_shape_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDenseArrayShapeValue(TypedRef value,
                                                      Encoder& encoder) {
  /* It's safe because we dispatch based on qtype in EncodeDenseArray(). */
  const auto& dense_array_shape = value.UnsafeAs<DenseArrayShape>();
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DenseArrayV1Proto::extension)
      ->set_dense_array_shape_value(dense_array_shape.size);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDenseArray(TypedRef value, Encoder& encoder) {
  using QTypeEncoder = absl::StatusOr<ValueProto> (*)(Encoder&);
  using ValueEncoder = absl::StatusOr<ValueProto> (*)(TypedRef, Encoder&);
  using QTypeEncoders = absl::flat_hash_map<QTypePtr, QTypeEncoder>;
  using ValueEncoders = absl::flat_hash_map<QTypePtr, ValueEncoder>;
  static const absl::NoDestructor<QTypeEncoders> kQTypeEncoders(QTypeEncoders{
      {GetDenseArrayQType<Unit>(), &EncodeDenseArrayUnitQType},
      {GetDenseArrayQType<bool>(), &EncodeDenseArrayBooleanQType},
      {GetDenseArrayQType<Bytes>(), &EncodeDenseArrayBytesQType},
      {GetDenseArrayQType<Text>(), &EncodeDenseArrayTextQType},
      {GetDenseArrayQType<int32_t>(), &EncodeDenseArrayInt32QType},
      {GetDenseArrayQType<int64_t>(), &EncodeDenseArrayInt64QType},
      {GetDenseArrayQType<uint64_t>(), &EncodeDenseArrayUInt64QType},
      {GetDenseArrayQType<float>(), &EncodeDenseArrayFloat32QType},
      {GetDenseArrayQType<double>(), &EncodeDenseArrayFloat64QType},
      {GetQType<DenseArrayEdge>(), &EncodeDenseArrayEdgeQType},
      {GetQType<DenseArrayGroupScalarEdge>(),
       &EncodeDenseArrayToScalarEdgeQType},
      {GetQType<DenseArrayShape>(), &EncodeDenseArrayShapeQType},
  });
  static const absl::NoDestructor<ValueEncoders> kValueEncoders(ValueEncoders{
      {GetDenseArrayQType<Unit>(), &EncodeDenseArrayUnitValue},
      {GetDenseArrayQType<bool>(), &EncodeDenseArrayBooleanValue},
      {GetDenseArrayQType<Bytes>(), &EncodeDenseArrayBytesValue},
      {GetDenseArrayQType<Text>(), &EncodeDenseArrayTextValue},
      {GetDenseArrayQType<int32_t>(), &EncodeDenseArrayInt32Value},
      {GetDenseArrayQType<int64_t>(), &EncodeDenseArrayInt64Value},
      {GetDenseArrayQType<uint64_t>(), &EncodeDenseArrayUInt64Value},
      {GetDenseArrayQType<float>(), &EncodeDenseArrayFloat32Value},
      {GetDenseArrayQType<double>(), &EncodeDenseArrayFloat64Value},
      {GetQType<DenseArrayEdge>(), &EncodeDenseArrayEdgeValue},
      {GetQType<DenseArrayGroupScalarEdge>(),
       &EncodeDenseArrayToScalarEdgeValue},
      {GetQType<DenseArrayShape>(), &EncodeDenseArrayShapeValue},
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
      kDenseArrayV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<DenseArrayEdge>(), EncodeDenseArray));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<DenseArrayGroupScalarEdge>(), EncodeDenseArray));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              GetQType<DenseArrayShape>(), EncodeDenseArray));
          absl::Status status;
          arolla::meta::foreach_type<ScalarTypes>([&](auto meta_type) {
            if (status.ok()) {
              status = RegisterValueEncoderByQType(
                  GetDenseArrayQType<typename decltype(meta_type)::type>(),
                  EncodeDenseArray);
            }
          });
          return status;
        })

}  // namespace
}  // namespace arolla::serialization_codecs
