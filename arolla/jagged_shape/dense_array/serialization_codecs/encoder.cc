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
#include "arolla/serialization_base/encoder.h"

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/jagged_shape/dense_array/serialization_codecs/codec_name.h"
#include "arolla/jagged_shape/dense_array/serialization_codecs/jagged_shape_codec.pb.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::JaggedDenseArrayShapeV1Proto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index,
                   encoder.EncodeCodec(kJaggedDenseArrayShapeV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeJaggedDenseArrayShapeQType(TypedRef value,
                                                            Encoder& encoder) {
  // Note: Safe since this function is only called for QTypes.
  const auto& qtype = value.UnsafeAs<QTypePtr>();
  if (qtype != GetQType<JaggedDenseArrayShape>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s does not support serialization of %s",
                        kJaggedDenseArrayShapeV1Codec, qtype->name()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(JaggedDenseArrayShapeV1Proto::extension)
      ->set_jagged_dense_array_shape_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeJaggedDenseArrayShapeValue(TypedRef value,
                                                            Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(JaggedDenseArrayShapeV1Proto::extension)
      ->set_jagged_dense_array_shape_value(true);
  // Note: Safe since this function is only called for JaggedDenseArrayShapes.
  const auto& jagged_shape = value.UnsafeAs<JaggedDenseArrayShape>();
  for (const auto& edge : jagged_shape.edges()) {
    ASSIGN_OR_RETURN(int64_t edge_index,
                     encoder.EncodeValue(TypedValue::FromValue(edge)));
    value_proto.add_input_value_indices(edge_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeJaggedDenseArrayShape(TypedRef value,
                                                       Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    return EncodeJaggedDenseArrayShapeQType(value, encoder);
  } else if (value.GetType() == GetQType<JaggedDenseArrayShape>()) {
    return EncodeJaggedDenseArrayShapeValue(value, encoder);
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "%s does not support serialization of %s: %s",
        kJaggedDenseArrayShapeV1Codec, value.GetType()->name(), value.Repr()));
  }
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueEncoderByQType(GetQType<JaggedDenseArrayShape>(),
                                             EncodeJaggedDenseArrayShape);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
