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

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/dict/codec_name.h"
#include "arolla/serialization_codecs/dict/dict_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kDictV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeKeyToRowDictQType(
    QTypePtr key_to_row_dict_qtype, Encoder& encoder) {
  DCHECK(IsKeyToRowDictQType(key_to_row_dict_qtype));
  QTypePtr key_qtype = key_to_row_dict_qtype->value_qtype();
  DCHECK_NE(key_qtype, nullptr);
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DictV1Proto::extension)
      ->mutable_key_to_row_dict_qtype();
  ASSIGN_OR_RETURN(auto key_qtype_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(key_qtype)));
  value_proto.add_input_value_indices(key_qtype_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDictQType(QTypePtr dict_qtype,
                                           Encoder& encoder) {
  DCHECK(IsDictQType(dict_qtype));
  QTypePtr key_qtype = GetDictKeyQTypeOrNull(dict_qtype);
  DCHECK_NE(key_qtype, nullptr);
  QTypePtr value_qtype = GetDictValueQTypeOrNull(dict_qtype);
  DCHECK_NE(value_qtype, nullptr);
  if (key_qtype == nullptr) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "unable to infer value_qtype of %s", dict_qtype->name()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DictV1Proto::extension)->mutable_dict_qtype();
  ASSIGN_OR_RETURN(auto key_qtype_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(key_qtype)));
  ASSIGN_OR_RETURN(auto value_qtype_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(value_qtype)));
  value_proto.add_input_value_indices(key_qtype_value_index);
  value_proto.add_input_value_indices(value_qtype_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDict(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    const auto& qtype_value = value.UnsafeAs<QTypePtr>();
    if (IsKeyToRowDictQType(qtype_value)) {
      return EncodeKeyToRowDictQType(qtype_value, encoder);
    } else if (IsDictQType(qtype_value)) {
      return EncodeDictQType(qtype_value, encoder);
    }
  }
  // TODO: Support serialization for the dict values.
  return absl::UnimplementedError(absl::StrFormat(
      "%s does not support serialization of %s: %s; this may indicate a "
      "missing BUILD dependency on the encoder for this qtype",
      kDictV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          // Ensure the DENSE_ARRAY_INT64 qtype registration.
          GetDenseArrayQType<int64_t>();
          auto* key_to_row_dict_qtype = GetKeyToRowDictQType<int64_t>();
          ASSIGN_OR_RETURN(
              auto* dict_qtype,
              (GetDictQType(GetQType<int64_t>(), GetQType<int64_t>())));
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              key_to_row_dict_qtype->qtype_specialization_key(), EncodeDict));
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              dict_qtype->qtype_specialization_key(), EncodeDict));
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::serialization_codecs
