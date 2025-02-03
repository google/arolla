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
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/slice_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/tuple_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kTupleV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeTupleValue(TypedRef value, Encoder& encoder) {
  DCHECK(IsTupleQType(value.GetType()));  // It's safe because we have already
                                          // done such check in EncodeTuple().
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(TupleV1Proto::extension)->set_tuple_value(true);
  for (int64_t i = 0; i < value.GetFieldCount(); ++i) {
    const auto field = value.GetField(i);
    ASSIGN_OR_RETURN(auto value_index, encoder.EncodeValue(TypedValue(field)));
    value_proto.add_input_value_indices(value_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeNamedTupleValue(TypedRef value,
                                                 Encoder& encoder) {
  DCHECK(IsNamedTupleQType(value.GetType()));
  ASSIGN_OR_RETURN(ValueProto value_proto, GenValueProto(encoder));
  TupleV1Proto::NamedTupleValueProto* namedtuple_value_proto =
      value_proto.MutableExtension(TupleV1Proto::extension)
          ->mutable_namedtuple_value();
  const auto field_names = GetFieldNames(value.GetType());
  namedtuple_value_proto->mutable_field_names()->Assign(field_names.begin(),
                                                        field_names.end());
  for (int64_t i = 0; i < value.GetFieldCount(); ++i) {
    ASSIGN_OR_RETURN(auto value_index,
                     encoder.EncodeValue(TypedValue(value.GetField(i))));
    value_proto.add_input_value_indices(value_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeTupleQType(QTypePtr tuple_qtype,
                                            Encoder& encoder) {
  DCHECK(IsTupleQType(tuple_qtype));
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(TupleV1Proto::extension)->set_tuple_qtype(true);
  value_proto.mutable_input_value_indices()->Reserve(
      tuple_qtype->type_fields().size());
  for (const auto& field : tuple_qtype->type_fields()) {
    ASSIGN_OR_RETURN(
        auto value_index,
        encoder.EncodeValue(TypedValue::FromValue(field.GetType())));
    value_proto.add_input_value_indices(value_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeNamedTupleQType(QTypePtr namedtuple_qtype,
                                                 Encoder& encoder) {
  DCHECK(IsNamedTupleQType(namedtuple_qtype));
  const auto* tuple_qtype = DecayDerivedQType(namedtuple_qtype);
  if (!IsTupleQType(tuple_qtype)) {
    return absl::FailedPreconditionError(
        "expected %s to be a derived qtype from tuple");
  }
  ASSIGN_OR_RETURN(ValueProto value_proto, GenValueProto(encoder));
  ASSIGN_OR_RETURN(auto tuple_qtype_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(tuple_qtype)));
  value_proto.add_input_value_indices(tuple_qtype_value_index);
  TupleV1Proto::NamedTupleQTypeProto* namedtuple_qtype_proto =
      value_proto.MutableExtension(TupleV1Proto::extension)
          ->mutable_namedtuple_qtype();
  const auto field_names = GetFieldNames(namedtuple_qtype);
  namedtuple_qtype_proto->mutable_field_names()->Assign(field_names.begin(),
                                                        field_names.end());
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeSliceQType(QTypePtr slice_qtype,
                                            Encoder& encoder) {
  DCHECK(IsSliceQType(slice_qtype));
  const auto* tuple_qtype = DecayDerivedQType(slice_qtype);
  DCHECK(IsTupleQType(tuple_qtype));
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(TupleV1Proto::extension)->set_slice_qtype(true);
  ASSIGN_OR_RETURN(auto tuple_qtype_value_index,
                   encoder.EncodeValue(TypedValue::FromValue(tuple_qtype)));
  value_proto.add_input_value_indices(tuple_qtype_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeSliceValue(TypedRef value, Encoder& encoder) {
  DCHECK(IsSliceQType(value.GetType()));
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(TupleV1Proto::extension)->set_slice_value(true);
  ASSIGN_OR_RETURN(auto tuple_value_index,
                   encoder.EncodeValue(TypedValue(DecayDerivedQValue(value))));
  value_proto.add_input_value_indices(tuple_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeTuple(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    const auto& qtype_value = value.UnsafeAs<QTypePtr>();
    if (IsTupleQType(qtype_value)) {
      return EncodeTupleQType(qtype_value, encoder);
    } else if (IsNamedTupleQType(qtype_value)) {
      return EncodeNamedTupleQType(qtype_value, encoder);
    } else if (IsSliceQType(qtype_value)) {
      return EncodeSliceQType(qtype_value, encoder);
    }
  } else if (IsTupleQType(value.GetType())) {
    return EncodeTupleValue(value, encoder);
  } else if (IsNamedTupleQType(value.GetType())) {
    return EncodeNamedTupleValue(value, encoder);
  } else if (IsSliceQType(value.GetType())) {
    return EncodeSliceValue(value, encoder);
  }
  return absl::UnimplementedError(
      absl::StrFormat("%s does not support serialization of %s: %s",
                      kTupleV1Codec, value.GetType()->name(), value.Repr()));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          const auto* tuple_qtype = MakeTupleQType({});
          ASSIGN_OR_RETURN(const auto* namedtuple_qtype,
                           MakeNamedTupleQType({}, tuple_qtype));
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              tuple_qtype->qtype_specialization_key(), EncodeTuple));
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              namedtuple_qtype->qtype_specialization_key(), EncodeTuple));
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              GetSliceQTypeSpecializationKey(), EncodeTuple));
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::serialization_codecs
