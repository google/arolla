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
#include <cstddef>
#include <cstdint>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/s11n/codec.pb.h"
#include "arolla/sequence/sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::SequenceV1Proto;

inline constexpr absl::string_view kSequenceV1Codec =
    "arolla.serialization_codecs.SequenceV1Proto.extension";

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kSequenceV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

// --- Encoders ---

absl::StatusOr<ValueProto> EncodeSequenceQType(TypedRef value,
                                               Encoder& encoder) {
  // Note: Safe since this function is only called for QTypes.
  const auto& qtype = value.UnsafeAs<QTypePtr>();
  if (!IsSequenceQType(qtype)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s does not support serialization of %s",
                        kSequenceV1Codec, qtype->name()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(SequenceV1Proto::extension)
      ->set_sequence_qtype(true);
  ASSIGN_OR_RETURN(
      auto value_qtype_value_index,
      encoder.EncodeValue(TypedValue::FromValue(qtype->value_qtype())));
  value_proto.add_input_value_indices(value_qtype_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeSequenceValue(TypedRef value,
                                               Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(SequenceV1Proto::extension)
      ->set_sequence_value(true);
  // Note: Safe since this function is only called for Sequences.
  const auto& seq = value.UnsafeAs<Sequence>();
  ASSIGN_OR_RETURN(
      int64_t qtype_index,
      encoder.EncodeValue(TypedValue::FromValue(seq.value_qtype())));
  value_proto.add_input_value_indices(qtype_index);
  for (size_t i = 0; i < seq.size(); ++i) {
    ASSIGN_OR_RETURN(int64_t value_index,
                     encoder.EncodeValue(TypedValue(seq.GetRef(i))));
    value_proto.add_input_value_indices(value_index);
  }
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeSequence(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    return EncodeSequenceQType(value, encoder);
  } else if (IsSequenceQType(value.GetType())) {
    return EncodeSequenceValue(value, encoder);
  } else {
    return absl::InvalidArgumentError(absl::StrFormat(
        "%s does not support serialization of %s: %s", kSequenceV1Codec,
        value.GetType()->name(), value.Repr()));
  }
}

// --- Decoders ---

absl::StatusOr<ValueDecoderResult> DecodeSequenceValue(
    absl::Span<const TypedValue> input_values) {
  if (input_values.empty()) {
    return absl::InvalidArgumentError(
        "expected non-empty input_values; value=SEQUENCE_VALUE");
  }
  if (input_values[0].GetType() != GetQTypeQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a qtype, got input_values[0].qtype=%s; "
                        "value=SEQUENCE_VALUE",
                        input_values[0].GetType()->name()));
  }
  const QTypePtr& value_qtype = input_values[0].UnsafeAs<QTypePtr>();
  ASSIGN_OR_RETURN(auto mut_seq,
                   MutableSequence::Make(value_qtype, input_values.size() - 1));
  for (size_t i = 1; i < input_values.size(); ++i) {
    if (input_values[i].GetType() != value_qtype) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected %s, got input_values[%d].qtype=%s; value=SEQUENCE_VALUE",
          value_qtype->name(), i, input_values[i].GetType()->name()));
    }
    mut_seq.UnsafeSetRef(i - 1, input_values[i].AsRef());
  }
  return TypedValue::FromValueWithQType(std::move(mut_seq).Finish(),
                                        GetSequenceQType(value_qtype));
}

absl::StatusOr<TypedValue> DecodeSequenceQType(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected input_value_indices.size=1, got %d; value=SEQUENCE_QTYPE",
        input_values.size()));
  }
  if (input_values[0].GetType() != GetQTypeQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a qtype, got input_values[0].qtype=%s; "
                        "value=SEQUENCE_QTYPE",
                        input_values[0].GetType()->name()));
  }
  const QTypePtr& value_qtype = input_values[0].UnsafeAs<QTypePtr>();
  return TypedValue::FromValue(GetSequenceQType(value_qtype));
}

absl::StatusOr<ValueDecoderResult> DecodeSequence(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const expr::ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(SequenceV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& sequence_proto =
      value_proto.GetExtension(SequenceV1Proto::extension);
  switch (sequence_proto.value_case()) {
    case SequenceV1Proto::kSequenceQtype:
      return DecodeSequenceQType(input_values);
    case SequenceV1Proto::kSequenceValue:
      return DecodeSequenceValue(input_values);
    case SequenceV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(sequence_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              GetSequenceQType<QTypePtr>()->qtype_specialization_key(),
              EncodeSequence));
          RETURN_IF_ERROR(
              RegisterValueDecoder(kSequenceV1Codec, DecodeSequence));
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::serialization_codecs
