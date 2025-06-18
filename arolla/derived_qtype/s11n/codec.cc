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
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/derived_qtype/labeled_qtype.h"
#include "arolla/derived_qtype/s11n/codec.pb.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

constexpr absl::string_view kCodecName =
    "arolla.serialization_codecs.DerivedQTypeV1Proto.extension";

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kCodecName));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDerivedValue(TypedRef value,
                                              Encoder& encoder) {
  DCHECK(dynamic_cast<const DerivedQTypeInterface*>(value.GetType()) !=
         nullptr);
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DerivedQTypeV1Proto::extension)
      ->mutable_derived_value();
  ASSIGN_OR_RETURN(auto base_value_index,
                   encoder.EncodeValue(TypedValue(DecayDerivedQValue(value))));
  value_proto.add_input_value_indices(base_value_index);
  ASSIGN_OR_RETURN(auto derived_qtype_index,
                   encoder.EncodeValue(TypedValue::FromValue(value.GetType())));
  value_proto.add_input_value_indices(derived_qtype_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeLabeledDerivedQType(  // clang-format hint
    QTypePtr absl_nonnull qtype, Encoder& encoder) {
  DCHECK(IsLabeledQType(qtype));
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(DerivedQTypeV1Proto::extension)
      ->mutable_labeled_qtype()
      ->set_label(GetQTypeLabel(qtype));
  ASSIGN_OR_RETURN(
      auto base_qtype_value_index,
      encoder.EncodeValue(TypedValue::FromValue(DecayDerivedQType(qtype))));
  value_proto.add_input_value_indices(base_qtype_value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> Encode(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    const auto& qtype_value = value.UnsafeAs<QTypePtr>();
    if (IsLabeledQType(qtype_value)) {
      return EncodeLabeledDerivedQType(qtype_value, encoder);
    }
  } else if (dynamic_cast<const DerivedQTypeInterface*>(value.GetType()) !=
             nullptr) {
    return EncodeDerivedValue(value, encoder);
  }
  return absl::UnimplementedError(
      absl::StrFormat("%s does not support serialization of %s: %s", kCodecName,
                      value.GetType()->name(), value.Repr()));
}

absl::StatusOr<TypedValue> DecodeLabeledQType(
    const DerivedQTypeV1Proto::LabeledQTypeProto& labeled_qtype_proto,
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a single input value, got %d; value=LABELED_QTYPE",
        input_values.size()));
  }
  if (input_values[0].GetType() != GetQTypeQType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a qtype in input_values[0], got a %s value; "
                        "value=LABELED_QTYPE",
                        input_values[0].GetType()->name()));
  }
  auto base_qtype = input_values[0].UnsafeAs<QTypePtr>();
  if (!labeled_qtype_proto.has_label()) {
    return absl::InvalidArgumentError("missing label; value=LABELED_QTYPE");
  }
  return TypedValue::FromValue(
      GetLabeledQType(base_qtype, labeled_qtype_proto.label()));
}

absl::StatusOr<TypedValue> DecodeDerivedValue(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 2) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected two input values, got %d; value=DERIVED_VALUE",
        input_values.size()));
  }
  if (input_values[1].GetType() != GetQTypeQType()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a derived qtype in input_values[1], got a %s value; "
        "value=DERIVED_VALUE",
        input_values[1].GetType()->name()));
  }
  auto derived_qtype = input_values[1].UnsafeAs<QTypePtr>();
  auto base_qtype = DecayDerivedQType(derived_qtype);
  if (base_qtype == derived_qtype) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a derived qtype in input_values[1], got %s; "
                        "value=DERIVED_VALUE",
                        derived_qtype->name()));
  }
  if (input_values[0].GetType() != base_qtype) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a value of type %s in input_values[0], got %s; "
        "value=DERIVED_VALUE",
        base_qtype->name(), input_values[0].GetType()->name()));
  }
  return UnsafeDowncastDerivedQValue(derived_qtype, input_values[0]);
}

absl::StatusOr<ValueDecoderResult> Decode(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr absl_nonnull> input_exprs) {
  if (!value_proto.HasExtension(DerivedQTypeV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& derived_qtype_proto =
      value_proto.GetExtension(DerivedQTypeV1Proto::extension);
  switch (derived_qtype_proto.value_case()) {
    case DerivedQTypeV1Proto::kDerivedValue:
      return DecodeDerivedValue(input_values);

    case DerivedQTypeV1Proto::kLabeledQtype:
      return DecodeLabeledQType(derived_qtype_proto.labeled_qtype(),
                                input_values);

    case DerivedQTypeV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("unexpected value=%d",
                      static_cast<int>(derived_qtype_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              GetLabeledQTypeSpecializationKey(), Encode));
          RETURN_IF_ERROR(RegisterValueDecoder(kCodecName, Decode));
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::serialization_codecs
