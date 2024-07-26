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
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/slice_qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/tuple_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<TypedValue> DecodeTupleQType(
    absl::Span<const TypedValue> input_values) {
  std::vector<QTypePtr> field_qtypes;
  field_qtypes.reserve(input_values.size());
  for (const auto& input_value : input_values) {
    if (input_value.GetType() != GetQType<QTypePtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a qtype, got a %s value as an input; value=TUPLE_QTYPE",
          input_value.GetType()->name()));
    }
    field_qtypes.push_back(input_value.UnsafeAs<QTypePtr>());
  }
  return TypedValue::FromValue(MakeTupleQType(field_qtypes));
}

absl::StatusOr<TypedValue> DecodeNamedTuple(
    const TupleV1Proto::NamedTupleValueProto& namedtuple_value_proto,
    absl::Span<const TypedValue> input_values) {
  const google::protobuf::RepeatedPtrField<std::string>& field_names_proto =
      namedtuple_value_proto.field_names();
  std::vector<std::string> field_names(field_names_proto.begin(),
                                       field_names_proto.end());
  ASSIGN_OR_RETURN(auto result, MakeNamedTuple(field_names, input_values),
                   _ << "value=NAMEDTUPLE");
  return result;
}

absl::StatusOr<TypedValue> DecodeNamedTupleQType(
    const TupleV1Proto::NamedTupleQTypeProto& namedtuple_qtype_proto,
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a single input value, got %d; value=NAMEDTUPLE_QTYPE",
        input_values.size()));
  }
  if (input_values[0].GetType() != GetQType<QTypePtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a qtype, got a %s value as an input; value=NAMEDTUPLE_QTYPE",
        input_values[0].GetType()->name()));
  }
  auto* tuple_qtype = input_values[0].UnsafeAs<QTypePtr>();
  if (!IsTupleQType(tuple_qtype)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a tuple qtype, got %s as an input; value=NAMEDTUPLE_QTYPE",
        tuple_qtype->name()));
  }
  const google::protobuf::RepeatedPtrField<std::string>& field_names_proto =
      namedtuple_qtype_proto.field_names();
  std::vector<std::string> field_names(field_names_proto.begin(),
                                       field_names_proto.end());
  ASSIGN_OR_RETURN(QTypePtr namedtuple_qtype,
                   MakeNamedTupleQType(field_names, tuple_qtype),
                   _ << "value=NAMEDTUPLE_QTYPE");
  return TypedValue::FromValue(namedtuple_qtype);
}

absl::StatusOr<TypedValue> DecodeSliceQType(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a single input value, got %d; value=SLICE_QTYPE",
        input_values.size()));
  }
  ASSIGN_OR_RETURN(auto tuple_qtype, input_values[0].As<QTypePtr>(),
                   _ << "value=SLICE_QTYPE");
  const auto& type_fields = tuple_qtype.get()->type_fields();
  if (type_fields.size() != 3) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 3 qtypes (start, stop, step), got %d; value=SLICE_QTYPE",
        type_fields.size()));
  }
  QTypePtr slice_qtype =
      MakeSliceQType(type_fields[0].GetType(), type_fields[1].GetType(),
                     type_fields[2].GetType());
  return TypedValue::FromValue(slice_qtype);
}

absl::StatusOr<TypedValue> DecodeSliceValue(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a single input value, got %d; value=SLICE",
                        input_values.size()));
  }
  const TypedValue& tpl = input_values[0];
  if (!IsTupleQType(tpl.GetType()) || tpl.GetFieldCount() != 3) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a 3-tuple (start, stop, step), got %s; value=SLICE",
        tpl.Repr()));
  }
  auto slice_qtype =
      MakeSliceQType(tpl.GetField(0).GetType(), tpl.GetField(1).GetType(),
                     tpl.GetField(2).GetType());
  return UnsafeDowncastDerivedQValue(slice_qtype, tpl);
}

absl::StatusOr<ValueDecoderResult> DecodeTuple(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(TupleV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& tuple_proto = value_proto.GetExtension(TupleV1Proto::extension);
  switch (tuple_proto.value_case()) {
    case TupleV1Proto::kTupleValue:
      return MakeTuple(input_values);

    case TupleV1Proto::kTupleQtype:
      return DecodeTupleQType(input_values);

    case TupleV1Proto::kNamedtupleValue:
      return DecodeNamedTuple(tuple_proto.namedtuple_value(), input_values);

    case TupleV1Proto::kNamedtupleQtype:
      return DecodeNamedTupleQType(tuple_proto.namedtuple_qtype(),
                                   input_values);

    case TupleV1Proto::kSliceQtype:
      return DecodeSliceQType(input_values);

    case TupleV1Proto::kSliceValue:
      return DecodeSliceValue(input_values);

    case TupleV1Proto::VALUE_NOT_SET: {
      return absl::InvalidArgumentError("missing value");
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(tuple_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kTupleV1Codec, &DecodeTuple);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
