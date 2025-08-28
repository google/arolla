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
#include <cstdint>
#include <optional>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/objects/object_qtype.h"
#include "arolla/objects/s11n/codec.pb.h"
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

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::ObjectsV1Proto;

inline constexpr absl::string_view kObjectV1Codec =
    "arolla.serialization_codecs.ObjectsV1Proto.extension";

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kObjectV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

// --- Encoders ---

absl::StatusOr<ValueProto> EncodeObject(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQTypeQType()) {
    DCHECK_EQ(value.UnsafeAs<QTypePtr>(), GetQType<Object>());
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
    value_proto.MutableExtension(ObjectsV1Proto::extension)
        ->set_object_qtype(true);
    return value_proto;
  }
  if (value.GetType() != GetQType<Object>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s does not support serialization of %s: %s",
                        kObjectV1Codec, value.GetType()->name(), value.Repr()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));

  const auto& object = value.UnsafeAs<Object>();
  auto* object_proto = value_proto.MutableExtension(ObjectsV1Proto::extension)
                           ->mutable_object_value();
  object_proto->mutable_keys()->Reserve(object.attributes().size());
  for (const auto* pair : object.GetSortedAttributes()) {
    object_proto->add_keys(pair->first);
    ASSIGN_OR_RETURN(int64_t value_index, encoder.EncodeValue(pair->second));
    value_proto.add_input_value_indices(value_index);
  }
  if (object.prototype().has_value()) {
    ASSIGN_OR_RETURN(
        int64_t prototype_index,
        encoder.EncodeValue(TypedValue::FromValue(*object.prototype())));
    value_proto.add_input_value_indices(prototype_index);
  }
  return value_proto;
}

// --- Decoders ---

absl::StatusOr<ValueDecoderResult> DecodeObjectValue(
    const ObjectsV1Proto::ObjectProto& object_proto,
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != object_proto.keys_size() &&
      input_values.size() != object_proto.keys_size() + 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected input_value.size==keys_size() (+1), got input_value.size=%d, "
        "keys_size=%d; value=OBJECT",
        input_values.size(), object_proto.keys_size()));
  }
  Object::Attributes attributes;
  attributes.reserve(object_proto.keys_size());
  for (int i = 0; i < object_proto.keys_size(); ++i) {
    auto [_, inserted] =
        attributes.emplace(object_proto.keys(i), input_values[i]);
    if (!inserted) {
      return absl::InvalidArgumentError(
          absl::StrFormat("duplicate key='%s'; value=OBJECT",
                          absl::Utf8SafeCHexEscape(object_proto.keys(i))));
    }
  }
  std::optional<Object> prototype;
  if (input_values.size() > object_proto.keys_size()) {
    if (input_values.back().GetType() != GetQType<Object>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected prototype to be %s, got %s; value=OBJECT",
          GetQType<Object>()->name(), input_values.back().GetType()->name()));
    }
    prototype = input_values.back().UnsafeAs<Object>();
  }
  return TypedValue::FromValue(
      Object(std::move(attributes), std::move(prototype)));
}

absl::StatusOr<ValueDecoderResult> DecodeObject(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const expr::ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(ObjectsV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& object_proto =
      value_proto.GetExtension(ObjectsV1Proto::extension);
  switch (object_proto.value_case()) {
    case ObjectsV1Proto::kObjectQtype:
      return TypedValue::FromValue(GetQType<Object>());
    case ObjectsV1Proto::kObjectValue:
      return DecodeObjectValue(object_proto.object_value(), input_values);
    case ObjectsV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(object_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(
              RegisterValueEncoderByQType(GetQType<Object>(), EncodeObject));
          RETURN_IF_ERROR(RegisterValueDecoder(kObjectV1Codec, DecodeObject));
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::serialization_codecs
