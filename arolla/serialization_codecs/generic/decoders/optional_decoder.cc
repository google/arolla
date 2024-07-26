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
#include <optional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/optional_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

TypedValue DecodeOptionalUnitValue(bool presence) {
  if (presence) {
    return TypedValue::FromValue(OptionalValue<Unit>(kUnit));
  }
  return TypedValue::FromValue(OptionalValue<Unit>(std::nullopt));
}

template <typename T, typename OptionalValueProto>
TypedValue DecodeOptionalValue(const OptionalValueProto& optional_value_proto) {
  if (optional_value_proto.has_value()) {
    return TypedValue::FromValue(
        OptionalValue<T>(T(optional_value_proto.value())));
  }
  return TypedValue::FromValue(OptionalValue<T>(std::nullopt));
}

absl::StatusOr<TypedValue> DecodeOptionalWeakFloatValue(
    const OptionalV1Proto::OptionalWeakFloatProto& optional_value_proto) {
  if (optional_value_proto.has_value()) {
    return TypedValue::FromValueWithQType(
        OptionalValue<double>(optional_value_proto.value()),
        GetOptionalWeakFloatQType());
  }
  return TypedValue::FromValueWithQType(OptionalValue<double>(std::nullopt),
                                        GetOptionalWeakFloatQType());
}

absl::StatusOr<ValueDecoderResult> DecodeOptional(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(OptionalV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& optional_proto =
      value_proto.GetExtension(OptionalV1Proto::extension);
  switch (optional_proto.value_case()) {
    case OptionalV1Proto::kOptionalUnitValue:
      return DecodeOptionalUnitValue(optional_proto.optional_unit_value());
    case OptionalV1Proto::kOptionalBooleanValue:
      return DecodeOptionalValue<bool>(optional_proto.optional_boolean_value());
    case OptionalV1Proto::kOptionalBytesValue:
      return DecodeOptionalValue<Bytes>(optional_proto.optional_bytes_value());
    case OptionalV1Proto::kOptionalTextValue:
      return DecodeOptionalValue<Text>(optional_proto.optional_text_value());
    case OptionalV1Proto::kOptionalInt32Value:
      return DecodeOptionalValue<int32_t>(
          optional_proto.optional_int32_value());
    case OptionalV1Proto::kOptionalInt64Value:
      return DecodeOptionalValue<int64_t>(
          optional_proto.optional_int64_value());
    case OptionalV1Proto::kOptionalUint64Value:
      return DecodeOptionalValue<uint64_t>(
          optional_proto.optional_uint64_value());
    case OptionalV1Proto::kOptionalFloat32Value:
      return DecodeOptionalValue<float>(
          optional_proto.optional_float32_value());
    case OptionalV1Proto::kOptionalFloat64Value:
      return DecodeOptionalValue<double>(
          optional_proto.optional_float64_value());
    case OptionalV1Proto::kOptionalWeakFloatValue:
      return DecodeOptionalWeakFloatValue(
          optional_proto.optional_weak_float_value());
    case OptionalV1Proto::kOptionalShapeValue:
      return TypedValue::FromValue(OptionalScalarShape());
    case OptionalV1Proto::kOptionalUnitQtype:
      return TypedValue::FromValue(GetOptionalQType<Unit>());
    case OptionalV1Proto::kOptionalBooleanQtype:
      return TypedValue::FromValue(GetOptionalQType<bool>());
    case OptionalV1Proto::kOptionalBytesQtype:
      return TypedValue::FromValue(GetOptionalQType<Bytes>());
    case OptionalV1Proto::kOptionalTextQtype:
      return TypedValue::FromValue(GetOptionalQType<Text>());
    case OptionalV1Proto::kOptionalInt32Qtype:
      return TypedValue::FromValue(GetOptionalQType<int32_t>());
    case OptionalV1Proto::kOptionalInt64Qtype:
      return TypedValue::FromValue(GetOptionalQType<int64_t>());
    case OptionalV1Proto::kOptionalUint64Qtype:
      return TypedValue::FromValue(GetOptionalQType<uint64_t>());
    case OptionalV1Proto::kOptionalFloat32Qtype:
      return TypedValue::FromValue(GetOptionalQType<float>());
    case OptionalV1Proto::kOptionalFloat64Qtype:
      return TypedValue::FromValue(GetOptionalQType<double>());
    case OptionalV1Proto::kOptionalWeakFloatQtype:
      return TypedValue::FromValue(GetOptionalWeakFloatQType());
    case OptionalV1Proto::kOptionalShapeQtype:
      return TypedValue::FromValue(GetQType<OptionalScalarShape>());
    case OptionalV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(optional_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kOptionalV1Codec, DecodeOptional);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
