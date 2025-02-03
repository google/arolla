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

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/generic/codec_name.h"
#include "arolla/serialization_codecs/generic/scalar_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprQuote;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueDecoderResult> DecodeScalar(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(ScalarV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& scalar_proto = value_proto.GetExtension(ScalarV1Proto::extension);
  switch (scalar_proto.value_case()) {
    case ScalarV1Proto::kUnitValue:
      return TypedValue::FromValue<Unit>(kUnit);
    case ScalarV1Proto::kBooleanValue:
      return TypedValue::FromValue<bool>(scalar_proto.boolean_value());
    case ScalarV1Proto::kBytesValue:
      return TypedValue::FromValue<Bytes>(Bytes(scalar_proto.bytes_value()));
    case ScalarV1Proto::kTextValue:
      return TypedValue::FromValue<Text>(Text(scalar_proto.text_value()));
    case ScalarV1Proto::kInt32Value:
      return TypedValue::FromValue<int32_t>(scalar_proto.int32_value());
    case ScalarV1Proto::kInt64Value:
      return TypedValue::FromValue<int64_t>(scalar_proto.int64_value());
    case ScalarV1Proto::kUint64Value:
      return TypedValue::FromValue<uint64_t>(scalar_proto.uint64_value());
    case ScalarV1Proto::kFloat32Value:
      return TypedValue::FromValue<float>(scalar_proto.float32_value());
    case ScalarV1Proto::kFloat64Value:
      return TypedValue::FromValue<double>(scalar_proto.float64_value());
    case ScalarV1Proto::kWeakFloatValue:
      return TypedValue::FromValueWithQType(scalar_proto.weak_float_value(),
                                            GetWeakFloatQType());
    case ScalarV1Proto::kScalarToScalarEdgeValue:
      return TypedValue::FromValue(ScalarToScalarEdge());
    case ScalarV1Proto::kScalarShapeValue:
      return TypedValue::FromValue(ScalarShape());
    case ScalarV1Proto::kUnspecifiedValue:
      return GetUnspecifiedQValue();
    case ScalarV1Proto::kExprQuoteValue:
      if (input_exprs.size() != 1) {
        return absl::InvalidArgumentError(
            absl::StrFormat("ExprQuote requires exactly one input expr, got %d",
                            input_exprs.size()));
      }
      return TypedValue::FromValue(ExprQuote{input_exprs[0]});
    case ScalarV1Proto::kUnitQtype:
      return TypedValue::FromValue(GetQType<Unit>());
    case ScalarV1Proto::kBooleanQtype:
      return TypedValue::FromValue(GetQType<bool>());
    case ScalarV1Proto::kBytesQtype:
      return TypedValue::FromValue(GetQType<Bytes>());
    case ScalarV1Proto::kTextQtype:
      return TypedValue::FromValue(GetQType<Text>());
    case ScalarV1Proto::kInt32Qtype:
      return TypedValue::FromValue(GetQType<int32_t>());
    case ScalarV1Proto::kInt64Qtype:
      return TypedValue::FromValue(GetQType<int64_t>());
    case ScalarV1Proto::kUint64Qtype:
      return TypedValue::FromValue(GetQType<uint64_t>());
    case ScalarV1Proto::kFloat32Qtype:
      return TypedValue::FromValue(GetQType<float>());
    case ScalarV1Proto::kFloat64Qtype:
      return TypedValue::FromValue(GetQType<double>());
    case ScalarV1Proto::kWeakFloatQtype:
      return TypedValue::FromValue(GetWeakFloatQType());
    case ScalarV1Proto::kScalarToScalarEdgeQtype:
      return TypedValue::FromValue(GetQType<ScalarToScalarEdge>());
    case ScalarV1Proto::kScalarShapeQtype:
      return TypedValue::FromValue(GetQType<ScalarShape>());
    case ScalarV1Proto::kUnspecifiedQtype:
      return TypedValue::FromValue(GetUnspecifiedQType());
    case ScalarV1Proto::kExprQuoteQtype:
      return TypedValue::FromValue(GetQType<ExprQuote>());
    case ScalarV1Proto::kQtypeQtype:
      return TypedValue::FromValue(
          scalar_proto.qtype_qtype() ? GetQTypeQType() : GetNothingQType());
    case ScalarV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(scalar_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kScalarV1Codec, DecodeScalar);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
