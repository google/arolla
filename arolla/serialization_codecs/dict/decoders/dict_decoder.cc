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

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/dict/codec_name.h"
#include "arolla/serialization_codecs/dict/dict_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoder;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<TypedValue> DecodeKeyToRowDictQType(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected input_value_indices.size=1, got %d; "
                        "value=KEY_TO_ROW_DICT_QTYPE",
                        input_values.size()));
  }
  if (input_values[0].GetType() != GetQType<QTypePtr>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a qtype, got input_values[0].qtype=%s; "
                        "value=KEY_TO_ROW_DICT_QTYPE",
                        input_values[0].GetType()->name()));
  }
  const QTypePtr& key_qtype = input_values[0].UnsafeAs<QTypePtr>();
  ASSIGN_OR_RETURN(QTypePtr key_to_row_dict_qtype,
                   GetKeyToRowDictQType(key_qtype),
                   _ << "value=KEY_TO_ROW_DICT_QTYPE");
  return TypedValue::FromValue(key_to_row_dict_qtype);
}

absl::StatusOr<TypedValue> DecodeDictQType(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 2) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected input_value_indices.size=2, got %d; value=DICT_QTYPE",
        input_values.size()));
  }
  for (size_t i = 0; i < input_values.size(); ++i) {
    if (input_values[i].GetType() != GetQType<QTypePtr>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a qtype, got input_values[%d].qtype=%s; "
                          "value=DICT_QTYPE",
                          i, input_values[i].GetType()->name()));
    }
  }
  const QTypePtr& key_qtype = input_values[0].UnsafeAs<QTypePtr>();
  const QTypePtr& value_qtype = input_values[1].UnsafeAs<QTypePtr>();
  ASSIGN_OR_RETURN(QTypePtr dict_qtype, GetDictQType(key_qtype, value_qtype),
                   _ << "value=DICT_QTYPE");
  return TypedValue::FromValue(dict_qtype);
}

absl::StatusOr<ValueDecoderResult> DecodeDict(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> /*input_exprs*/) {
  if (!value_proto.HasExtension(DictV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& dict_proto = value_proto.GetExtension(DictV1Proto::extension);
  switch (dict_proto.value_case()) {
    case DictV1Proto::kKeyToRowDictQtype:
      return DecodeKeyToRowDictQType(input_values);
    case DictV1Proto::kDictQtype:
      return DecodeDictQType(input_values);
    case DictV1Proto::VALUE_NOT_SET: {
      return absl::InvalidArgumentError("missing value");
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(dict_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kDictV1Codec, DecodeDict);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
