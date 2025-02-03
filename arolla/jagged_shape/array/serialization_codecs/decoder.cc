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
#include "arolla/serialization_base/decoder.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/jagged_shape/array/jagged_shape.h"
#include "arolla/jagged_shape/array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/jagged_shape/array/serialization_codecs/codec_name.h"
#include "arolla/jagged_shape/array/serialization_codecs/jagged_shape_codec.pb.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::JaggedArrayShapeV1Proto;

absl::StatusOr<ValueDecoderResult> DecodeJaggedArrayShapeValue(
    absl::Span<const TypedValue> input_values) {
  JaggedArrayShape::EdgeVec edges;
  edges.reserve(input_values.size());
  for (const auto& value : input_values) {
    ASSIGN_OR_RETURN(edges.emplace_back(), value.As<JaggedArrayShape::Edge>());
  }
  ASSIGN_OR_RETURN(auto jagged_shape,
                   JaggedArrayShape::FromEdges(std::move(edges)));
  return TypedValue::FromValue(std::move(jagged_shape));
}

absl::StatusOr<ValueDecoderResult> DecodeJaggedArrayShape(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const expr::ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(JaggedArrayShapeV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& jagged_shape_proto =
      value_proto.GetExtension(JaggedArrayShapeV1Proto::extension);
  switch (jagged_shape_proto.value_case()) {
    case JaggedArrayShapeV1Proto::kJaggedArrayShapeQtype:
      return TypedValue::FromValue(GetQType<JaggedArrayShape>());
    case JaggedArrayShapeV1Proto::kJaggedArrayShapeValue:
      return DecodeJaggedArrayShapeValue(input_values);
    case JaggedArrayShapeV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("unexpected value=%d",
                      static_cast<int>(jagged_shape_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kJaggedArrayShapeV1Codec,
                                      DecodeJaggedArrayShape);
        })

}  // namespace
}  // namespace arolla::serialization_codecs
