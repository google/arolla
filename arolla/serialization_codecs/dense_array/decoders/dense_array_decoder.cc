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
#include <cstdint>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/dense_array/codec_name.h"
#include "arolla/serialization_codecs/dense_array/dense_array_codec.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_codecs {
namespace {

namespace bm = ::arolla::bitmap;

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

absl::Status CheckFieldPresence(absl::string_view field_name, bool presence) {
  if (presence) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrCat("missing field ", field_name));
}

absl::Status CheckRepeatedFieldSize(absl::string_view field_name,
                                    int64_t actual_size,
                                    int64_t expected_size) {
  if (expected_size == actual_size) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrCat("expected ", expected_size,
                                                 " items in ", field_name,
                                                 ", got ", actual_size));
}

// Deserializes and validates:
//   * dense_array_size -- number of items in dense_array
//   * bitmap -- bitmap buffer for dense_array
#define DECODE_DENSE_ARRAY_HEADER(FIELD)                                       \
  RETURN_IF_ERROR(CheckFieldPresence(#FIELD "_value.size",                     \
                                     dense_array_value_proto.has_size()));     \
  const int64_t dense_array_size = dense_array_value_proto.size();             \
  if (dense_array_size < 0) {                                                  \
    return absl::InvalidArgumentError(absl::StrCat(                            \
        "expected a non-negative value in " #FIELD "_value.size, got ",        \
        dense_array_size));                                                    \
  }                                                                            \
  bm::Bitmap bitmap;                                                           \
  if (!dense_array_value_proto.bitmap().empty()) {                             \
    const auto& bitmap_proto = dense_array_value_proto.bitmap();               \
    RETURN_IF_ERROR(CheckRepeatedFieldSize(#FIELD "_value.bitmap",             \
                                           bitmap_proto.size(),                \
                                           bm::BitmapSize(dense_array_size))); \
    bitmap = bm::Bitmap::Create(bitmap_proto.begin(), bitmap_proto.end());     \
  }

absl::StatusOr<TypedValue> DecodeDenseArrayUnitValue(
    const DenseArrayV1Proto::DenseArrayUnitProto& dense_array_value_proto) {
  DECODE_DENSE_ARRAY_HEADER(dense_array_unit)
  return TypedValue::FromValue(
      DenseArray<Unit>{VoidBuffer(dense_array_size), std::move(bitmap)});
}

#define GEN_DECODE_DENSE_ARRAY_VALUE(NAME, T, FIELD)                     \
  absl::StatusOr<TypedValue> DecodeDenseArray##NAME##Value(              \
      const std::decay_t<decltype(DenseArrayV1Proto().FIELD##_value())>& \
          dense_array_value_proto) {                                     \
    DECODE_DENSE_ARRAY_HEADER(FIELD)                                     \
    const int64_t dense_array_count =                                    \
        bm::CountBits(bitmap, 0, dense_array_size);                      \
    RETURN_IF_ERROR(CheckRepeatedFieldSize(                              \
        #FIELD "_value.values", dense_array_value_proto.values_size(),   \
        dense_array_count));                                             \
    Buffer<T>::Builder values_builder(dense_array_size);                 \
    auto values_data = values_builder.GetMutableSpan();                  \
    int64_t i = 0, j = 0;                                                \
    bm::Iterate(bitmap, 0, dense_array_size, [&](bool present) {         \
      if (present) {                                                     \
        values_data[i] = dense_array_value_proto.values(j++);            \
      }                                                                  \
      ++i;                                                               \
    });                                                                  \
    return TypedValue::FromValue(                                        \
        DenseArray<T>{std::move(values_builder).Build(dense_array_size), \
                      std::move(bitmap)});                               \
  }

GEN_DECODE_DENSE_ARRAY_VALUE(Boolean, bool, dense_array_boolean)
GEN_DECODE_DENSE_ARRAY_VALUE(Int32, int32_t, dense_array_int32)
GEN_DECODE_DENSE_ARRAY_VALUE(Int64, int64_t, dense_array_int64)
GEN_DECODE_DENSE_ARRAY_VALUE(Uint64, uint64_t, dense_array_uint64)
GEN_DECODE_DENSE_ARRAY_VALUE(Float32, float, dense_array_float32)
GEN_DECODE_DENSE_ARRAY_VALUE(Float64, double, dense_array_float64)

#undef GEN_DECODE_DENSE_ARRAY_VALUE

absl::Status CheckStringsOffsets(absl::string_view field,
                                 absl::Span<const int64_t> starts,
                                 absl::Span<const int64_t> ends, int64_t size) {
  for (size_t i = 0; i < starts.size() && i < ends.size(); ++i) {
    if (starts[i] < 0) {
      return absl::InvalidArgumentError(
          absl::StrCat("expected non-negative items in ", field,
                       "_value.value_offset_starts, got ", starts[i]));
    }
    if (starts[i] > ends[i]) {
      return absl::InvalidArgumentError(
          absl::StrCat("expected items in ", field,
                       "_value.value_offset_starts to be less-or-equal "
                       "than corresponding .value_offset_ends, got ",
                       starts[i], " greater than ", ends[i]));
    }
    if (ends[i] > size) {
      return absl::InvalidArgumentError(
          absl::StrCat("expected items in ", field,
                       "_value.value_offset_ends to be less-or-equal than "
                       ".characters size, got ",
                       ends[i], " greater than ", size));
    }
  }
  return absl::OkStatus();
}

#define GEN_DECODE_DENSE_ARRAY_STRINGS_VALUE(NAME, T, FIELD)                   \
  absl::StatusOr<TypedValue> DecodeDenseArray##NAME##Value(                    \
      const DenseArrayV1Proto::DenseArrayStringProto&                          \
          dense_array_value_proto) {                                           \
    DECODE_DENSE_ARRAY_HEADER(FIELD)                                           \
    RETURN_IF_ERROR(                                                           \
        CheckFieldPresence(#FIELD "_value.characters",                         \
                           dense_array_value_proto.has_characters()));         \
    const int64_t dense_array_count =                                          \
        bm::CountBits(bitmap, 0, dense_array_size);                            \
    RETURN_IF_ERROR(CheckRepeatedFieldSize(                                    \
        #FIELD "_value.value_offset_starts",                                   \
        dense_array_value_proto.value_offset_starts_size(),                    \
        dense_array_count));                                                   \
    RETURN_IF_ERROR(CheckRepeatedFieldSize(                                    \
        #FIELD "_value.value_offset_ends",                                     \
        dense_array_value_proto.value_offset_ends_size(), dense_array_count)); \
    RETURN_IF_ERROR(CheckStringsOffsets(                                       \
        #FIELD, dense_array_value_proto.value_offset_starts(),                 \
        dense_array_value_proto.value_offset_ends(),                           \
        dense_array_value_proto.characters().size()));                         \
    auto offsets_builder =                                                     \
        Buffer<StringsBuffer::Offsets>::Builder(dense_array_size);             \
    auto offsets_data = offsets_builder.GetMutableSpan();                      \
    int64_t i = 0, j = 0;                                                      \
    bm::Iterate(bitmap, 0, dense_array_size, [&](bool present) {               \
      if (present) {                                                           \
        offsets_data[i] = {dense_array_value_proto.value_offset_starts(j),     \
                           dense_array_value_proto.value_offset_ends(j)};      \
        ++j;                                                                   \
      } else {                                                                 \
        offsets_data[i] = {};                                                  \
      }                                                                        \
      ++i;                                                                     \
    });                                                                        \
    auto characters =                                                          \
        Buffer<char>::Create(dense_array_value_proto.characters().begin(),     \
                             dense_array_value_proto.characters().end());      \
    return TypedValue::FromValue(DenseArray<T>{                                \
        StringsBuffer(std::move(offsets_builder).Build(dense_array_size),      \
                      std::move(characters)),                                  \
        std::move(bitmap)});                                                   \
  }

GEN_DECODE_DENSE_ARRAY_STRINGS_VALUE(Bytes, Bytes, dense_array_bytes)
GEN_DECODE_DENSE_ARRAY_STRINGS_VALUE(Text, Text, dense_array_text)

#undef GEN_DECODE_DENSE_ARRAY_STRINGS_VALUE
#undef DECODE_DENSE_ARRAY_HEADER

absl::StatusOr<DenseArrayEdge::EdgeType> DecodeDenseArrayEdgeType(
    const DenseArrayV1Proto::DenseArrayEdgeProto& dense_array_edge_proto) {
  if (!dense_array_edge_proto.has_edge_type()) {
    return absl::InvalidArgumentError(
        "missing field dense_array_edge_value.edge_type");
  }
  switch (dense_array_edge_proto.edge_type()) {
    case DenseArrayV1Proto::DenseArrayEdgeProto::MAPPING:
      return DenseArrayEdge::EdgeType::MAPPING;
    case DenseArrayV1Proto::DenseArrayEdgeProto::SPLIT_POINTS:
      return DenseArrayEdge::EdgeType::SPLIT_POINTS;
    case DenseArrayV1Proto::DenseArrayEdgeProto::EDGE_TYPE_UNSPECIFIED:
      break;
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported value in dense_array_edge_value.edge_type: ",
                   dense_array_edge_proto.edge_type()));
}

absl::StatusOr<ValueDecoderResult> DecodeDenseArrayEdgeValue(
    const DenseArrayV1Proto::DenseArrayEdgeProto dense_array_edge_proto,
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 item in input_values, got %d", input_values.size()));
  }
  if (input_values[0].GetType() != GetDenseArrayQType<int64_t>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s in input_values[0], got %s",
                        GetDenseArrayQType<int64_t>()->name(),
                        input_values[0].GetType()->name()));
  }
  const auto& dense_array = input_values[0].UnsafeAs<DenseArray<int64_t>>();
  ASSIGN_OR_RETURN(DenseArrayEdge::EdgeType edge_type,
                   DecodeDenseArrayEdgeType(dense_array_edge_proto));
  // Always a valid enum since we checked in DecodeDenseArrayEdgeType:
  switch (edge_type) {
    case DenseArrayEdge::MAPPING: {
      if (!dense_array_edge_proto.has_parent_size()) {
        return absl::InvalidArgumentError(
            "missing field dense_array_edge_value.parent_size");
      }
      ASSIGN_OR_RETURN(auto dense_array_edge,
                       DenseArrayEdge::FromMapping(
                           dense_array, dense_array_edge_proto.parent_size()));
      return TypedValue::FromValue(dense_array_edge);
    }
    case DenseArrayEdge::SPLIT_POINTS: {
      if (dense_array_edge_proto.has_parent_size()) {
        return absl::InvalidArgumentError(
            "unexpected field dense_array_edge_value.parent_size");
      }
      ASSIGN_OR_RETURN(auto dense_array_edge,
                       DenseArrayEdge::FromSplitPoints(dense_array));
      return TypedValue::FromValue(dense_array_edge);
    }
  }
  return absl::InternalError("Invalid edge type");
}

absl::StatusOr<ValueDecoderResult> DecodeDenseArrayToScalarEdgeValue(
    int64_t dense_array_to_scalar_edge_value) {
  if (dense_array_to_scalar_edge_value < 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected non-negative dense_array_to_scalar_edge_value, got %d",
        dense_array_to_scalar_edge_value));
  }
  return TypedValue::FromValue(
      DenseArrayGroupScalarEdge(dense_array_to_scalar_edge_value));
}

absl::StatusOr<ValueDecoderResult> DecodeDenseArrayShapeValue(
    int64_t dense_array_shape_value) {
  if (dense_array_shape_value < 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected non-negative dense_array_shape_value, got %d",
                        dense_array_shape_value));
  }
  return TypedValue::FromValue(DenseArrayShape{dense_array_shape_value});
}

}  // namespace

absl::StatusOr<ValueDecoderResult> DecodeDenseArray(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(DenseArrayV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& dense_array_proto =
      value_proto.GetExtension(DenseArrayV1Proto::extension);
  switch (dense_array_proto.value_case()) {
#define GEN_CASE(NAME, T, FIELD)                                             \
  case DenseArrayV1Proto::kDenseArray##NAME##Value:                          \
    return DecodeDenseArray##NAME##Value(dense_array_proto.FIELD##_value()); \
  case DenseArrayV1Proto::kDenseArray##NAME##Qtype:                          \
    return TypedValue::FromValue(GetDenseArrayQType<T>());

    GEN_CASE(Unit, Unit, dense_array_unit)
    GEN_CASE(Bytes, Bytes, dense_array_bytes)
    GEN_CASE(Text, Text, dense_array_text)
    GEN_CASE(Boolean, bool, dense_array_boolean)
    GEN_CASE(Int32, int32_t, dense_array_int32)
    GEN_CASE(Int64, int64_t, dense_array_int64)
    GEN_CASE(Uint64, uint64_t, dense_array_uint64)
    GEN_CASE(Float32, float, dense_array_float32)
    GEN_CASE(Float64, double, dense_array_float64)
#undef GEN_CASE

    case DenseArrayV1Proto::kDenseArrayEdgeValue:
      return DecodeDenseArrayEdgeValue(
          dense_array_proto.dense_array_edge_value(), input_values);
    case DenseArrayV1Proto::kDenseArrayEdgeQtype:
      return TypedValue::FromValue(GetQType<DenseArrayEdge>());

    case DenseArrayV1Proto::kDenseArrayToScalarEdgeValue:
      return DecodeDenseArrayToScalarEdgeValue(
          dense_array_proto.dense_array_to_scalar_edge_value());
    case DenseArrayV1Proto::kDenseArrayToScalarEdgeQtype:
      return TypedValue::FromValue(GetQType<DenseArrayGroupScalarEdge>());
    case DenseArrayV1Proto::kDenseArrayShapeValue:
      return DecodeDenseArrayShapeValue(
          dense_array_proto.dense_array_shape_value());
    case DenseArrayV1Proto::kDenseArrayShapeQtype:
      return TypedValue::FromValue(GetQType<DenseArrayShape>());

    case DenseArrayV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "unexpected value=", static_cast<int>(dense_array_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kDenseArrayV1Codec, DecodeDenseArray);
        })

}  // namespace arolla::serialization_codecs
