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
#include <algorithm>
#include <cstdint>
#include <functional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/id_filter.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/array/array_codec.pb.h"
#include "arolla/serialization_codecs/array/codec_name.h"
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

absl::StatusOr<int64_t> DecodeArraySize(
    absl::string_view field_name, const ArrayV1Proto::ArrayProto& array_proto) {
  if (!array_proto.has_size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("missing field %s.size", field_name));
  }
  if (array_proto.size() < 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected non-negative %s.size, got %d", field_name,
                        array_proto.size()));
  }
  return array_proto.size();
}

absl::StatusOr<Buffer<int64_t>> DecodeArrayIds(
    absl::string_view field_name, int64_t expected_size,
    int64_t expected_id_limit, const ArrayV1Proto::ArrayProto& array_proto) {
  if (array_proto.ids_size() != expected_size) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %d items in %s.ids, got %d", expected_size,
                        field_name, array_proto.ids_size()));
  }
  auto result = Buffer<int64_t>::Create(array_proto.ids().begin(),
                                        array_proto.ids().end());
  if (!result.empty()) {
    if (std::adjacent_find(result.begin(), result.end(),
                           std::greater_equal<>()) != result.end()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a strictly monotonic sequence in %s.ids", field_name));
    }
    if (result[0] < 0) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected %s.ids[0] to be non-negative, got %d",
                          field_name, result[0]));
    }
    if (result[expected_size - 1] >= expected_id_limit) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected %s.ids[last] to be less-than %d, got %d", field_name,
          expected_id_limit, result[expected_size - 1]));
    }
  }
  return result;
}

template <typename T>
absl::StatusOr<ValueDecoderResult> DecodeArrayValue(
    absl::string_view field_name, const ArrayV1Proto::ArrayProto array_proto,
    absl::Span<const TypedValue> input_values) {
  ASSIGN_OR_RETURN(int64_t size, DecodeArraySize(field_name, array_proto));
  if (size == 0) {
    // Empty.
    if (!input_values.empty()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected no input_values, got %d", input_values.size()));
    }
    if (!array_proto.ids().empty()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected no %s.ids, got %d", field_name, array_proto.ids_size()));
    }
    return TypedValue::FromValue(Array<T>());
  }
  if (input_values.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s in input_values[0], got no value",
                        GetDenseArrayQType<T>()->name()));
  }
  if (input_values[0].GetType() != GetDenseArrayQType<T>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected %s in input_values[0], got %s",
        GetDenseArrayQType<T>()->name(), input_values[0].GetType()->name()));
  }
  const auto& dense_data = input_values[0].UnsafeAs<DenseArray<T>>();
  if (dense_data.size() > size) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected size of input_values[0] to be less-or-equal than %d, got %d",
        size, dense_data.size()));
  }

  if (dense_data.size() == size) {
    // Dense.
    if (input_values.size() != 1) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected 1 item in input_values, got %d", input_values.size()));
    }
    if (!array_proto.ids().empty()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected no %s.ids", field_name));
    }
    return TypedValue::FromValue(Array<T>(dense_data));
  }

  // Const or Sparse.
  if (input_values.size() < 2) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected %s in input_values[1], got no value",
                        GetOptionalQType<T>()->name()));
  }
  if (input_values[1].GetType() != GetOptionalQType<T>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected %s in input_values[1], got %s", GetOptionalQType<T>()->name(),
        input_values[1].GetType()->name()));
  }
  const auto& missing_id_value = input_values[1].UnsafeAs<OptionalValue<T>>();
  if (input_values.size() != 2) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 2 items in input_values, got %d", input_values.size()));
  }
  ASSIGN_OR_RETURN(
      Buffer<int64_t> ids,
      DecodeArrayIds(field_name, dense_data.size(), size, array_proto));
  return TypedValue::FromValue(
      Array<T>(size, IdFilter(size, ids), dense_data, missing_id_value));
}

absl::StatusOr<ArrayEdge::EdgeType> DecodeArrayEdgeType(
    const ArrayV1Proto::ArrayEdgeProto& array_edge_proto) {
  if (!array_edge_proto.has_edge_type()) {
    return absl::InvalidArgumentError(
        "missing field array_edge_value.edge_type");
  }
  switch (array_edge_proto.edge_type()) {
    case ArrayV1Proto::ArrayEdgeProto::MAPPING:
      return ArrayEdge::EdgeType::MAPPING;
    case ArrayV1Proto::ArrayEdgeProto::SPLIT_POINTS:
      return ArrayEdge::EdgeType::SPLIT_POINTS;
    case ArrayV1Proto::ArrayEdgeProto::EDGE_TYPE_UNSPECIFIED:
      break;
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "unknown ArrayEdge edge type: ", array_edge_proto.edge_type()));
}

absl::StatusOr<int64_t> DecodeArrayEdgeGroupSize(
    const ArrayV1Proto::ArrayEdgeProto& array_edge_proto) {
  if (!array_edge_proto.has_parent_size()) {
    return absl::InvalidArgumentError(
        "missing field array_edge_value.parent_size");
  }
  if (array_edge_proto.parent_size() < 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected non-negative array_edge_value.parent_size, got %d",
        array_edge_proto.parent_size()));
  }
  return array_edge_proto.parent_size();
}

absl::StatusOr<ValueDecoderResult> DecodeArrayEdgeValue(
    const ArrayV1Proto::ArrayEdgeProto array_edge_proto,
    absl::Span<const TypedValue> input_values) {
  ASSIGN_OR_RETURN(ArrayEdge::EdgeType edge_type,
                   DecodeArrayEdgeType(array_edge_proto));
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 item in input_values, got %d", input_values.size()));
  }
  if (input_values[0].GetType() != GetArrayQType<int64_t>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected %s in input_values[0], got %s",
        GetArrayQType<int64_t>()->name(), input_values[0].GetType()->name()));
  }
  const auto& array = input_values[0].UnsafeAs<Array<int64_t>>();
  // Always a valid enum since we checked in DecodeArrayEdgeType:
  switch (edge_type) {
    case ArrayEdge::MAPPING: {
      ASSIGN_OR_RETURN(const int64_t parent_size,
                       DecodeArrayEdgeGroupSize(array_edge_proto));
      ASSIGN_OR_RETURN(auto array_edge,
                       ArrayEdge::FromMapping(array, parent_size));
      return TypedValue::FromValue(array_edge);
    }
    case ArrayEdge::SPLIT_POINTS: {
      ASSIGN_OR_RETURN(auto array_edge, ArrayEdge::FromSplitPoints(array));
      return TypedValue::FromValue(array_edge);
    }
  }
  return absl::InternalError("Invalid edge type");
}

absl::StatusOr<ValueDecoderResult> DecodeArrayToScalarEdgeValue(
    int64_t array_to_scalar_edge_value) {
  if (array_to_scalar_edge_value < 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected non-negative array_to_scalar_edge_value, got %d",
        array_to_scalar_edge_value));
  }
  return TypedValue::FromValue(
      ArrayGroupScalarEdge(array_to_scalar_edge_value));
}

absl::StatusOr<ValueDecoderResult> DecodeArrayShapeValue(
    int64_t array_shape_value) {
  if (array_shape_value < 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected non-negative array_shape_value, got %d", array_shape_value));
  }
  return TypedValue::FromValue(ArrayShape{array_shape_value});
}

}  // namespace

absl::StatusOr<ValueDecoderResult> DecodeArray(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> /*input_exprs*/) {
  if (!value_proto.HasExtension(ArrayV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& array_proto = value_proto.GetExtension(ArrayV1Proto::extension);
  switch (array_proto.value_case()) {
#define GEN_CASE(NAME, T, FIELD)                                             \
  case ArrayV1Proto::kArray##NAME##Value:                                    \
    return DecodeArrayValue<T>(#FIELD "_value", array_proto.FIELD##_value(), \
                               input_values);                                \
  case ArrayV1Proto::kArray##NAME##Qtype:                                    \
    return TypedValue::FromValue(GetArrayQType<T>());

    GEN_CASE(Unit, Unit, array_unit)
    GEN_CASE(Bytes, Bytes, array_bytes)
    GEN_CASE(Text, Text, array_text)
    GEN_CASE(Boolean, bool, array_boolean)
    GEN_CASE(Int32, int32_t, array_int32)
    GEN_CASE(Int64, int64_t, array_int64)
    GEN_CASE(Uint64, uint64_t, array_uint64)
    GEN_CASE(Float32, float, array_float32)
    GEN_CASE(Float64, double, array_float64)
#undef GEN_CASE

    case ArrayV1Proto::kArrayEdgeValue:
      return DecodeArrayEdgeValue(array_proto.array_edge_value(), input_values);
    case ArrayV1Proto::kArrayEdgeQtype:
      return TypedValue::FromValue(GetQType<ArrayEdge>());

    case ArrayV1Proto::kArrayToScalarEdgeValue:
      return DecodeArrayToScalarEdgeValue(
          array_proto.array_to_scalar_edge_value());
    case ArrayV1Proto::kArrayToScalarEdgeQtype:
      return TypedValue::FromValue(GetQType<ArrayGroupScalarEdge>());
    case ArrayV1Proto::kArrayShapeValue:
      return DecodeArrayShapeValue(array_proto.array_shape_value());
    case ArrayV1Proto::kArrayShapeQtype:
      return TypedValue::FromValue(GetQType<ArrayShape>());

    case ArrayV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "unexpected value=", static_cast<int>(array_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          return RegisterValueDecoder(kArrayV1Codec, DecodeArray);
        })

}  // namespace arolla::serialization_codecs
