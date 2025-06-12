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
#ifndef AROLLA_OPERATORS_BOOTSTRAP_QTYPE_OPERATOR_H_
#define AROLLA_OPERATORS_BOOTSTRAP_QTYPE_OPERATOR_H_

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/slice_qtype.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/sequence/sequence_qtype.h"

namespace arolla {

// qtype.broadcast_qtype_like operator.
struct BroadcastQTypeLikeOp {
  QTypePtr operator()(QTypePtr target, QTypePtr x) const {
    if (auto* result = BroadcastQType({target}, x)) {
      return result;
    }
    return GetNothingQType();
  }
};

// qtype.common_qtype operator.
struct CommonQTypeOp {
  QTypePtr operator()(QTypePtr x, QTypePtr y) const {
    if (auto result = CommonQType(x, y, /*enable_broadcasting=*/true)) {
      return result;
    }
    return GetNothingQType();
  }
};

// qtype.conditional_qtype operator.
struct ConditionalQTypeOp {
  QTypePtr operator()(OptionalUnit condition, QTypePtr true_qtype,
                      QTypePtr false_qtype) const {
    return condition.present ? true_qtype : false_qtype;
  }
};

// qtype.decay_derived_qtype operator.
struct DecayDerivedQTypeOp {
  QTypePtr operator()(QTypePtr x) const { return DecayDerivedQType(x); }
};

// qtype.get_child_shape_qtype
struct GetChildShapeQTypeOp {
  QTypePtr operator()(QTypePtr qtype) const {
    if (auto* edge_qtype = dynamic_cast<const EdgeQType*>(qtype)) {
      return edge_qtype->child_shape_qtype();
    }
    return GetNothingQType();
  }
};

// qtype.get_edge_qtype operator.
struct GetEdgeQTypeOp {
  QTypePtr operator()(QTypePtr x) const {
    if (auto* array_qtype = dynamic_cast<const ArrayLikeQType*>(x)) {
      if (auto* result = array_qtype->edge_qtype()) {
        return result;
      }
    }
    return GetNothingQType();
  }
};

// qtype.get_parent_shape_qtype
struct GetParentShapeQTypeOp {
  QTypePtr operator()(QTypePtr qtype) const {
    if (auto* edge_qtype = dynamic_cast<const EdgeQType*>(qtype)) {
      return edge_qtype->parent_shape_qtype();
    }
    return GetNothingQType();
  }
};

// qtype.get_edge_to_scalar_qtype operator.
struct GetEdgeToScalarQTypeOp {
  QTypePtr operator()(QTypePtr x) const {
    if (auto* array_qtype = dynamic_cast<const ArrayLikeQType*>(x)) {
      if (auto* result = array_qtype->group_scalar_edge_qtype()) {
        return result;
      }
    }
    if (IsScalarQType(x) || IsOptionalQType(x)) {
      return GetQType<ScalarToScalarEdge>();
    }
    return GetNothingQType();
  }
};

// qtype.get_scalar_qtype operator.
struct GetScalarQTypeOp {
  QTypePtr operator()(QTypePtr x) const {
    if (auto* result = GetScalarQTypeOrNull(x)) {
      return result;
    }
    return GetNothingQType();
  }
};

// qtype.get_shape_qtype operator.
struct GetShapeQTypeOp {
  QTypePtr operator()(QTypePtr x) const {
    if (auto* result = GetShapeQTypeOrNull(x)) {
      return result;
    }
    return GetNothingQType();
  }
};

// qtype.get_value_qtype operator.
struct GetValueQTypeOp {
  QTypePtr operator()(QTypePtr x) const {
    auto* result = x->value_qtype();
    if (result == nullptr) {
      return GetNothingQType();
    }
    return result;
  }
};

// qtype._get_key_to_row_dict_qtype
struct GetKeyToRowDictQTypeOp {
  QTypePtr operator()(QTypePtr x) const {
    return GetKeyToRowDictQType(x).value_or(GetNothingQType());
  }
};

// qtype.make_dict_qtype operator.
struct MakeDictQTypeOp {
  QTypePtr operator()(QTypePtr key, QTypePtr value) const {
    return GetDictQType(key, value).value_or(GetNothingQType());
  }
};

// qtype.is_edge_qtype operator.
struct IsEdgeQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsEdgeQType(x));
  }
};

// qtype.is_sequence_qtype operator.
struct IsSequenceQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsSequenceQType(x));
  }
};

// qtype.is_shape_qtype operator.
struct IsShapeQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsShapeQType(x));
  }
};

// qtype.is_dict_qtype operator.
struct IsDictQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsDictQType(x));
  }
};

// qtype.is_tuple_qtype operator.
struct IsTupleQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsTupleQType(x));
  }
};

// qtype.is_namedtuple_qtype operator.
struct IsNamedTupleQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsNamedTupleQType(x));
  }
};

// qtype.is_slice_qtype operator.
struct IsSliceQTypeOp {
  OptionalUnit operator()(QTypePtr x) const {
    return OptionalUnit(IsSliceQType(x));
  }
};

// qtype.make_sequence_qtype operator.
struct MakeSequenceQTypeOp {
  QTypePtr operator()(QTypePtr value_qtype) const {
    return GetSequenceQType(value_qtype);
  }
};

// qtype.with_value_qtype operator.
struct WithValueQTypeOp {
  QTypePtr operator()(QTypePtr shape_qtype, QTypePtr value_qtype) const {
    if (auto* sq = dynamic_cast<const ShapeQType*>(shape_qtype)) {
      return sq->WithValueQType(value_qtype).value_or(GetNothingQType());
    }
    return GetNothingQType();
  }
};

// qtype.get_field_count operator.
struct GetFieldCountOp {
  int64_t operator()(QTypePtr qtype) const {
    return qtype->type_fields().size();
  }
};

// qtype.make_tuple_qtype operator.
class MakeTupleQTypeOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_qtypes,
      QTypePtr output_type) const final;
};

// qtype.get_field_qtypes operator.
class GetFieldQTypesOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

// qtype.get_field_names operator.
class GetFieldNamesOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types, QTypePtr output_type) const final;
};

// qtype.get_field_qtype operator.
struct GetFieldQTypeOp {
  QTypePtr operator()(QTypePtr qtype, int64_t idx) const {
    const auto& fields = qtype->type_fields();
    if (idx < 0 || idx >= fields.size()) {
      return GetNothingQType();
    }
    return fields[idx].GetType();
  }
};

// qtype.slice_tuple_qtype operator.
struct SliceTupleQTypeOp {
  QTypePtr operator()(QTypePtr tuple_qtype, int64_t offset,
                      int64_t size) const {
    const auto& fields = tuple_qtype->type_fields();
    const auto unsigned_offset = static_cast<size_t>(offset);
    if (offset < 0 || unsigned_offset > fields.size()) {
      return GetNothingQType();
    }
    const auto unsigned_size = (size == -1 ? fields.size() - unsigned_offset
                                           : static_cast<size_t>(size));
    if (size < -1 || unsigned_offset + unsigned_size > fields.size()) {
      return GetNothingQType();
    }
    std::vector<QTypePtr> slice(unsigned_size);
    for (size_t i = 0; i < slice.size(); ++i) {
      slice[i] = fields[unsigned_offset + i].GetType();
    }
    return MakeTupleQType(slice);
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_BOOTSTRAP_QTYPE_OPERATOR_H_
