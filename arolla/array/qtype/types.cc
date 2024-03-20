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
#include "arolla/array/qtype/types.h"

#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/qtype/any_qtype.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

static const char ArrayTypeName[] = "Array";
using ValueToArrayTypeMapping =
    ValueToArrayLikeTypeMapping<ArrayQTypeBase, ArrayTypeName>;

class ArrayWeakFloatQType final : public ArrayQType<double>,
                                  public DerivedQTypeInterface {
 public:
  ArrayWeakFloatQType()
      : ArrayQType(meta::type<Array<double>>(), "ARRAY_WEAK_FLOAT",
                   GetWeakFloatQType()) {
    CHECK_OK(VerifyDerivedQType(this));
    RegisterValueQType();
  }

  QTypePtr GetBaseQType() const final { return GetArrayQType<double>(); }

  ReprToken UnsafeReprToken(const void* source) const final {
    return ArrayReprToken(
        *static_cast<const Array<double>*>(source),
        [](double value) {
          return std::move(GenReprTokenWeakFloat(value).str);
        },
        GetWeakFloatQType()->name());
  }
};

class ArrayEdgeQType final : public EdgeQType {
 public:
  static const ArrayEdgeQType* GetInstance() {
    static absl::NoDestructor<ArrayEdgeQType> result;
    return result.get();
  }

  ArrayEdgeQType() : EdgeQType(meta::type<ArrayEdge>(), "ARRAY_EDGE") {}

  QTypePtr parent_shape_qtype() const final;

  QTypePtr child_shape_qtype() const final;
};

class ArrayGroupScalarEdgeQType final : public EdgeQType {
 public:
  static const ArrayGroupScalarEdgeQType* GetInstance() {
    static absl::NoDestructor<ArrayGroupScalarEdgeQType> result;
    return result.get();
  }

  ArrayGroupScalarEdgeQType()
      : EdgeQType(meta::type<ArrayGroupScalarEdge>(), "ARRAY_TO_SCALAR_EDGE") {}

  QTypePtr parent_shape_qtype() const final;

  QTypePtr child_shape_qtype() const final;
};

class ArrayShapeQType final : public ArrayLikeShapeQType {
 public:
  static const ArrayShapeQType* GetInstance() {
    static absl::NoDestructor<ArrayShapeQType> result;
    return result.get();
  }

  ArrayShapeQType()
      : ArrayLikeShapeQType(meta::type<ArrayShape>(), "ARRAY_SHAPE") {}

  absl::StatusOr<QTypePtr> WithValueQType(QTypePtr value_qtype) const override {
    return GetArrayQTypeByValueQType(value_qtype);
  }

  QTypePtr presence_qtype() const override { return GetQType<Array<Unit>>(); }
};

}  // namespace

absl::StatusOr<const ArrayQTypeBase*> GetArrayQTypeByValueQType(
    QTypePtr value_qtype) {
  return ValueToArrayTypeMapping::GetInstance()->Get(
      DecayOptionalQType(value_qtype));
}

void ArrayQTypeBase::RegisterValueQType() {
  ValueToArrayTypeMapping::GetInstance()->Set(value_qtype(), this);
}

const EdgeQType* ArrayQTypeBase::edge_qtype() const {
  return ArrayEdgeQType::GetInstance();
}

const EdgeQType* ArrayQTypeBase::group_scalar_edge_qtype() const {
  return ArrayGroupScalarEdgeQType::GetInstance();
}

QTypePtr QTypeTraits<ArrayShape>::type() {
  return ArrayShapeQType::GetInstance();
}

QTypePtr QTypeTraits<ArrayEdge>::type() {
  return ArrayEdgeQType::GetInstance();
}

QTypePtr QTypeTraits<ArrayGroupScalarEdge>::type() {
  return ArrayGroupScalarEdgeQType::GetInstance();
}

const ArrayLikeShapeQType* ArrayQTypeBase::shape_qtype() const {
  return ArrayShapeQType::GetInstance();
}

QTypePtr ArrayEdgeQType::parent_shape_qtype() const {
  return ArrayShapeQType::GetInstance();
}

QTypePtr ArrayEdgeQType::child_shape_qtype() const {
  return ArrayShapeQType::GetInstance();
}

QTypePtr ArrayGroupScalarEdgeQType::parent_shape_qtype() const {
  return GetQType<OptionalScalarShape>();
}

QTypePtr ArrayGroupScalarEdgeQType::child_shape_qtype() const {
  return ArrayShapeQType::GetInstance();
}

AROLLA_FOREACH_BASE_TYPE(AROLLA_DEFINE_ARRAY_QTYPE);
AROLLA_DEFINE_ARRAY_QTYPE(UNIT, Unit);
AROLLA_DEFINE_ARRAY_QTYPE(ANY, Any);

QTypePtr GetArrayWeakFloatQType() {
  static const absl::NoDestructor<ArrayWeakFloatQType> result;
  return result.get();
}

ReprToken ReprTraits<ArrayEdge>::operator()(const ArrayEdge& value) const {
  ReprToken result;
  if (value.edge_type() == ArrayEdge::SPLIT_POINTS) {
    result.str = absl::StrFormat("array_edge(split_points=%s)",
                                 Repr(value.edge_values()));
  } else if (value.edge_type() == ArrayEdge::MAPPING) {
    result.str =
        absl::StrFormat("array_edge(mapping=%s, parent_size=%d)",
                        Repr(value.edge_values()), value.parent_size());
  } else {
    result.str = "array_edge";
  }
  return result;
}

ReprToken ReprTraits<ArrayGroupScalarEdge>::operator()(
    const ArrayGroupScalarEdge& value) const {
  return ReprToken{absl::StrFormat("array_to_scalar_edge(child_size=%d)",
                                   value.child_size())};
}

}  // namespace arolla
