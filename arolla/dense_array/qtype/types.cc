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
#include "arolla/dense_array/qtype/types.h"

#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
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

static const char DenseArrayTypeName[] = "DenseArray";

using ValueToDenseArrayTypeMapping =
    ValueToArrayLikeTypeMapping<DenseArrayQTypeBase, DenseArrayTypeName>;

class DenseArrayWeakFloatQType final : public DenseArrayQType<double>,
                                       public DerivedQTypeInterface {
 public:
  DenseArrayWeakFloatQType()
      : DenseArrayQType(meta::type<DenseArray<double>>(),
                        "DENSE_ARRAY_WEAK_FLOAT", GetWeakFloatQType()) {
    CHECK_OK(VerifyDerivedQType(this));
    RegisterValueQType();
  }

  QTypePtr GetBaseQType() const final { return GetDenseArrayQType<double>(); }

  ReprToken UnsafeReprToken(const void* source) const final {
    return DenseArrayReprToken(
        *static_cast<const DenseArray<double>*>(source),
        [](double value) {
          return std::move(GenReprTokenWeakFloat(value).str);
        },
        GetWeakFloatQType()->name());
  }
};

class DenseArrayEdgeQType final : public EdgeQType {
 public:
  static const DenseArrayEdgeQType* GetInstance() {
    static absl::NoDestructor<DenseArrayEdgeQType> result;
    return result.get();
  }

  DenseArrayEdgeQType()
      : EdgeQType(meta::type<DenseArrayEdge>(), "DENSE_ARRAY_EDGE") {}

  QTypePtr parent_shape_qtype() const final;

  QTypePtr child_shape_qtype() const final;
};

class DenseArrayGroupScalarEdgeQType final : public EdgeQType {
 public:
  static const DenseArrayGroupScalarEdgeQType* GetInstance() {
    static absl::NoDestructor<DenseArrayGroupScalarEdgeQType> result;
    return result.get();
  }

  DenseArrayGroupScalarEdgeQType()
      : EdgeQType(meta::type<DenseArrayGroupScalarEdge>(),
                  "DENSE_ARRAY_TO_SCALAR_EDGE") {}

  QTypePtr parent_shape_qtype() const final;

  QTypePtr child_shape_qtype() const final;
};

class DenseArrayShapeQType final : public ArrayLikeShapeQType {
 public:
  static const DenseArrayShapeQType* GetInstance() {
    static absl::NoDestructor<DenseArrayShapeQType> result;
    return result.get();
  }

  DenseArrayShapeQType()
      : ArrayLikeShapeQType(meta::type<DenseArrayShape>(),
                            "DENSE_ARRAY_SHAPE") {}

  absl::StatusOr<QTypePtr> WithValueQType(QTypePtr value_qtype) const override {
    return GetDenseArrayQTypeByValueQType(value_qtype);
  }

  QTypePtr presence_qtype() const override {
    return GetQType<DenseArray<Unit>>();
  }
};

}  // namespace

absl::StatusOr<const DenseArrayQTypeBase*> GetDenseArrayQTypeByValueQType(
    QTypePtr value_qtype) {
  return ValueToDenseArrayTypeMapping::GetInstance()->Get(
      DecayOptionalQType(value_qtype));
}

void DenseArrayQTypeBase::RegisterValueQType() {
  ValueToDenseArrayTypeMapping::GetInstance()->Set(value_qtype(), this);
}

const EdgeQType* DenseArrayQTypeBase::edge_qtype() const {
  return DenseArrayEdgeQType::GetInstance();
}

const EdgeQType* DenseArrayQTypeBase::group_scalar_edge_qtype() const {
  return DenseArrayGroupScalarEdgeQType::GetInstance();
}

QTypePtr QTypeTraits<DenseArrayShape>::type() {
  return DenseArrayShapeQType::GetInstance();
}

QTypePtr QTypeTraits<DenseArrayEdge>::type() {
  return DenseArrayEdgeQType::GetInstance();
}

QTypePtr QTypeTraits<DenseArrayGroupScalarEdge>::type() {
  return DenseArrayGroupScalarEdgeQType::GetInstance();
}

const ArrayLikeShapeQType* DenseArrayQTypeBase::shape_qtype() const {
  return DenseArrayShapeQType::GetInstance();
}

QTypePtr DenseArrayEdgeQType::parent_shape_qtype() const {
  return DenseArrayShapeQType::GetInstance();
}

QTypePtr DenseArrayEdgeQType::child_shape_qtype() const {
  return DenseArrayShapeQType::GetInstance();
}

QTypePtr DenseArrayGroupScalarEdgeQType::parent_shape_qtype() const {
  return GetQType<OptionalScalarShape>();
}

QTypePtr DenseArrayGroupScalarEdgeQType::child_shape_qtype() const {
  return DenseArrayShapeQType::GetInstance();
}

AROLLA_FOREACH_BASE_TYPE(AROLLA_DEFINE_DENSE_ARRAY_QTYPE);
AROLLA_DEFINE_DENSE_ARRAY_QTYPE(UNIT, Unit);
AROLLA_DEFINE_DENSE_ARRAY_QTYPE(ANY, Any);

QTypePtr GetDenseArrayWeakFloatQType() {
  static const absl::NoDestructor<DenseArrayWeakFloatQType> result;
  return result.get();
}

ReprToken ReprTraits<DenseArrayEdge>::operator()(
    const DenseArrayEdge& value) const {
  ReprToken result;
  if (value.edge_type() == DenseArrayEdge::SPLIT_POINTS) {
    result.str = absl::StrFormat("dense_array_edge(split_points=%s)",
                                 Repr(value.edge_values()));
  } else if (value.edge_type() == DenseArrayEdge::MAPPING) {
    result.str =
        absl::StrFormat("dense_array_edge(mapping=%s, parent_size=%d)",
                        Repr(value.edge_values()), value.parent_size());
  } else {
    result.str = "dense_array_edge";
  }
  return result;
}

ReprToken ReprTraits<DenseArrayGroupScalarEdge>::operator()(
    const DenseArrayGroupScalarEdge& value) const {
  return ReprToken{absl::StrFormat("dense_array_to_scalar_edge(child_size=%d)",
                                   value.child_size())};
}

ReprToken ReprTraits<DenseArrayShape>::operator()(
    const DenseArrayShape& value) const {
  return ReprToken{absl::StrFormat("dense_array_shape{size=%d}", value.size)};
}

}  // namespace arolla
