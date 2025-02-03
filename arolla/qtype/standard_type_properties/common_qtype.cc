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
#include "arolla/qtype/standard_type_properties/common_qtype.h"

#include <algorithm>
#include <array>
#include <cstdint>

#include "absl/algorithm/container.h"
#include "absl/types/span.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"

namespace arolla {
namespace {

// Computes the common qtype of two scalar qtypes or nullptr if it doesn't
// exist.
//
// We allow the following implicit casts:
//   int32 -> int64 -> float32 -> float64
//   weak_float -> float32 -> float64
// so, for example, the common type of `int64` and `weak_float` is `float32`.
//
const QType* CommonScalarQType(const QType* lhs_qtype, const QType* rhs_qtype) {
  if (lhs_qtype == rhs_qtype) {
    return lhs_qtype;
  }

  // Except for the (weak_float, weak_float) case, common type with weak_float
  // is the same as with float32.
  if (lhs_qtype == GetWeakFloatQType()) {
    lhs_qtype = GetQType<float>();
  }
  if (rhs_qtype == GetWeakFloatQType()) {
    rhs_qtype = GetQType<float>();
  }

  // float64 <- float32 <- int64 <- int32
  static const std::array numeric_types = {
      GetQType<double>(), GetQType<float>(), GetQType<int64_t>(),
      GetQType<int32_t>()};
  auto lhs_it = absl::c_find(numeric_types, lhs_qtype);
  auto rhs_it = absl::c_find(numeric_types, rhs_qtype);
  if (lhs_it != numeric_types.end() && rhs_it != numeric_types.end()) {
    return *std::min(lhs_it, rhs_it);
  }

  return nullptr;
}

const ShapeQType* CommonShapeQType(const ShapeQType* lhs_qtype,
                                   const ShapeQType* rhs_qtype,
                                   bool enable_broadcasting) {
  if (lhs_qtype == rhs_qtype) {
    return rhs_qtype;
  }
  if (!enable_broadcasting &&
      (IsArrayLikeShapeQType(lhs_qtype) || IsArrayLikeShapeQType(rhs_qtype))) {
    // Stop handling array broadcasting, if it's disabled.
    return nullptr;
  }
  if (lhs_qtype == GetQType<ScalarShape>()) {
    return rhs_qtype;
  }
  if (rhs_qtype == GetQType<ScalarShape>()) {
    return lhs_qtype;
  }
  if (lhs_qtype == GetQType<OptionalScalarShape>()) {
    return rhs_qtype;
  }
  if (rhs_qtype == GetQType<OptionalScalarShape>()) {
    return lhs_qtype;
  }
  return nullptr;  // For example, arrays of different kinds.
}

}  // namespace

const QType* CommonQType(const QType* lhs_qtype, const QType* rhs_qtype,
                         bool enable_broadcasting) {
  if (lhs_qtype == nullptr || rhs_qtype == nullptr) {
    return nullptr;
  }
  if (lhs_qtype == rhs_qtype) {
    return lhs_qtype;
  }
  const QType* scalar_qtype;
  {
    auto lhs_scalar_qtype = GetScalarQTypeOrNull(lhs_qtype);
    if (lhs_scalar_qtype == nullptr) {
      return nullptr;
    }
    auto rhs_scalar_qtype = GetScalarQTypeOrNull(rhs_qtype);
    if (rhs_scalar_qtype == nullptr) {
      return nullptr;
    }
    scalar_qtype = CommonScalarQType(lhs_scalar_qtype, rhs_scalar_qtype);
    if (scalar_qtype == nullptr) {
      return nullptr;
    }
  }
  const ShapeQType* shape_qtype =
      CommonShapeQType(GetShapeQTypeOrNull(lhs_qtype),
                       GetShapeQTypeOrNull(rhs_qtype), enable_broadcasting);
  if (shape_qtype == nullptr) {
    return nullptr;
  }
  return shape_qtype->WithValueQType(scalar_qtype).value_or(nullptr);
}

bool CanCastImplicitly(QTypePtr from_qtype, QTypePtr to_qtype,
                       bool enable_broadcasting) {
  return to_qtype != nullptr &&
         CommonQType(from_qtype, to_qtype, enable_broadcasting) == to_qtype;
}

const QType* CommonQType(absl::Span<const QType* const> qtypes,
                         bool enable_broadcasting) {
  if (qtypes.empty()) {
    return nullptr;
  }
  const QType* result = qtypes[0];
  for (const QType* qtype : qtypes.subspan(1)) {
    result = CommonQType(result, qtype, enable_broadcasting);
  }
  return result;
}

const QType* BroadcastQType(absl::Span<QType const* const> target_qtypes,
                            const QType* qtype) {
  if (absl::c_any_of(target_qtypes,
                     [](auto* qtype) { return qtype == nullptr; }) ||
      qtype == nullptr) {
    return nullptr;
  }
  const ShapeQType* shape_qtype = GetShapeQTypeOrNull(qtype);
  for (const auto* target_qtype : target_qtypes) {
    shape_qtype =
        CommonShapeQType(shape_qtype, GetShapeQTypeOrNull(target_qtype),
                         /*enable_broadcasting=*/true);
  }
  if (shape_qtype == nullptr) {
    return nullptr;
  }
  auto* scalar_qtype = GetScalarQTypeOrNull(qtype);
  if (scalar_qtype == nullptr) {
    return nullptr;
  }
  return shape_qtype->WithValueQType(scalar_qtype).value_or(nullptr);
}

}  // namespace arolla
