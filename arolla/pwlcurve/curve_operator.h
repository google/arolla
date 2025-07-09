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
#ifndef AROLLA_QEXPR_OPERATORS_CORE_CURVE_OPERATOR_H
#define AROLLA_QEXPR_OPERATORS_CORE_CURVE_OPERATOR_H

#include <cstdint>
#include <memory>
#include <type_traits>

#include "absl/status/statusor.h"
#include "arolla/pwlcurve/curves.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"  // IWYU pragma: keep
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace pwlcurve {

// Access Curve via this wrapper.
using CurvePtr = std::shared_ptr<const pwlcurve::Curve>;

// create_curve operator creates a Curve operator from spec or a list of control
// points in runtime.
//
// Curve spec version accepts one input with curve spec encoded as
// arolla::Bytes. Spec example: "PWLCurve({{1;0};{5;1};{inf;inf}})".
//
// Control points version accepts three inputs:
//   - type: int32_t, an integral value of CurveType enum.
//   - x_ctrl_points, y_ctrl_points: vectors of floats, represent curve control
//   points.
//
struct CreateCurveOp {
  // Creates Curve operator from spec.
  absl::StatusOr<CurvePtr> operator()(const Bytes& spec) const;
  // Creates Curve operator from curve type (arolla::curves::CurveType)
  // and a list of control points.
  absl::StatusOr<CurvePtr> operator()(
      int32_t curve_type, const DenseArray<float>& x_ctrl_points,
      const DenseArray<float>& y_ctrl_points) const;
};

// eval_curve operator evaluates a Curve on a provided point / array of points.
struct EvalCurveOp {
  template <typename FloatType>
  std::enable_if_t<std::is_floating_point_v<FloatType>, FloatType> operator()(
      const CurvePtr& curve, FloatType point) const {
    return curve->Eval(point);
  }
};

}  // namespace pwlcurve

template <>
struct FingerprintHasherTraits<std::shared_ptr<const pwlcurve::Curve>> {
  void operator()(FingerprintHasher* hasher,
                  const pwlcurve::CurvePtr& curve) const {
    // Combine() works for vectors of doubles but not vectors of points, so we
    // have to extract the points into separate xs and ys first.
    auto points = curve->ControlPoints();
    hasher->Combine(curve->Type(), points.size());
    for (auto p : points) {
      hasher->Combine(p.x, p.y);
    }
  }
};

AROLLA_DECLARE_SIMPLE_QTYPE(CURVE, pwlcurve::CurvePtr);

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_CORE_CURVE_OPERATOR_H
