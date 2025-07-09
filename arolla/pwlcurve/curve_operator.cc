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
#include "arolla/pwlcurve/curve_operator.h"

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/pwlcurve/curves.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"  // IWYU pragma: keep
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

AROLLA_DEFINE_SIMPLE_QTYPE(CURVE, pwlcurve::CurvePtr);

namespace pwlcurve {

absl::StatusOr<CurvePtr> CreateCurveOp::operator()(const Bytes& spec) const {
  ASSIGN_OR_RETURN(auto curve, NewCurve(spec));
  return curve;
}

absl::StatusOr<CurvePtr> CreateCurveOp::operator()(
    int32_t curve_type, const DenseArray<float>& x_ctrl_points,
    const DenseArray<float>& y_ctrl_points) const {
  if (!x_ctrl_points.IsFull() || !y_ctrl_points.IsFull()) {
    return absl::InvalidArgumentError("expected a full array");
  }
  ASSIGN_OR_RETURN(
      auto curve,
      NewCurve(static_cast<pwlcurve::CurveType>(curve_type),
               x_ctrl_points.values.span(), y_ctrl_points.values.span()));
  return curve;
}

}  // namespace pwlcurve
}  // namespace arolla
