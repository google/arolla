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
#include "arolla/qexpr/operators/core/curve_operator.h"

#include <cstdint>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
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

namespace {

using absl::Status;
using absl::StatusOr;

std::vector<double> ToDouble(const std::vector<float>& v) {
  std::vector<double> res;
  res.reserve(v.size());
  for (float x : v) res.push_back(x);
  return res;
}

StatusOr<std::vector<float>> FullDenseArrayToVector(
    const DenseArray<float>& array) {
  std::vector<float> result;
  result.reserve(array.size());
  Status status;
  array.ForEach([&](auto id, auto present, auto value) {
    if (!present) {
      status = absl::InvalidArgumentError(absl::StrFormat(
          "expected a full array starting from 0, but id %d is missing", id));
    }
    result.push_back(value);
  });
  return result;
}

}  // namespace

StatusOr<CurvePtr> CreateCurveOp::operator()(const Bytes& spec) const {
  ASSIGN_OR_RETURN(auto curve, NewCurve(spec));
  return curve;
}

StatusOr<CurvePtr> CreateCurveOp::operator()(
    int32_t curve_type, const DenseArray<float>& x_ctrl_points,
    const DenseArray<float>& y_ctrl_points) const {
  ASSIGN_OR_RETURN(auto xs_array, FullDenseArrayToVector(x_ctrl_points));
  ASSIGN_OR_RETURN(auto ys_array, FullDenseArrayToVector(y_ctrl_points));
  auto curve_type_enum = static_cast<pwlcurve::CurveType>(curve_type);
  ASSIGN_OR_RETURN(auto curve, NewCurve(curve_type_enum, ToDouble(xs_array),
                                        ToDouble(ys_array)));
  return curve;
}

}  // namespace pwlcurve
}  // namespace arolla
