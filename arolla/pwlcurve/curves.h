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
#ifndef AROLLA_PWLCURVE_CURVES_H_
#define AROLLA_PWLCURVE_CURVES_H_

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace arolla::pwlcurve {

enum CurveType : int {
  CurveTypeMin = 0,

  PWLCurve = 0,
  LogPWLCurve = 1,
  Log1pPWLCurve = 2,
  Symlog1pPWLCurve = 3,

  // Legacy names for backwards compatibility.
  LogP1PWLCurve = 2,
  SymmetricLogP1PWLCurve = 3,

  CurveTypeMax = 3
};

// Curves are defined by specifying a list of control points. This struct
// represents a single control point.
template <typename T>
struct Point {
  T x;
  T y;

  static_assert(std::is_floating_point<T>::value,
                "Point<T> must be templated on a floating point type.");

  Point() = default;
  Point(T input_x, T input_y) : x(input_x), y(input_y) {}

  // Implicit conversion from differently-templated Point.
  template <typename U>
  /*implicit*/ Point(Point<U> p) : x(p.x), y(p.y) {}
};

// Curve class used for all curve implementations.
// To obtain an instance, use any of Curve's factory methods.
class Curve {
 public:
  // Disallow copy and assign.
  Curve(const Curve&) = delete;
  Curve& operator=(const Curve&) = delete;

  // Curves are constructed by NewCurve rather than a class constructor, because
  // constructors aren't allowed to fail. NewCurve returns StatusOr<Curve> so
  // that failure is possible without crashing or throwing an exception.
  Curve() = default;
  virtual ~Curve() = default;

  // Evaluates the curve at a given x, and returns the result.
  virtual float Eval(float x) const = 0;
  virtual double Eval(double x) const = 0;

  // Evaluates the curve at multiple xs.
  virtual std::vector<float> Eval(absl::Span<const float> xs) const = 0;
  virtual std::vector<double> Eval(absl::Span<const double> xs) const = 0;

  // Returns the type and the control points used to create the curve.
  absl::Span<const Point<double>> ControlPoints() const { return points_; }
  CurveType Type() const { return type_; }

  // Serialize the curve losslessly as a string.
  std::string ToString() const;

 protected:
  // Stores the type and control points. Used by child classes.
  Curve(CurveType type, absl::Span<const Point<double>> points)
      : type_(type), points_(points.begin(), points.end()) {}

 private:
  CurveType type_;
  const std::vector<Point<double>> points_;
};

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const Point<double>> points);

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const double> x_ctrl_points,
    absl::Span<const double> y_ctrl_points);
absl::StatusOr<std::unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const float> x_ctrl_points,
    absl::Span<const float> y_ctrl_points);

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(absl::string_view spec);

absl::StatusOr<std::unique_ptr<Curve>> NewCurveWithSeparator(
    char separator_char, absl::string_view spec);

absl::StatusOr<std::unique_ptr<Curve>> NewCurveWithAllowedSeparators(
    absl::string_view allowed_separators, absl::string_view spec);

inline bool IsValidCurveType(int value) {
  return value >= CurveTypeMin && value <= CurveTypeMax;
}

namespace internals {
absl::Status Parse(char separator_char, absl::string_view curve,
                   CurveType* type, std::vector<Point<double>>* points);

constexpr absl::string_view CurveType_Names[] = {
    "PWLCurve", "LogPWLCurve", "Log1pPWLCurve", "Symlog1pPWLCurve"};

inline absl::string_view CurveType_Name(CurveType type) {
  return CurveType_Names[static_cast<int>(type)];
}

int FindSeparatorUsed(absl::string_view allowed_separators,
                      absl::string_view curve);

}  // namespace internals
}  // namespace arolla::pwlcurve

#endif  // AROLLA_PWLCURVE_CURVES_H_
