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
#include "arolla/pwlcurve/curves.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "double-conversion/double-to-string.h"
#include "double-conversion/utils.h"

// These functions are internal-only and should only be called from curves.h.
namespace arolla::pwlcurve {
namespace internals {

using absl::Status;
using absl::StatusCode;
using absl::StatusOr;
using absl::string_view;
using std::unique_ptr;

constexpr double INF = std::numeric_limits<double>::infinity();
// This is already defined in the top level namespace and we don't want to use
// that version.
using std::isinf;

// Serialize a Point<double> losslessly as a string.
// We use the double_conversion library to losslessly convert doubles to decimal
// strings without using more digits than necessary.
std::string PointToString(const Point<double> point) {
  static const double_conversion::DoubleToStringConverter converter(
      double_conversion::DoubleToStringConverter::UNIQUE_ZERO, "inf", "nan",
      'e', -6, 21, 6, 0);

  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  builder.AddString("{");
  converter.ToShortest(point.x, &builder);
  builder.AddString(";");
  converter.ToShortest(point.y, &builder);
  builder.AddString("}");
  return builder.Finalize();
}

// ============================================================================
// Parsing
// ============================================================================

// This parser minimizes overhead such as string copying and avoids
// regular expression execution. The top-level routine is
// internals::Parse. The parser provides rudimentary error
// information: only the first error encountered is reported and we don't
// save position information because this would be too expensive to
// do comprehensively in the later stages; e.g. when monotonicity in
// point lists is checked.

// Helper method for computing the static type map in ParseType below.
inline absl::btree_map<string_view, CurveType>* NewCurveTypeByName() {
  auto m = new absl::btree_map<string_view, CurveType>;
  for (int i = CurveTypeMin; i <= CurveTypeMax; ++i) {
    (*m)[CurveType_Names[i]] = static_cast<CurveType>(i);
  }
  // Accept legacy names for backwards compatibility.
  (*m)["LogP1PWLCurve"] = LogP1PWLCurve;
  (*m)["SymmetricLogP1PWLCurve"] = SymmetricLogP1PWLCurve;
  return m;
}

// Given |type_str|, a string containing a curve type name
// ("PWLCurve", "LogPWLCurve", ...), store the appropriate enum value
// in |type|. Returns OK or UNKNOWN_CURVE_TYPE.
//
// The method avoids allocating a string.
inline Status ParseType(string_view type_str, CurveType* type) {
  static const absl::btree_map<string_view, CurveType>* curve_type_by_name =
      NewCurveTypeByName();
  auto it = curve_type_by_name->find(type_str);
  if (it == curve_type_by_name->end())
    return Status(StatusCode::kInvalidArgument, "UNKNOWN_CURVE_TYPE");
  *type = it->second;
  return Status();
}

// Parses a point (e.g., {0.1222;15.25}) from the contents of |curve|
// starting at |pos| into |point|. Afterwards, |pos| is located at the position
// after the closing curly. Returns OK or an appropriate error.
Status ParsePoint(char separator_char, string_view curve, int* pos,
                  Point<double>* point) {
  if (*pos >= curve.size() || (curve)[*pos] != '{')
    return Status(StatusCode::kInvalidArgument,
                  "MISSING_OPENING_CURLY_FOR_POINT");
  ++*pos;
  string_view::size_type separator = curve.find(separator_char, *pos);
  if (separator == string_view::npos)
    return Status(StatusCode::kInvalidArgument,
                  "MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES");

  // We look for '}' from *pos even though we could
  // optimistically look from separator. This allows us to detect
  // separator > closing_curly (MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES)
  // below. It's a tad slower but the errors can be esp. frustrating
  // otherwise, especially since we don't give position information in errors.
  string_view::size_type closing_curly = curve.find('}', *pos);
  if (closing_curly == string_view::npos)
    return Status(StatusCode::kInvalidArgument,
                  "MISSING_CLOSING_CURLY_FOR_POINT");

  if (separator > closing_curly)
    return Status(StatusCode::kInvalidArgument,
                  "MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES");

  // absl::SimpleAtod requires a double pointer, but &point->x might be a float
  // pointer. So we fill a temporary double, and set point->x to that value.
  auto x_str = curve.substr(*pos, separator - *pos);
  double x;
  if (!absl::SimpleAtod(x_str, &x))
    return Status(StatusCode::kInvalidArgument, "INVALID_DOUBLE");
  point->x = x;

  auto y_str = curve.substr(separator + 1, closing_curly - separator - 1);
  double y;
  if (!absl::SimpleAtod(y_str, &y))
    return Status(StatusCode::kInvalidArgument, "INVALID_DOUBLE");
  point->y = y;

  *pos = closing_curly + 1;
  return Status();
}

// We only allow ';' and '|' as separator chars for now.
inline bool IsValidSeparatorChar(char separator_char) {
  return separator_char == ';' || separator_char == '|';
}

// Parses a |curve| expression such as "PWLCurve({{0;1};{1;1.5};{2;2.5}})"
// into |type| and |points|. Returns true iff successful.
Status Parse(char separator_char, string_view curve, CurveType* type,
             std::vector<Point<double>>* points) {
  if (!IsValidSeparatorChar(separator_char))
    return Status(StatusCode::kInvalidArgument, "INVALID_SEPARATOR_CHAR");
  // Parse 'CurveName('
  int pos = curve.find('(');
  if (pos == string_view::npos) {
    return Status(StatusCode::kInvalidArgument, "MISSING_OPEN_PAREN");
  }
  {
    Status status = ParseType(curve.substr(0, pos), type);
    if (!status.ok()) return status;
  }
  // Parse '{'
  ++pos;
  if (pos >= static_cast<int64_t>(curve.size()) || curve[pos] != '{')
    return Status(StatusCode::kInvalidArgument,
                  "MISSING_OPENING_CURLY_FOR_POINT_LIST");
  ++pos;
  // Now pos is set to the first point, e.g. here:
  //                |
  //               \|/
  // Log1pPWLCurve({{0;1};{2;2};{4;8}})
  points->clear();
  points->emplace_back();
  {
    Status status = ParsePoint(separator_char, curve, &pos, &points->back());
    if (!status.ok()) return status;
  }
  // As long as we keep encountering separator_char, continue to parse
  // additional points.
  while (pos < static_cast<int64_t>(curve.size()) &&
         curve[pos] == separator_char) {
    ++pos;
    points->emplace_back();
    Status status = ParsePoint(separator_char, curve, &pos, &points->back());
    if (!status.ok()) return status;
  }
  if (pos + 2 == curve.size() && curve[pos] == '}' && curve[pos + 1] == ')')
    return Status();
  if (pos == curve.size() || curve[pos] != '}')
    return Status(StatusCode::kInvalidArgument,
                  "MISSING_CLOSING_CURLY_OR_SEPARATOR_FOR_POINT_LIST");
  ++pos;
  if (pos == curve.size() || curve[pos] != ')') {
    return Status(StatusCode::kInvalidArgument, "MISSING_CLOSING_PAREN");
  }
  return Status(StatusCode::kInvalidArgument,
                "TRAILING_JUNK_AFTER_CLOSING_PAREN");
}

// If |curve| contains none of the chars in |allowed_separators|, returns
// allowed_separators[0].
// If |curve| contains exactly one of the chars in |allowed_separators|,
// returns it.
// If |curve| contains multiple chars in |allowed_separators| or
// |allowed_separators| is empty, returns -1.
int FindSeparatorUsed(string_view allowed_separators, string_view curve) {
  if (allowed_separators.empty()) {
    return -1;
  }
  int separator_used = -1;
  for (char allowed : allowed_separators) {
    if (!absl::StrContains(curve, allowed)) continue;
    if (separator_used == -1)
      separator_used = allowed;
    else
      return -1;
  }
  return separator_used == -1 ? allowed_separators[0] : separator_used;
}

//=============================================================================
// Helper routines for working with control points.
//=============================================================================

// Strips points with duplicate Y coordinates from both end of the
// range from begin to end, by adjusting the provided iterators.
template <class It>
void TrimDuplicateYPointsAtEnds(It* begin, It* end) {
  while ((*begin + 1 < *end) && ((*begin)->y == (*begin + 1)->y))
    ++*begin;
  while ((*end - 1 > *begin) && ((*end - 1)->y == (*end - 2)->y))
    --*end;
}

// Checks that are common to all curves. This is parameterized by a Hook.
// The Hook provides:
// - ::ValidateMin: a validation routine that can be custom to a
//   particular curve - this routine can inspect the range of provided control
//   points.
// This routine potentially modifies |begin| and |end|; it reports the success
// or failure in its return value.
template <typename Hook, class Point>
Status CommonChecks(absl::Span<const Point> points,
                    typename absl::Span<const Point>::const_iterator* begin,
                    typename absl::Span<const Point>::const_iterator* end) {
  if (points.empty())
    return Status(StatusCode::kInvalidArgument, "NOT_ENOUGH_FINITE_POINTS");
  for (int ii = 1; ii < points.size(); ++ii) {
    // Check whether x values are sorted and unique.
    if (points[ii - 1].x >= points[ii].x) {
      return Status(StatusCode::kInvalidArgument,
                    "X_VALUES_NOT_STRICTLY_MONOTONICALLY_INCREASING");
    }
  }
  *begin = points.begin();
  *end = points.end();
  TrimDuplicateYPointsAtEnds(begin, end);
  DCHECK(*begin < *end);   // There's at least 1 point left after trimming.
  if (*end - *begin == 1)  // It's a constant - that's OK.
    return Status();
  if (*end - *begin >= 3) {
    if ((isinf(points.front().x) || isinf(points.front().y)) &&
        ((*begin + 1)->y == (*begin + 2)->y)) {
      return Status(StatusCode::kInvalidArgument,
                    "UNSUPPORTED_ASYMPTOTE_ADJACENT_TO_CONSTANT_SEGMENT");
    }
    if ((isinf(points.back().x) || isinf(points.back().y)) &&
        ((*end - 3)->y == (*end - 2)->y)) {
      return Status(StatusCode::kInvalidArgument,
                    "UNSUPPORTED_ASYMPTOTE_ADJACENT_TO_CONSTANT_SEGMENT");
    }
  }
  return Hook::ValidateMin(/*min_x=*/points.front().x);
}

// ============================================================================
// Curve implementations: trivial cases (ConstantCurve).
// ============================================================================
// The constant curve yields a constant y-value.
class ConstantCurve : public Curve {
 public:
  // Disallow copy and assign.
  ConstantCurve(const ConstantCurve&) = delete;
  ConstantCurve& operator=(const ConstantCurve&) = delete;

  explicit ConstantCurve(double y, CurveType type,
                         absl::Span<const Point<double>> points)
      : Curve(type, points), y_(y) {}

  float Eval(float x) const override { return y_; }
  double Eval(double x) const override { return y_; }

  std::vector<float> Eval(absl::Span<const float> xs) const override {
    return std::vector<float>(xs.size(), y_);
  }
  std::vector<double> Eval(absl::Span<const double> xs) const override {
    return std::vector<double>(xs.size(), y_);
  }

 private:
  double y_;
};

// ============================================================================
// Curve implementations: Curves based on linear interpolation.
// ============================================================================

// For any given x coordinate, this binary search locates the
// applicable coefficients in a vector which is sorted by the
// maximal x value that the corresponding equation is applicable to.
// So, we compare with std::get<0>(c), the first element of the tuple.
template <typename C, typename T>
const C& FindCoefficients(const std::vector<C>& coefficients, T x) {
  return *std::lower_bound(coefficients.begin(), coefficients.end() - 1, x,
                           [](const C& c, T x) { return std::get<0>(c) < x; });
}

struct IdentityHook {
  // Called by CommonChecks (which applies to all curves); it's possible to
  // check curve-specific requirements here. Note that Hook::ValidateMin
  // is called at the end of CommonChecks, so it's OK to assume the properties
  // that CommonChecks verifies earlier.
  static Status ValidateMin(double min_x) { return Status(); }

  // Input transformation, applied before the interpolation.
  static float In(float x) { return x; }
  static double In(double x) { return x; }
};

// Helper function that maps a control point into a different space; this
// is used during curve construction.
template <typename Hook, typename T>
Point<T> TransformControlPoint(const Point<T>& in) {
  return Point<T>{Hook::In(in.x), in.y};
}

// Piecewise linear interpolation works like so: The x-axis is divided into
// areas by the x coordinates of the control points. For n points, there's n - 1
// areas between the points, each of which are covered by a linear equation of
// the form:
//     y = m * x + b.
template <typename T>
using LinearCoefficients =
    std::tuple<T /* the maximal x covered by the equation */, T /* m */,
               T /* b */>;
// The regions to the left and right outside the control points are not
// covered by the piecewise linear interpolation are dealt with directly
// within PWLCurveTmpl::Eval - they are not stored as coefficients, but the
// actual PWLCurveTmpl covers the range between -infinity and +infinity.
//
// Preconditions:
// - at least 1 point.
// - points must be sorted increasing by x coordinates, no duplicate x values.
template <typename T>
std::vector<LinearCoefficients<T>> PWLCoefficientsFor(
    bool net_increasing,
    typename std::vector<Point<T>>::const_iterator begin,
    typename std::vector<Point<T>>::const_iterator end) {
  DCHECK_LE(1, end - begin);
  if (end - begin == 1) {
    // There's exactly one point, and the curve isn't a constant. This happens
    // if you have infinity in an endpoint.
    // Note that if half the curve is constant, then the constant portion is
    // already covered by front or back - ::Eval checks for those and avoids
    // using the coefficients in that case.
    // The non-constant portion of the curve must go through the one and
    // only point, and it applies whenever the front/back portions don't
    // apply. The slope is a unit - either up or down.
    // We apply the point-slope formula to make the equation:
    // http://www.purplemath.com/modules/strtlneq2.htm
    //      y - y1 = m (x - x1)
    // <=>  y = m*x - m*x1 + y1
    // <=>  y = m*x - m*begin->x + begin->y
    T m = net_increasing ? 1 : -1;
    return {
        LinearCoefficients<T>{static_cast<T>(INF), m, begin->y - m * begin->x}};
  }
  std::vector<LinearCoefficients<T>> coefficients((end - begin) - 1);
  for (int ii = 0; ii < coefficients.size(); ++ii) {
    auto it = begin + ii;
    const Point<T>& current = *it;
    const Point<T>& next = *(it + 1);
    T dy = next.y - current.y;
    T dx = next.x - current.x;

    T m = dy / dx;
    T b = current.y - m * current.x;
    coefficients[ii] = LinearCoefficients<T>{next.x, m, b};
  }
  return coefficients;
}

// Evaluate a linear equation specified by |coefficients| for a given
// |x|.
template <typename T>
T EvaluateLinearEquation(const LinearCoefficients<T>& coefficients, T x) {
  T m = std::get<1>(coefficients);
  T b = std::get<2>(coefficients);
  return m * x + b;
}

// The piecewise linear curve template uses linear interpolation (also
// see PWLCoefficientsFor and the other routines and data structures above).
// The coefficients are constructed upon invoking the constructor of the
// template class.
// The template is parameterized with the type Hook, which fills in the
// behavior for the different PWL curve variants.
// For evaluation, this type provides:
// Hook::In: an input transformation for the x values (applied before
//           interpolation).
// Also see the comments on IdentityHook, the most basic hook type for this
// typename Template.
template <typename Hook>
class PWLCurveTmpl : public Curve {
 public:
  // Disallow copy and assign.
  PWLCurveTmpl(const PWLCurveTmpl&) = delete;
  PWLCurveTmpl& operator=(const PWLCurveTmpl&) = delete;

  PWLCurveTmpl(
      const Point<double>& front, const Point<double>& back,
      CurveType type,
      absl::Span<const Point<double>> points,
      typename std::vector<Point<double>>::const_iterator interior_begin,
      typename std::vector<Point<double>>::const_iterator interior_end,
      typename std::vector<Point<float>>::const_iterator f_interior_begin,
      typename std::vector<Point<float>>::const_iterator f_interior_end)
      : Curve(type, points),
        front_(front),
        back_(back),
        float_front_(front),
        float_back_(back),
        float_coefficients_(PWLCoefficientsFor<float>(
            front.y < back.y, f_interior_begin, f_interior_end)),
        double_coefficients_(PWLCoefficientsFor<double>(
            front.y < back.y, interior_begin, interior_end)) {}

  double Eval(double x) const override {
    if (x <= front_.x) {
      return front_.y;
    } else if (x >= back_.x) {
      return back_.y;
    }
    x = Hook::In(x);
    return EvaluateLinearEquation(FindCoefficients(double_coefficients_, x), x);
  }

  // Float evaluation is less precise but faster.
  float Eval(float x) const override {
    if (x <= float_front_.x) {
      return float_front_.y;
    } else if (x >= float_back_.x) {
      return float_back_.y;
    }
    x = Hook::In(x);
    return EvaluateLinearEquation(FindCoefficients(float_coefficients_, x), x);
  }

  // TODO: Vectorize this calculation using eigen.
  std::vector<double> Eval(absl::Span<const double> xs) const override {
    std::vector<double> ys;
    ys.reserve(xs.size());
    for (double x : xs) {
      ys.push_back(Eval(x));
    }
    return ys;
  }

  std::vector<float> Eval(absl::Span<const float> xs) const override {
    std::vector<float> ys;
    ys.reserve(xs.size());
    for (float x : xs) {
      ys.push_back(Eval(x));
    }
    return ys;
  }

 private:
  const Point<double> front_, back_;
  const Point<float> float_front_, float_back_;
  const std::vector<LinearCoefficients<float>> float_coefficients_;
  const std::vector<LinearCoefficients<double>> double_coefficients_;
};

// Specializes the PWLCurve typename Template to implement the Log1pPWLCurve.
struct Log1pHook {
  static Status ValidateMin(double min_x) {
    // For all p: p.x >= -1.
    if (min_x < -1)
      return Status(StatusCode::kInvalidArgument,
                    "X_VALUE_BELOW_MINUS_ONE_FOUND");
    return Status();
  }

  static float In(float x) { return log1p(x); }
  static double In(double x) { return log1p(x); }
};

// Specializes the PWLCurve typename Template to implement the
// Symlog1pPWLCurve.
struct Symlog1pHook {
  static Status ValidateMin(double min_x) {
    // Valid for all x.
    return Status();
  }

  static float In(float x) { return x >= 0 ? log1p(x) : -log1p(-x); }
  static double In(double x) { return x >= 0 ? log1p(x) : -log1p(-x); }
};

// Specializes the PWLCurve typename Template to implement the LogPWLCurve.
struct LogHook {
  static Status ValidateMin(double min_x) {
    // For all p: p.x >= 0.
    if (min_x < 0)
      return Status(StatusCode::kInvalidArgument, "NEGATIVE_X_VALUE_FOUND");
    return Status();
  }

  static float In(float x) {
    return std::log(x);
  }
  static double In(double x) {
    return std::log(x);
  }
};

bool IsStrictlyMonotonic(double a, double b, double c) {
  return ((a < b) && (b < c)) || ((a > b) && (b > c));
}

// This builder separates:
// - Checking a list of points for all requirements that are needed for
//   constructing a particular PWL curve. This is done in the constructor,
//   with the status made available via ::Status().
// - Constructing the curve; this is done by ::Build().
//
// The Hook template parameter implements all specializations for the
// PWLCurve variants (PWLCurve, LogPWLCurve, ...).
template <typename Hook>
class PWLCurveBuilder {
 public:
  // Disallow copy and assign.
  PWLCurveBuilder(const PWLCurveBuilder&) = delete;
  PWLCurveBuilder& operator=(const PWLCurveBuilder&) = delete;

  // Note that |points| are referenced by the state of this builder - that is,
  // their lifetime must be at least as long as that of this builder. However
  // once a curve is constructed via the ::Build() method, it no longer depends
  // on |points|.
  explicit PWLCurveBuilder(CurveType type,
                           absl::Span<const Point<double>> points)
      : type_(type), points_(points), constant_(false) {
    status_ = CommonChecks<Hook, Point<double>>(points, &begin_, &end_);
    if (!status_.ok()) return;
    if (end_ - begin_ == 1) {
      constant_ = true;
      return;
    }
    // At this point, we're looking at the Point<T> objects in the from
    // begin_ up to but excluding end_. We've made sure that:
    // - the points are with strictly increasing x coordinates
    // - custom checks (curve specific, but PWLCurve has none defined)
    // - points with duplicate y values eliminated from both sides
    // - there are at least two control points (at least one
    //   after redundancy elimination, and 2 since we handle constants just
    //   above here).

    // We now must figure out front and back point for the PWLCurve.
    // Please see PWLCurve::Eval - we distinguish:
    // (1) A constant area with y=front.y between x=-INF and x=front.x
    // (2) A piecewise linear area between front.x and back.x
    // (3) A constant area with y=back.y between x=back.x and x=INF.
    // Note that some curves don't have area (1) or area (3) - in that
    // case, we'll set front.x=-INF and/or back.x=INF.
    // In any case, at the end of this constructor, begin_ and end_
    // are setup to cover the area between front_ and back_.
    // Some curves rely on transformations over the control points - these
    // are satisfied by also maintaining
    // transformed_front_ / transformed_back_ during construction.

    // Now adjust begin_ and determine front_ / transformed_front_.
    // Asymptotes are not supported so we must detect them.
    front_ = *begin_;
    transformed_front_ = TransformControlPoint<Hook, double>(front_);
    back_ = *(end_ - 1);
    transformed_back_ = TransformControlPoint<Hook, double>(back_);
    // Here we need to check if infinity at the end is "unreachable", i.e. has
    // wrong sign. For example, suppose first 3 elements of points are:
    // {-INF, -INF}, {3, 5}, {14, 0}, ...
    // The first point should really be {-INF, +INF}, because the last segment,
    // if extended, will never reach {-INF, -INF}.
    if (end_ - begin_ >= 3) {
      if (isinf(transformed_front_.y) &&
          !IsStrictlyMonotonic(transformed_front_.y, (begin_ + 1)->y,
                               (begin_ + 2)->y)) {
        status_ = Status(StatusCode::kInvalidArgument, "WRONG_SIGN_INF_AT_END");
        return;
      }
      if (isinf(transformed_back_.y) &&
          !IsStrictlyMonotonic((end_ - 3)->y, (end_ - 2)->y,
                               transformed_back_.y)) {
        status_ = Status(StatusCode::kInvalidArgument, "WRONG_SIGN_INF_AT_END");
        return;
      }
    }

    if (isinf(transformed_front_.x)) {
      if (!isinf(transformed_front_.y)) {
        status_ = Status(StatusCode::kInvalidArgument,
                         "UNSUPPORTED_HORIZONTAL_ASYMPTOTE");
        return;
      }
      // Both x and y for the front are infinite. This means that we
      // adjust the interior of the curve to start at the second point.
      // This will be the left-most point that we feed to the
      // PWLCoefficientsFor method. Since front_.x is -INF, this
      // also disables the linear portion on the left side of the curve.
      // (See PWLCurveTmpl::Eval).
      ++begin_;
      if (begin_ == end_) {
        // While this curve has control points, they're all infinite
        // and their y-values aren't constant. So there is no
        // point through which to anchor the linear equation. Example:
        // PWLCurve({{-inf;-inf};{inf;inf}}).
        status_ = Status(StatusCode::kInvalidArgument,
                         "UNSUPPORTED_HORIZONTAL_ASYMPTOTE");
        return;
      }
      // Update the transformed front since we incremented begin_ above.
      // This will get used in ::Build().
      transformed_front_ = TransformControlPoint<Hook, double>(*begin_);
    } else if (isinf(transformed_front_.y)) {
      status_ = Status(StatusCode::kInvalidArgument,
                       "UNSUPPORTED_VERTICAL_ASYMPTOTE");
      return;
    }

    // And back_ / transformed_back_, mirroring the logic above.
    if (isinf(transformed_back_.x)) {
      if (!isinf(transformed_back_.y)) {
        status_ = Status(StatusCode::kInvalidArgument,
                         "UNSUPPORTED_HORIZONTAL_ASYMPTOTE");
        return;
      }
      --end_;
      if (begin_ == end_) {
        status_ =
            Status(StatusCode::kInvalidArgument, "NOT_ENOUGH_FINITE_POINTS");
        return;
      }
    } else if (isinf(transformed_back_.y)) {
      status_ = Status(StatusCode::kInvalidArgument,
                       "UNSUPPORTED_VERTICAL_ASYMPTOTE");
      return;
    }

    for (auto it = begin_; it < end_; ++it)
      if (isinf(it->y)) {
        status_ = Status(StatusCode::kInvalidArgument,
                         "INFINITE_INTERIOR_Y_COORDINATE");
        return;
      }

    // Given we checked for NOT_ENOUGH_FINITE_POINTS after adjusting
    // begin_ and end_, there's at least one point in the interior of the curve.
    DCHECK(begin_ != end_);
  }

  // Accesses the result for all checks required for this particular curve.
  Status status() const { return status_; }

  // Instantiates the appropriate curve.
  StatusOr<unique_ptr<Curve>> Build() const {
    if (!status_.ok()) return status_;
    if (constant_)
      return std::make_unique<ConstantCurve>(begin_->y, type_, points_);
    std::vector<Point<double>> transformed(end_ - begin_);
    for (int ii = 0; ii < transformed.size(); ++ii) {
      // Optimization: avoid recomputing TransformControlPoint<Hook> for the
      // front/back elements if we can help it.
      if (ii == 0 && front_.x == begin_->x)
        transformed.front() = transformed_front_;
      else if (ii == transformed.size() - 1 && back_.x == (end_ - 1)->x)
        transformed.back() = transformed_back_;
      else
        transformed[ii] = TransformControlPoint<Hook, double>(*(begin_ + ii));
    }
    // log(float(double x)) != float(log(double x)), so we have to perform the
    // transform separately for floats. Otherwise, a LogPWLCurve with (11, 13)
    // as a control point wouldn't evaluate to precisely 13 when x = 11.f.
    std::vector<Point<float>> float_transformed(end_ - begin_);
    for (int ii = 0; ii < float_transformed.size(); ++ii) {
      float_transformed[ii] =
          TransformControlPoint<Hook, float>(*(begin_ + ii));
    }
    return std::make_unique<PWLCurveTmpl<Hook>>(
        front_, back_, type_, points_,
        /*interior_begin=*/transformed.begin(),
        /*interior_end=*/transformed.end(), float_transformed.begin(),
        float_transformed.end());
  }

 private:
  CurveType type_;
  absl::Span<const Point<double>> points_;
  Status status_;
  typename absl::Span<const Point<double>>::const_iterator begin_;
  typename absl::Span<const Point<double>>::const_iterator end_;
  Point<double> front_;
  Point<double> transformed_front_;
  Point<double> back_;
  Point<double> transformed_back_;
  bool constant_;
};

// ============================================================================
// Curve instantiation
// ============================================================================
// The switch statements in the function below maps the enum constants to
// the appropriate hook types.

StatusOr<unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const Point<double>> points) {
  switch (type) {
    case PWLCurve:
      return PWLCurveBuilder<IdentityHook>(type, points).Build();
    case Log1pPWLCurve:
      return PWLCurveBuilder<Log1pHook>(type, points).Build();
    case LogPWLCurve:
      return PWLCurveBuilder<LogHook>(type, points).Build();
    case Symlog1pPWLCurve:
      return PWLCurveBuilder<Symlog1pHook>(type, points).Build();
  }
  return absl::InvalidArgumentError("Unsupported curve type");
}

}  // namespace internals

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const Point<double>> points) {
  return internals::NewCurve(type, points);
}

absl::StatusOr<std::unique_ptr<Curve>> NewCurveWithSeparator(
    char separator_char, absl::string_view spec) {
  CurveType type;
  std::vector<Point<double>> points;
  absl::Status status = internals::Parse(separator_char, spec, &type, &points);
  if (!status.ok()) return status;
  return NewCurve(type, points);
}

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(absl::string_view spec) {
  return NewCurveWithSeparator(';', spec);
}

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const double> x_ctrl_points,
    absl::Span<const double> y_ctrl_points) {
  if (x_ctrl_points.size() != y_ctrl_points.size()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "DIFFERENT_X_AND_Y_POINT_SIZE");
  }
  std::vector<Point<double>> points;
  points.reserve(x_ctrl_points.size());
  for (int i = 0; i < x_ctrl_points.size(); ++i) {
    points.push_back({x_ctrl_points[i], y_ctrl_points[i]});
  }
  return NewCurve(type, points);
}

absl::StatusOr<std::unique_ptr<Curve>> NewCurve(
    CurveType type, absl::Span<const float> x_ctrl_points,
    absl::Span<const float> y_ctrl_points) {
  if (x_ctrl_points.size() != y_ctrl_points.size()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "DIFFERENT_X_AND_Y_POINT_SIZE");
  }
  std::vector<Point<double>> points;
  points.reserve(x_ctrl_points.size());
  for (int i = 0; i < x_ctrl_points.size(); ++i) {
    points.push_back({x_ctrl_points[i], y_ctrl_points[i]});
  }
  return NewCurve(type, points);
}

absl::StatusOr<std::unique_ptr<Curve>> NewCurveWithAllowedSeparators(
    absl::string_view allowed_separators, absl::string_view spec) {
  int separator_used = internals::FindSeparatorUsed(allowed_separators, spec);
  if (separator_used == -1) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "MULTIPLE_SEPARATORS_IN_CURVE_SPEC");
  }
  return NewCurveWithSeparator(separator_used, spec);
}

// Serialize a Curve losslessly as a string.
std::string Curve::ToString() const {
  std::vector<std::string> point_strings;
  point_strings.reserve(points_.size());
  for (auto point : points_) {
    point_strings.push_back(internals::PointToString(point));
  }
  return absl::StrCat(internals::CurveType_Name(type_), "({",
                      absl::StrJoin(point_strings, ";"), "})");
}

}  // namespace arolla::pwlcurve
