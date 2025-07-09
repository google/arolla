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

#include <limits>
#include <list>
#include <cmath>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/util/status_macros_backport.h"  // IWYU pragma: keep


using absl::Status;
using absl::StatusCode;
using arolla::pwlcurve::internals::CurveType_Name;
using testing::ElementsAre;

namespace arolla::pwlcurve {
template <class T1, class T2>
inline bool operator==(const Point<T1>& p, const Point<T2>& q) {
  return p.x == q.x && p.y == q.y;
}

namespace {
constexpr double INF = std::numeric_limits<double>::infinity();

// Expect that the expression fails with this this status message.
#define EXPECT_STATUS_ERROR(message, expression) \
  EXPECT_THAT(expression,                        \
              absl_testing::StatusIs(StatusCode::kInvalidArgument, message))

// ============================================================================
// Test configuration to support testing curves,
// ============================================================================

template <typename T>
class FloatTypesTest : public ::testing::Test {
 public:
  typedef std::list<T> List;
};

using FloatTypes = ::testing::Types<float, double>;
TYPED_TEST_SUITE(FloatTypesTest, FloatTypes);

// ============================================================================
// Tests for the curve parser (the internals::Parse function).
// ============================================================================

TEST(ParseTest, ParseBasicExample) {
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_OK(
      internals::Parse(';', "Log1pPWLCurve({{0;1};{2;2};{4;8}})", &t, &points));
  EXPECT_EQ(Log1pPWLCurve, t);
  EXPECT_THAT(points, ElementsAre(Point<double>{0, 1}, Point<double>{2, 2},
                                  Point<double>{4, 8}));
}

TEST(ParseTest, ParseNumberFormats) {
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_OK(internals::Parse(';',
                             "PWLCurve({{0;1.0};{2.0e0; +3.0};"
                             "{+4.0e0;-5};{-6e0;-7.00e0}})",
                             &t, &points));
  EXPECT_EQ(PWLCurve, t);
  EXPECT_THAT(points, ElementsAre(Point<double>{0, 1}, Point<double>{2, 3},
                                  Point<double>{4, -5}, Point<double>{-6, -7}));
}

TEST(ParseTest, ParseSupportedSeparatorChars) {
  // We support two separator chars: ';' and '|'.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR(
      "INVALID_SEPARATOR_CHAR",
      internals::Parse('!', "PWLCurve({{1.0;1.0}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "INVALID_SEPARATOR_CHAR",
      internals::Parse('?', "PWLCurve({{1.0;1.0}})", &t, &points));
  EXPECT_OK(internals::Parse(';', "PWLCurve({{1.0;1.0}})", &t, &points));
  EXPECT_OK(internals::Parse('|', "PWLCurve({{1.0|1.0}})", &t, &points));
}

TEST(ParseTest, ParseInfinity) {
  CurveType t;
  std::vector<Point<double>> points;
  // Capitalization variations for infinity are accepted.
  EXPECT_OK(
      internals::Parse(';', "PWLCurve({{-inf;0};{INF;+Inf}})", &t, &points));
  EXPECT_THAT(points,
              ElementsAre(Point<double>{-INF, 0}, Point<double>{INF, INF}));
}

TEST(ParseTest, ParseLengthOneToThree) {
  CurveType t;
  std::vector<Point<double>> points;

  // We don't accept curves without any points.
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({})", &t, &points));

  // A curve with 1 point.
  EXPECT_OK(internals::Parse(';', "PWLCurve({{1.5;2.5}})", &t, &points));
  EXPECT_THAT(points, ElementsAre(Point<double>{1.5, 2.5}));

  // 2 points.
  EXPECT_OK(
      internals::Parse(';', "PWLCurve({{5.5;2.5};{3.5;4.5}})", &t, &points));
  EXPECT_THAT(points,
              ElementsAre(Point<double>{5.5, 2.5}, Point<double>{3.5, 4.5}));

  // 3 points.
  EXPECT_OK(
      internals::Parse('|', "PWLCurve({{1|1}|{2|2}|{3|3}})", &t, &points));
  EXPECT_THAT(points, ElementsAre(Point<double>{1, 1}, Point<double>{2, 2},
                                  Point<double>{3, 3}));
}

TEST(ParseTest, ParseMissingOpenParen) {
  // An opening paren is required in any curve specification.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR("MISSING_OPEN_PAREN",
                      internals::Parse(';', "", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPEN_PAREN",
                      internals::Parse(';', "PWLCurve", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_OPEN_PAREN",
      internals::Parse(';', "PWLCurve{{1.0;1.0}})", &t, &points));
  EXPECT_OK(internals::Parse(';', "PWLCurve({{1.0;1.0}})", &t, &points));
}

TEST(ParseTest, ParseCurveType) {
  // The parser recognizes the names of the CurveType enum values.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR(
      "UNKNOWN_CURVE_TYPE",
      internals::Parse(';', "CrazyCurve({{1.0;1.0}})", &t, &points));
  EXPECT_OK(internals::Parse(';', "PWLCurve({{1.0;1.0}})", &t, &points));
  EXPECT_EQ(PWLCurve, t);
  EXPECT_OK(internals::Parse(';', "LogPWLCurve({{1.0;1.0}})", &t, &points));
  EXPECT_EQ(LogPWLCurve, t);
}

TEST(ParseTest, ParseMissingOpeningCurlies) {
  // Many things can go wrong when opening curlies are misplaced or missing!
  // Some of the test cases exemplify limitations with our error reporting -
  // it's sometimes non-intuitive why the parser prefers reporting a
  // particular error. Yet a quick cheap hint may be better than nothing.
  // At least we can distinguish between points and point lists.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT_LIST",
                      internals::Parse(';', "PWLCurve(", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT_LIST",
                      internals::Parse(';', "PWLCurve(junk", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT_LIST",
                      internals::Parse(';', "PWLCurve()", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({junk", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({})", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_OPENING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({}junk", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_OPENING_CURLY_FOR_POINT",
      internals::Parse(';', "PWLCurve({;{1.5;2.5}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_OPENING_CURLY_FOR_POINT",
      internals::Parse(';', "PWLCurve({{1.5;2.5};}", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_OPENING_CURLY_FOR_POINT",
      internals::Parse(';', "PWLCurve({;{1.5;2.5};{3.5;4.5}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_OPENING_CURLY_FOR_POINT",
      internals::Parse(';', "PWLCurve({{1.5;2.5};{3.5;4.5};})", &t, &points));
}

TEST(ParseTest, ParseMissingClosingCurlies) {
  // The error reporting distinguishes between points and point lists for
  // closing curlies.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR("MISSING_CLOSING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({{1;1", &t, &points));
  EXPECT_STATUS_ERROR("MISSING_CLOSING_CURLY_FOR_POINT",
                      internals::Parse(';', "PWLCurve({{1;1junk", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_CURLY_FOR_POINT",
      internals::Parse(';', "PWLCurve({{1;1};{2;", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_CURLY_FOR_POINT",
      internals::Parse(';', "PWLCurve({{1;1};{2;junk", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_CURLY_OR_SEPARATOR_FOR_POINT_LIST",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_CURLY_OR_SEPARATOR_FOR_POINT_LIST",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}junk", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_CURLY_OR_SEPARATOR_FOR_POINT_LIST",
      internals::Parse(';', "PWLCurve({{1.5;2.5},{3.5;4.5}})", &t, &points));
}

TEST(ParseTest, ParseMissingClosingParenAndTrailingJunk) {
  // All curve expressions end with a paren. Trailing junk is not permitted.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_PAREN",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}}", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_PAREN",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}}", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_PAREN",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}}junk", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_CLOSING_PAREN",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "TRAILING_JUNK_AFTER_CLOSING_PAREN",
      internals::Parse(';', "PWLCurve({{1;1};{2;2}})junk", &t, &points));
}

TEST(ParseTest, ParseMissingSeparatorBetweenPointCoordinates) {
  // Between the coordinates of any point, a separator char is required.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR(
      "MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES",
      internals::Parse(';', "PWLCurve({{1,2.5}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES",
      internals::Parse(';', "PWLCurve({{1.5,2}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES",
      internals::Parse(';', "PWLCurve({{1.5,2.5};{3;4.5}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "MISSING_SEPARATOR_BETWEEN_POINT_COORDINATES",
      internals::Parse(';', "PWLCurve({{1.5,2.5};{3.5;d}})", &t, &points));
}

TEST(ParseTest, ParseNonNumbersAreInvalidDoubles) {
  // These examples show that the parser requires numeric coordinates - it
  // doesn't like characters instead of floating point numbers.
  CurveType t;
  std::vector<Point<double>> points;
  EXPECT_STATUS_ERROR(
      "INVALID_DOUBLE",
      internals::Parse(';', "PWLCurve({{a;2.5}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "INVALID_DOUBLE",
      internals::Parse(';', "PWLCurve({{1.5;b}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "INVALID_DOUBLE",
      internals::Parse(';', "PWLCurve({{1.5;2.5};{c;4.5}})", &t, &points));
  EXPECT_STATUS_ERROR(
      "INVALID_DOUBLE",
      internals::Parse(';', "PWLCurve({{1.5;2.5};{3.5;d}})", &t, &points));
  // This y coordinate is empty.
  EXPECT_STATUS_ERROR("INVALID_DOUBLE",
                      internals::Parse(';', "PWLCurve({{1.5;}})", &t, &points));
}

TEST(FindSeparatorUsedTest, YieldSeparatorUsedOrMinusOne) {
  // Base case: just one separator allowed and that's the one that's used.
  EXPECT_EQ(';', internals::FindSeparatorUsed(";", "PWLCurve({{1;1};{2;2}})"));
  // Still one separator allowed, and it occurs in the curve. '|' occurs in the
  // curve as well but it's not an allowed separator here, so it would just
  // be a syntax error.
  EXPECT_EQ(';', internals::FindSeparatorUsed(";", "PWLCurve({{1;1}|{2|2}})"));
  // Now both ';' and '|' are allowed and both are used, so we get -1.
  EXPECT_EQ(-1, internals::FindSeparatorUsed(";|", "PWLCurve({{1;1}|{2|2}})"));
  // Now both ';' and '|' are allowed and both are used, so we get -1.
  EXPECT_EQ('|', internals::FindSeparatorUsed(";|", "PWLCurve({{1|1}|{2|2}})"));
  // If allowed_separators contains a dupe, the return value is -1.
  EXPECT_EQ(-1, internals::FindSeparatorUsed("|;|", "PWLCurve({{1|1}|{2|2}})"));
  // If allowed_separators is empty, the return value is -1.
  EXPECT_EQ(-1, internals::FindSeparatorUsed("", "PWLCurve({{1|1}|{2|2}})"));
}

// ============================================================================
// Custom matchers for testing curves.
// ============================================================================

// These matchers are designed to make the tests for curves easier to work with,
// by providing descriptive error messages and by allowing compact test cases.
// Example:
//   unique_ptr<Curve> curve = ...
//   EXPECT_THAT(curve, EvalsNear(1e-4, {{0, 2}, {1, 3}, {2, 4}}));
// This means:
//   - we're expecting that the curve evaluates through these three points;
//     e.g., for x=0 we expect y=2 as an evaluation result, and for x=1 we
//     expect y=3, etc.
//   - we're allowing 1e-4 delta from the actual value.
// This matcher fully supports comparisons with infinity (e.g., INF, -INF).
// It reports all observed discrepancies that don't fit within the provided
//   delta.
// If no delta is desired (strict comparison), it's fine to pass 0 as the delta.
// Note that the matcher isn't invoked directly - instead we use the convenience
// method below to teach C++ that the argument is a vector<Point<double>>.
MATCHER_P2(EvalsNearPoints, delta, points, "") {
  bool success = true;
  // Verify pointwise Eval.
  for (auto& p : points) {
    auto y = arg->Eval(p.x);
    if (p.y == y)  // This covers comparisons with infinity.
      continue;
    if (std::abs(p.y - y) <= delta)
      continue;
    success = false;
    *result_listener << "\nfor x=" << p.x
                     << " saw y=" << y
                     << " (expected " << p.y << "; diff = "
                     << (p.y - y) << ")";
  }

  if (!success) return success;

  // If pointwise match succeeds, also verify vectorized Eval.
  // std::vector can't implicitly cast between <float> and <double>, so we have
  // to pass Curve arg a vector that matches arg's template.
  // We determine that type with decltype of an arg->Eval(x) output.
  // Inelegant but it works.
  if (points.empty()) return success;
  std::vector<decltype(arg->Eval(points[0].x))> pxs;
  pxs.reserve(points.size());
  for (auto& p : points) {
    pxs.push_back(p.x);
  }

  // Perform vectorized Eval and compare each output y to the expected y.
  auto ys = arg->Eval(pxs);
  for (int i = 0; i < points.size(); ++i) {
    auto y = ys[i];
    auto p = points[i];
    if (p.y == y)  // This covers comparisons with infinity.
      continue;
    if (std::abs(p.y - y) <= delta)
      continue;
    success = false;
    *result_listener << "\nfor x=" << p.x
                     << " saw y=" << y
                     << " (expected " << p.y << "; diff = "
                     << (p.y - y) << ")";
  }
  if (!success) {
    *result_listener << "\nVectorized Eval() failed.";
  }

  return success;
}

template <class T>
inline EvalsNearPointsMatcherP2<T, std::vector<Point<T>>> EvalsNear(
    T delta, const std::vector<Point<T>>& points) {
  return EvalsNearPointsMatcherP2<T, std::vector<Point<T>>>(delta, points);
}

// Example:
//   unique_ptr<Curve> curve = ...
//   EXPECT_THAT(curve, EvalsLt(2, 5));
// This means that we're evaluating curve for x=2, and that we're expecting
// that the result be less than 5.
MATCHER_P2(EvalsLt, x, y, "") {
  double actual_y = arg->Eval(x);
  if (actual_y < y)
    return true;
  *result_listener << "for x=" << x << " saw y=" << actual_y
                   << "; required y<" <<  y;
  return false;
}

// Example:
//   unique_ptr<Curve> curve = ...
//   EXPECT_THAT(curve, EvalsGt(2, 5));
// This means that we're evaluating curve for x=2, and that we're expecting
// that the result be greater than 5.
MATCHER_P2(EvalsGt, x, y, "") {
  double actual_y = arg->Eval(x);
  if (actual_y > y)
    return true;
  *result_listener << "for x=" << x << " saw y=" << actual_y
                   << "; required y>" <<  y;
  return false;
}

TEST(NewCurveTest, CurveIsValid) {
  EXPECT_OK(NewCurve("LogPWLCurve({{1;0};{2;10};{3;11};{11;12}})"));
  EXPECT_OK(NewCurve("PWLCurve({{1;0};{2;10};{3;11};{11;12}})"));
}

TYPED_TEST(FloatTypesTest, CurveParallelVectors) {
  auto curve = NewCurve(PWLCurve, absl::Span<const float>{1, 2, 3, 4},
                        absl::Span<const float>{10, 20, 30, 40});
  EXPECT_OK(curve);
  EXPECT_DOUBLE_EQ(15, curve.value()->Eval(static_cast<TypeParam>(1.5)));
}

TYPED_TEST(FloatTypesTest, PointFromPointVectors) {
  // Test the automatic casting from Point<float> to Point<T>.
  auto points = std::vector<Point<TypeParam>>(
      {Point<float>(1, 1), Point<float>(2, 2), Point<float>(3, 3)});
}

TEST(LegacyCurveTypes, TypesAreEquivalent) {
  EXPECT_EQ(Log1pPWLCurve, LogP1PWLCurve);
  EXPECT_EQ(Symlog1pPWLCurve, SymmetricLogP1PWLCurve);
}

std::vector<CurveType> AllCurveTypes() {
  std::vector<CurveType> types;
  types.push_back(PWLCurve);
  types.push_back(LogPWLCurve);
  types.push_back(Log1pPWLCurve);
  types.push_back(Symlog1pPWLCurve);
  return types;
}

TEST(CurveTypeTest, IsValidCurveType) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));
    EXPECT_TRUE(IsValidCurveType(type));
  }

  // Explicitly check all values from [CurveTypeMin, CurveTypeMax].
  for (int v = CurveTypeMin; v <= CurveTypeMax; ++v) {
    EXPECT_TRUE(IsValidCurveType(v));
  }

  EXPECT_FALSE(IsValidCurveType(CurveTypeMin - 1));
  EXPECT_FALSE(IsValidCurveType(CurveTypeMax + 1));
}

TEST(CurveTest, ControlPointsAndTypeTest) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    const std::vector<Point<double>> points = {{1, 10}, {2, 20}, {3, 40}};
    ASSERT_OK_AND_ASSIGN(
      auto curve,
        NewCurve(type, points));
    EXPECT_EQ(points, curve->ControlPoints());
    EXPECT_EQ(type, curve->Type());
  }
}

TEST(CurveTest, ConstantCurveControlPointsAndTypeTest) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    const std::vector<Point<double>> points = {{1, 10}, {2, 10}, {3, 10}};
    ASSERT_OK_AND_ASSIGN(
      auto curve,
        NewCurve(type, points));
    EXPECT_EQ(points, curve->ControlPoints());
    EXPECT_EQ(type, curve->Type());
  }
}

TEST(SerializationTest, ToStringPreservesCurveTypeAndPoints) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    std::vector<Point<double>> points = {{1.2, 2.3}, {3.4, 5.5}};
    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve(type, points));
    ASSERT_OK_AND_ASSIGN(auto round_trip_curve, NewCurve(curve->ToString()));
    EXPECT_EQ(type, round_trip_curve->Type());
    // Test that the exact floating point representation is preserved.
    EXPECT_EQ(points, round_trip_curve->ControlPoints());
  }
}

TEST(SerializationTest, ToStringIsConcise) {
  std::string curve_string = "LogPWLCurve({{0.54;1.1231};{2.0192;2.959}})";
  ASSERT_OK_AND_ASSIGN(auto curve, NewCurve(curve_string));
  EXPECT_EQ(curve_string, curve->ToString());
}

TEST(SerializationTest, ToStringIsPrecise) {
  const double x = 1.23456789011121314151617;
  const double y = std::nextafter(x, x + 1);
  EXPECT_NE(x, y);
  std::vector<Point<double>> points = {{x, y}, {x + 1, y + 1}};

  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve(type, points));
    ASSERT_OK_AND_ASSIGN(auto round_trip_curve, NewCurve(curve->ToString()));
    EXPECT_EQ(curve->ToString(), round_trip_curve->ToString());
    EXPECT_EQ(curve->Type(), round_trip_curve->Type());
    // Test that the exact floating point representation is preserved.
    EXPECT_EQ(curve->ControlPoints(), round_trip_curve->ControlPoints());
  }
}

TEST(SerializationTest, ToStringIsPreciseForLargeDoubles) {
  EXPECT_TRUE(-0.0 == +0.0);
  const double x = 123456789101112131415161718192021222324252627282930.313233;
  const double y = std::nextafter(x, 10 * x);
  EXPECT_NE(x, y);
  std::vector<Point<double>> points = {{x, y}, {1.7 * x, 1.7 * y}};

  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve(type, points));
    ASSERT_OK_AND_ASSIGN(auto round_trip_curve, NewCurve(curve->ToString()));
    EXPECT_EQ(curve->ToString(), round_trip_curve->ToString());
    EXPECT_EQ(curve->Type(), round_trip_curve->Type());
    // Test that the exact floating point representation is preserved.
    EXPECT_EQ(curve->ControlPoints(), round_trip_curve->ControlPoints());
  }
}

// ============================================================================
// Constant curve, based on any curve spec (PWLCurve, LogPWLCurve, etc.).
// ============================================================================
TYPED_TEST(FloatTypesTest, ConstantCurves) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    // All of these specs define curves that are straight lines - basically y=3.
    std::vector<std::string> point_specs = {
        "{{11;3}}",
        "{{inf;3}}",
        "{{-inf;3};{inf;3}}",
        "{{-inf;3};{1;3};{11;3};{inf;3}}",
    };
    for (const std::string& point_spec : point_specs) {
      std::string spec =
          absl::StrCat(CurveType_Name(type), "(", point_spec, ")");
      SCOPED_TRACE(absl::StrCat("spec=", spec));
      ASSERT_OK_AND_ASSIGN(auto curve, NewCurve(spec));

      // Should be 3 everywhere
      EXPECT_THAT(
          curve,
          EvalsNear(static_cast<TypeParam>(0.0),
                    {{-1000, 3}, {-100, 3}, {0, 3}, {100, 3}, {1000, 3}}));
      EXPECT_EQ(curve->Eval(100.), 3);

      std::vector<TypeParam> input_xs = {-1000, -100, 0, 100, 1000};
      auto ys = curve->Eval(input_xs);
      std::vector<TypeParam> expected_ys = {3, 3, 3, 3, 3};
      EXPECT_EQ(ys, expected_ys);
    }
  }
}

TYPED_TEST(FloatTypesTest, ConstantCurvesWithInf) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    // All of these specs define curves that are y=-inf.
    std::vector<std::string> point_specs = {
        "{{3;-inf}}",
        "{{inf;-inf}}",
        "{{0;-inf};{1;-inf}}",
        "{{-inf;-inf};{-1;-inf};{1;-inf};{inf;-inf}}",
    };
    for (const std::string& point_spec : point_specs) {
      std::string spec =
          absl::StrCat(CurveType_Name(type), "(", point_spec, ")");
      SCOPED_TRACE(absl::StrCat("spec=", spec));
      ASSERT_OK_AND_ASSIGN(auto curve, NewCurve(spec));

      // Should be -INF everywhere.
      EXPECT_THAT(
          curve,
          EvalsNear(static_cast<TypeParam>(0.0),
                    {{-1000, -std::numeric_limits<TypeParam>::infinity()},
                     {-100, -std::numeric_limits<TypeParam>::infinity()},
                     {0, -std::numeric_limits<TypeParam>::infinity()},
                     {100, -std::numeric_limits<TypeParam>::infinity()}}));
    }
  }
}

TEST(NewCurveTest, TrivialHorizontalAsymptotesAdjacentToConstantSegment) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    EXPECT_OK(NewCurve(type, {{1, 1}, {2, 2}, {3, 5}, {4, 5}, {INF, 5}}));
    EXPECT_OK(NewCurve(type, {{1, 8}, {2, 6}, {3, 5}, {4, 5}, {INF, 5}}));

    // Log1pPWLCurve and LogPWLCurve don't
    // allow negative x coordinates (or coordinates below -1, respectively).
    if (type == Log1pPWLCurve || type == LogPWLCurve) continue;
    EXPECT_OK(NewCurve(type, {{-INF, 5}, {1, 5}, {2, 5}, {3, 6}, {4, 7}}));
    EXPECT_OK(NewCurve(type, {{-INF, 5}, {1, 5}, {2, 5}, {3, 4}, {4, 3}}));
  }
}

TYPED_TEST(FloatTypesTest, NanToNan) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));
    const TypeParam nan = std::numeric_limits<TypeParam>::quiet_NaN();

    auto curve = NewCurve(type, {{1, 1}, {2, 2}, {3, 5}, {4, 5}, {INF, 5}});
    EXPECT_TRUE(std::isnan(curve.value()->Eval(nan)));
  }
}

// ============================================================================
// PWLCurve
// ============================================================================
TYPED_TEST(FloatTypesTest, PWLCurve) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("PWLCurve({{0;-10};{2;10};{3;11};{5;12};{100;100}})"));

  // Check value at control points.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4),
                        {{0, -10}, {2, 10}, {3, 11}, {5, 12}, {100, 100}}));

  // Should be linear between control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 0}, {2.5, 10.5}, {4, 11.5}, {52.5, 56}}));

  // Should be constant above and below maximum control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{-1000, -10}, {1000, 100}}));
}

TYPED_TEST(FloatTypesTest, PWLCurveDecreasing) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("PWLCurve({{0;100};{2;12};{3;11};{5;10};{100;-10}})"));

  // Check value at control points.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4),
                        {{0, 100}, {2, 12}, {3, 11}, {5, 10}, {100, -10}}));

  // Should be linear between control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 56}, {2.5, 11.5}, {4, 10.5}, {52.5, 0}}));

  // Should be constant above and below maximum control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{-1000, 100}, {1000, -10}}));
}

TYPED_TEST(FloatTypesTest, PWLCurveWithConstantRegion) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("PWLCurve({{-INF;-INF};{0;2};{1;3};{11;3};{12;4};{INF;INF}})"));

  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{-100, -98}, {0, 2}, {0.5, 2.5}}));
  EXPECT_THAT(curve, EvalsLt(static_cast<TypeParam>(1 - 1e-3), 3));

  // Should be 3 between 1 and 11.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 3}, {5, 3}, {11, 3}}));
  EXPECT_THAT(curve, EvalsGt(static_cast<TypeParam>(11 + 1e-3), 3));
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{11.5, 3.5}, {12, 4}, {100, 92}}));
}

TYPED_TEST(FloatTypesTest, TwoPointPWLCurve) {
  // PWLCurve works with 2 points.
  ASSERT_OK_AND_ASSIGN(auto curve, NewCurve("PWLCurve({{1;1};{2;2}})"));
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(0),
                        {{-10, 1}, {1, 1}, {1.5, 1.5}, {2, 2}, {10, 2}}));
}

TYPED_TEST(FloatTypesTest, TwoPointPWLCurveDecreasing) {
  // PWLCurve works with 2 points.
  ASSERT_OK_AND_ASSIGN(auto curve, NewCurve("PWLCurve({{1;2};{2;1}})"));
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(0),
                        {{-10, 2}, {1, 2}, {1.5, 1.5}, {2, 1}, {10, 1}}));
}

TYPED_TEST(FloatTypesTest, PWLCurveWithInf) {
  {  // Linear with slope 1 before 0, after that constant.
    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve("PWLCurve({{-INF;-INF};{0;0}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1000, -1000}, {-100, -100}, {0, 0}, {10, 0}, {100, 0}}));
  }
  {  // Linear with slope 1 before 3, after that constant (1.5).
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;-INF};{3;1.5}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-100, -101.5},
                                                                {0, -1.5},
                                                                {1, -0.5},
                                                                {2, 0.5},
                                                                {3, 1.5},
                                                                {10, 1.5},
                                                                {100, 1.5}}));
  }
  {  // Linear with slope -1 before 0, after that constant.
    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve("PWLCurve({{-INF;INF};{0;0}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1000, 1000}, {-100, 100}, {0, 0}, {10, 0}, {100, 0}}));
  }
  {  // Linear with slope -1 before 3, after that constant (1.5).
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;INF};{3;1.5}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-1000, 1004.5},
                                                                {-100, 104.5},
                                                                {0, 4.5},
                                                                {1, 3.5},
                                                                {2, 2.5},
                                                                {3, 1.5},
                                                                {10, 1.5},
                                                                {100, 1.5}}));
  }
  {  // Curve through a single control point with a slope of 1.0.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;-INF};{3;5.1};{INF;INF}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-1000, -997.9},
                                                                {-100, -97.9},
                                                                {0, 2.1},
                                                                {3, 5.1},
                                                                {10, 12.1},
                                                                {100, 102.1}}));
  }
  {  // Linear with slope 0.5 up to 2, then constant.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;-INF};{0;0};{2;1}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-1000, -500},
                                                                {-100, -50},
                                                                {0, 0},
                                                                {1, 0.5},
                                                                {2, 1},
                                                                {10, 1},
                                                                {100, 1}}));
  }
  {  // Constant before 0, then linear with slope 1.
    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve("PWLCurve({{0;0};{INF;INF}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1000, 0}, {-100, 0}, {0, 0}, {10, 10}, {100, 100}}));
  }
  {  // Constant before 0, then linear with slope -1.
    ASSERT_OK_AND_ASSIGN(auto curve, NewCurve("PWLCurve({{0;0};{INF;-INF}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1000, 0}, {-100, 0}, {0, 0}, {10, -10}, {100, -100}}));
  }
  {  // Constant before -2, then linear with slope -0.5.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-2;1};{0;0};{inf;-inf}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-1000, 1},
                                                                {-100, 1},
                                                                {-2, 1},
                                                                {0, 0},
                                                                {10, -5},
                                                                {10, -5},
                                                                {100, -50}}));
  }
  {  // Linear with slope 1 before 10, and constant after 10.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;-INF};{0;0};{10;10}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1000, -1000}, {-100, -100}, {0, 0}, {10, 10}, {100, 10}}));
  }
  {  // Linear with slope 1 after -10, and constant before -10.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-10;-10};{0;0};{INF;INF}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-100, -10}, {-10, -10}, {0, 0}, {100, 100}, {1000, 1000}}));
  }
  {  // Linear with slope 1 everywhere.
    ASSERT_OK_AND_ASSIGN(
        auto curve, NewCurve("PWLCurve({{-INF;-INF};{0;0};{1;1};{INF;INF}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-1000, -1000},
                                                                {-100, -100},
                                                                {0, 0},
                                                                {10, 10},
                                                                {100, 100},
                                                                {1000, 1000}}));
  }
  {  // Linear with slope -1 after -10, and constant before -10.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-10;10};{0;0};{INF;-INF}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-100, 10}, {-10, 10}, {0, 0}, {100, -100}, {1000, -1000}}));
  }
  {  // Linear with slope -1 before 0, and constant after 0.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;INF};{-10;10};{0;0}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-100, 100}, {-10, 10}, {0, 0}, {100, 0}, {1000, 0}}));
  }
  {  // Linear with slope -1 everywhere.
    ASSERT_OK_AND_ASSIGN(
        auto curve,
        NewCurve("PWLCurve({{-INF;INF};{-10;10};{0;0};{INF;-INF}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(
            static_cast<TypeParam>(1e-4),
            {{-100, 100}, {-10, 10}, {0, 0}, {100, -100}, {1000, -1000}}));
  }
  {  // Linear with slope 2 before 10, and constant after 10.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;-INF};{0;0};{10;20}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1000, -2000}, {-100, -200}, {0, 0}, {10, 20}, {100, 20}}));
  }
}

TYPED_TEST(FloatTypesTest, PWLCurveWithRedundantPoints) {
  {  // Linear with slope 1 between -10 and 10 with redundant points.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;-10};{-100;-10};{-"
                                  "10;-10};{0;0};{10;10};{INF;10}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-150, -10},
                                                                {-100, -10},
                                                                {-50, -10},
                                                                {-10, -10},
                                                                {0, 0},
                                                                {10, 10},
                                                                {50, 10},
                                                                {100, 10},
                                                                {150, 10}}));
  }
  {  // Linear with slope 1 between -10 and 10 with redundant points.
    ASSERT_OK_AND_ASSIGN(
        auto curve,
        NewCurve("PWLCurve({{-100;-10};{-10;-10};{0;0};{10;10};{INF;10}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-150, -10},
                                                                {-100, -10},
                                                                {-50, -10},
                                                                {-10, -10},
                                                                {0, 0},
                                                                {10, 10},
                                                                {50, 10},
                                                                {100, 10},
                                                                {150, 10}}));
  }
  {  // Linear with slope -1 between -10 and 10 with redundant points.
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{-INF;10};{-10;10};{0;0};{"
                                  "10;-10};{100;-10};{INF;-10}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-150, 10},
                                                                {-100, 10},
                                                                {-50, 10},
                                                                {-10, 10},
                                                                {0, 0},
                                                                {10, -10},
                                                                {50, -10},
                                                                {100, -10},
                                                                {150, -10}}));
  }
  {  // Linear with slope -1 between -10 and 10 with redundant points.
    ASSERT_OK_AND_ASSIGN(
        auto curve, NewCurve("PWLCurve({{-10;10};{0;0};{10;-10};{100;-10}})"));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-150, 10},
                                                                {-100, 10},
                                                                {-50, 10},
                                                                {-10, 10},
                                                                {0, 0},
                                                                {10, -10},
                                                                {50, -10},
                                                                {100, -10},
                                                                {150, -10}}));
  }
}

TYPED_TEST(FloatTypesTest, NonMonotonicPWLCurve) {
  {
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{1;1};{2;3};{3;1};{4;10}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(
            static_cast<TypeParam>(1e-4),
            {{1, 1}, {1.5, 2}, {2, 3}, {2.5, 2}, {3.5, 5.5}, {3.25, 3.25}}));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto curve,
                         NewCurve("PWLCurve({{0;0};{1;-1};{2;1}})"));
    EXPECT_THAT(
        curve,
        EvalsNear(static_cast<TypeParam>(1e-4),
                  {{-1, 0}, {0, 0}, {0.5, -0.5}, {1, -1}, {1.5, 0}, {2, 1}}));
  }
}

// ============================================================================
// Log1pPWLCurve
// ============================================================================
TYPED_TEST(FloatTypesTest, Log1pPWLCurveLegacyString) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("Log1pPWLCurve({{1;2};{2;10};{10;11};{11;13};{INF;INF}})"));
  ASSERT_OK_AND_ASSIGN(
      auto legacy_curve,
      NewCurve("LogP1PWLCurve({{1;2};{2;10};{10;11};{11;13};{INF;INF}})"));

  EXPECT_EQ(curve->ToString(), legacy_curve->ToString());
  EXPECT_EQ(curve->Type(), legacy_curve->Type());
  EXPECT_EQ(curve->ControlPoints(), legacy_curve->ControlPoints());
}

TYPED_TEST(FloatTypesTest, Log1pPWLCurveBoundedLeft) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("Log1pPWLCurve({{1;2};{2;10};{10;11};{11;13};{INF;INF}})"));

  // The curve is constant before 1.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(0),
                               {{0, 2}, {-2, 2}, {-100, 2}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve extends to (+INF, +INF).
  EXPECT_THAT(curve, EvalsGt(static_cast<TypeParam>(13), 15));
}

TYPED_TEST(FloatTypesTest, Log1pPWLCurveBoundedRight) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("Log1pPWLCurve({{-1;-INF};{1;2};{2;10};{10;11};{11;13}})"));

  // The curve goes towards -INF when x goes closer to -1.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{-0.5, -25.3522}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant before -1.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4),
                        {{-2, -std::numeric_limits<TypeParam>::infinity()},
                         {-100, -std::numeric_limits<TypeParam>::infinity()}}));
}

TYPED_TEST(FloatTypesTest, Log1pPWLCurveBoundedTwoSides) {
  ASSERT_OK_AND_ASSIGN(
      auto curve, NewCurve("Log1pPWLCurve({{1;2};{2;10};{10;11};{11;13}})"));

  // The curve is constant before 1.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{0, 2}, {-2, 2}, {-100, 2}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant above 11.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{12, 13}, {100, 13}}));
}

// Verifies that the interpolation for Log1pPWLCurve occurs in log(x+1)
// space.
TYPED_TEST(FloatTypesTest, Log1pPWLCurveLog1pInterpolation) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("Log1pPWLCurve({{1;1};{10;10};{INF;INF}})"));
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{1, 1}, {10, 10}}));

  const TypeParam m = (10 - 1) / (std::log(10 + 1) - std::log(1 + 1));
  const TypeParam b = 10 - m * std::log(10 + 1);
  for (TypeParam x : {2, 5, 8}) {
    SCOPED_TRACE(absl::StrCat("x=", x));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                                 {{x, m * std::log(x + 1) + b}}));
  }
}

TYPED_TEST(FloatTypesTest, Log1pPWLCurveNewCurveWithCurveType) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve(Log1pPWLCurve, {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant before 1.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{0, 2}, {-2, 2}, {-100, 2}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant above 11.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{12, 13}, {100, 13}}));
}

TYPED_TEST(FloatTypesTest, Log1pPWLCurveNonMonotonic) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("Log1pPWLCurve({{0;2};{1;1};{2;2};{3;1}})"));

  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-100, 2},
                                                              {-10, 2},
                                                              {-1.5, 2},
                                                              {-1, 2},
                                                              {0, 2},
                                                              {1, 1},
                                                              {2, 2},
                                                              {3, 1},
                                                              {4, 1},
                                                              {0.5, 1.41504},
                                                              {1.5, 1.55034},
                                                              {2.5, 1.46416}}));
}

// ============================================================================
// Symlog1pPWLCurve
// ============================================================================
TYPED_TEST(FloatTypesTest, Symlog1pPWLCurveLegacyString) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve(
          "Symlog1pPWLCurve({{-INF;-INF};{-5;-5};{-2;-1};{1;2};{INF;INF}})"));
  ASSERT_OK_AND_ASSIGN(auto legacy_curve,
                       NewCurve("SymmetricLogP1PWLCurve({{-INF;-INF};{-5;-5};{-"
                                "2;-1};{1;2};{INF;INF}})"));

  EXPECT_EQ(curve->ToString(), legacy_curve->ToString());
  EXPECT_EQ(curve->Type(), legacy_curve->Type());
  EXPECT_EQ(curve->ControlPoints(), legacy_curve->ControlPoints());
}

TYPED_TEST(FloatTypesTest, Symlog1pPWLCurveUnbounded) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("Symlog1pPWLCurve({{-INF;-INF};{-"
                                "5;-5};{-2;-1};{1;2};{INF;INF}})"));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{-5, -5}, {-2, -1}, {1, 2}}));

  // The curve extends to (+INF, +INF).
  EXPECT_THAT(curve, EvalsGt(static_cast<TypeParam>(100), 3));

  // The curve extends to (-INF, -INF).
  EXPECT_THAT(curve, EvalsLt(static_cast<TypeParam>(-100), -6));
}

TYPED_TEST(FloatTypesTest, Symlog1pPWLCurveBoundedTwoSides) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("Symlog1pPWLCurve({{-5;-5};{-2;-1};{1;2};{10;11}})"));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{-5, -5}, {-2, -1}, {1, 2}, {10, 11}}));

  // The curve is constant below -5.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{-6, -5}, {-100, -5}}));

  // The curve is constant above 10.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{10, 11}, {100, 11}}));
}

// Verifies that the interpolation for Symlog1pPWLCurve occurs in
// symmetriclog(x+1) space.
TYPED_TEST(FloatTypesTest, Symlog1pPWLCurveSymlog1pInterpolation) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("Symlog1pPWLCurve({{-10;-10};{-1;"
                                "-1};{1;1};{10;10};{INF;INF}})"));

  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{1, 1}, {10, 10}}));

  const TypeParam m = (10 - 1) / (std::log(10 + 1) - std::log(1 + 1));
  const TypeParam b = 10 - m * std::log(10 + 1);
  for (TypeParam x : {2, 5, 8}) {
    SCOPED_TRACE(absl::StrCat("x=", x));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                                 {{x, m * std::log(x + 1) + b}}));
  }

  for (TypeParam x : {-2, -5, -8}) {
    SCOPED_TRACE(absl::StrCat("x=", x));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                                 {{x, -m * std::log(-x + 1) - b}}));
  }
}

TYPED_TEST(FloatTypesTest, Symlog1pPWLCurveNewCurveWithCurveType) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve(Symlog1pPWLCurve, {{-5, -5}, {-2, -1}, {1, 2}, {10, 11}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{-5, -5}, {-2, -1}, {1, 2}, {10, 11}}));

  // The curve is constant below -5.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{-6, -5}, {-100, -5}}));

  // The curve is constant above 10.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{10, 11}, {100, 11}}));
}

// ============================================================================
// LogPWLCurve
// ============================================================================
TYPED_TEST(FloatTypesTest, LogPWLCurveBoundedLeft) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("LogPWLCurve({{1;2};{2;10};{10;11};{11;13};{INF;INF}})"));

  // The curve is constant before 1.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(0),
                               {{0, 2}, {-2, 2}, {-100, 2}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(0),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve extends to (+INF, +INF).
  EXPECT_THAT(curve, EvalsGt(static_cast<TypeParam>(13), 15));
}

TYPED_TEST(FloatTypesTest, LogPWLCurveBoundedRight) {
  ASSERT_OK_AND_ASSIGN(
      auto curve,
      NewCurve("LogPWLCurve({{0;-INF};{1;2};{2;10};{10;11};{11;13}})"));

  // The curve goes towards -INF when x goes closer to 0.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{0.25, -14}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant before 0.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4),
                        {{-2, -std::numeric_limits<TypeParam>::infinity()},
                         {-100, -std::numeric_limits<TypeParam>::infinity()}}));
}

TYPED_TEST(FloatTypesTest, BoundedTwoSides) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("LogPWLCurve({{1;2};{2;10};{10;11};{11;13}})"));

  // The curve is constant before 1.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{0, 2}, {-2, 2}, {-100, 2}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant above 11.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{12, 13}, {100, 13}}));
}

// Verifies that the interpolation for LogPWLCurve occurs in log(x)
// space.
TYPED_TEST(FloatTypesTest, LogInterpolation) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("LogPWLCurve({{1;0};{16;20};{INF;INF}})"));

  EXPECT_THAT(curve, EvalsNear(1e-4, {{1, 0}, {16, 20}}));

  const TypeParam m = (20 - 0) / (std::log(16) - std::log(1));
  const TypeParam b = 20 - m * std::log(16);
  for (TypeParam x : {2, 5, 8}) {
    SCOPED_TRACE(absl::StrCat("x=", x));
    EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                                 {{x, m * std::log(x) + b}}));
  }
}

TYPED_TEST(FloatTypesTest, LogPWLCurveNewCurveWithCurveType) {
  ASSERT_OK_AND_ASSIGN(
      auto curve, NewCurve(LogPWLCurve, {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant before 1.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{0, 2}, {-2, 2}, {-100, 2}}));

  // Check value at control points.
  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4),
                               {{1, 2}, {2, 10}, {10, 11}, {11, 13}}));

  // The curve is constant above 11.
  EXPECT_THAT(curve,
              EvalsNear(static_cast<TypeParam>(1e-4), {{12, 13}, {100, 13}}));
}

TYPED_TEST(FloatTypesTest, LogPWLCurveNonMonotonic) {
  ASSERT_OK_AND_ASSIGN(auto curve,
                       NewCurve("LogPWLCurve({{1;2};{2;1};{3;2};{4;1}})"));

  EXPECT_THAT(curve, EvalsNear(static_cast<TypeParam>(1e-4), {{-100, 2},
                                                              {-10, 2},
                                                              {-1.5, 2},
                                                              {-1, 2},
                                                              {0, 2},
                                                              {1, 2},
                                                              {2, 1},
                                                              {3, 2},
                                                              {4, 1},
                                                              {5, 1},
                                                              {1.5, 1.41504},
                                                              {2.5, 1.55034},
                                                              {3.5, 1.46416}}));
}

// //
// ============================================================================
// // Invalid curves
// //
// ============================================================================

TEST(InvalidCurveTest, InvalidCurveTypesWithStatusCode) {
  for (CurveType type : AllCurveTypes()) {
    SCOPED_TRACE(absl::StrCat("CurveType = ", CurveType_Name(type)));

    // Nonmonotonic x coordinates.
    EXPECT_STATUS_ERROR("X_VALUES_NOT_STRICTLY_MONOTONICALLY_INCREASING",
                        NewCurve(type, {{0, 0}, {-1, 0}, {2, 1}}));
    EXPECT_STATUS_ERROR(
        "X_VALUES_NOT_STRICTLY_MONOTONICALLY_INCREASING",
        NewCurve(type, {{-10, 0}, {-100, 0}, {0, 0}, {1, 1}, {2, 2}}));
    // Duplicate x coordinates.
    EXPECT_STATUS_ERROR("X_VALUES_NOT_STRICTLY_MONOTONICALLY_INCREASING",
                        NewCurve(type, {{0, 0}, {1, 1}, {1, 1}}));
    EXPECT_STATUS_ERROR("X_VALUES_NOT_STRICTLY_MONOTONICALLY_INCREASING",
                        NewCurve(type, {{0, 0}, {1, 1}, {1, 1.5}}));
    // No points.
    EXPECT_STATUS_ERROR("NOT_ENOUGH_FINITE_POINTS", NewCurve(type, {}));

    // Unsupported asymptote adjacent to constant segment.

    // Note: curves1 interprets this as y=0.5.
    EXPECT_STATUS_ERROR(
        "UNSUPPORTED_ASYMPTOTE_ADJACENT_TO_CONSTANT_SEGMENT",
        NewCurve(type, {{-INF, 0.0}, {0.0, 0.5}, {1.0, 0.5}, {INF, 1.0}}));
    // Note: curves1 interprets this as y=0.0.
    EXPECT_STATUS_ERROR(
        "UNSUPPORTED_ASYMPTOTE_ADJACENT_TO_CONSTANT_SEGMENT",
        NewCurve(type, {{-INF, -INF}, {0.0, 0.0}, {1.0, 0.0}, {INF, INF}}));

    EXPECT_STATUS_ERROR(
        "UNSUPPORTED_ASYMPTOTE_ADJACENT_TO_CONSTANT_SEGMENT",
        NewCurve(type, {{-1.0, -INF}, {0.0, 0.5}, {1.0, 0.5}, {2.0, INF}}));
    EXPECT_STATUS_ERROR(
        "UNSUPPORTED_ASYMPTOTE_ADJACENT_TO_CONSTANT_SEGMENT",
        NewCurve(type, {{-INF, -0.1}, {0.0, 0.0}, {1.0, 0.0}, {2.0, 1.0}}));
  }
}

TEST(InvalidCurveTest, InvalidPWLCurve) {
  // Horizontal asymptote.
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_HORIZONTAL_ASYMPTOTE",
      NewCurve(PWLCurve, {{-INF, 0}, {1, 1}, {10, 10}, {100, 100}}));
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_HORIZONTAL_ASYMPTOTE",
      NewCurve(PWLCurve, {{0, 0}, {1, 1}, {10, 10}, {INF, 100}}));
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_HORIZONTAL_ASYMPTOTE",
      NewCurve(PWLCurve, {{-INF, 0}, {1, 1}, {10, 10}, {INF, 100}}));

  // Vertical asymptote.
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_VERTICAL_ASYMPTOTE",
      NewCurve(PWLCurve, {{0, -INF}, {1, 1}, {10, 10}, {100, 100}}));
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_VERTICAL_ASYMPTOTE",
      NewCurve(PWLCurve, {{0, 0}, {1, 1}, {10, 10}, {100, INF}}));
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_VERTICAL_ASYMPTOTE",
      NewCurve(PWLCurve, {{0, -INF}, {1, 1}, {10, 10}, {100, INF}}));

  // Not enough points to define a curve, because at best we have
  // a slope here (-1 or 1), but no anchor point to go through.
  EXPECT_STATUS_ERROR("NOT_ENOUGH_FINITE_POINTS",
                      NewCurve(PWLCurve, {{-INF, INF}, {INF, -INF}}));
  EXPECT_STATUS_ERROR("NOT_ENOUGH_FINITE_POINTS",
                      NewCurve(PWLCurve, {{-INF, -INF}, {INF, INF}}));

  // Infinity is only allowed as first or last point of a curve.
  EXPECT_STATUS_ERROR(
      "INFINITE_INTERIOR_Y_COORDINATE",
      NewCurve(PWLCurve, {{0, 0}, {1, -INF}, {2, 1}, {INF, INF}}));

  // Infinity at the end must have valid sign.
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(PWLCurve, {{-INF, -INF}, {3, 5}, {14, 0}, {15, 1}}));
  EXPECT_STATUS_ERROR("WRONG_SIGN_INF_AT_END",
                      NewCurve(PWLCurve, {{-INF, -INF}, {1, 1}, {INF, -INF}}));
  EXPECT_STATUS_ERROR("WRONG_SIGN_INF_AT_END",
                      NewCurve(PWLCurve, {{-INF, INF}, {5, 10}, {INF, INF}}));
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(PWLCurve, {{1, -INF}, {3, 5}, {14, 0}, {15, 1}}));
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(PWLCurve, {{-INF, INF}, {3, 5}, {15, 1}, {20, INF}}));
}

TEST(InvalidCurveTest, InvalidSymlog1pPWLCurve) {
  // Horizontal asymptote.
  EXPECT_STATUS_ERROR("UNSUPPORTED_HORIZONTAL_ASYMPTOTE",
                      NewCurve(Symlog1pPWLCurve,
                               {{-INF, 0}, {1, 1}, {10, 10}, {100, 100}}));
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_HORIZONTAL_ASYMPTOTE",
      NewCurve(Symlog1pPWLCurve, {{0, 0}, {1, 1}, {10, 10}, {INF, 100}}));
  EXPECT_STATUS_ERROR("UNSUPPORTED_HORIZONTAL_ASYMPTOTE",
                      NewCurve(Symlog1pPWLCurve,
                               {{-INF, 0}, {1, 1}, {10, 10}, {INF, 100}}));

  // Vertical asymptote.
  EXPECT_STATUS_ERROR("UNSUPPORTED_VERTICAL_ASYMPTOTE",
                      NewCurve(Symlog1pPWLCurve,
                               {{0, -INF}, {1, 1}, {10, 10}, {100, 100}}));
  EXPECT_STATUS_ERROR(
      "UNSUPPORTED_VERTICAL_ASYMPTOTE",
      NewCurve(Symlog1pPWLCurve, {{0, 0}, {1, 1}, {10, 10}, {100, INF}}));
  EXPECT_STATUS_ERROR("UNSUPPORTED_VERTICAL_ASYMPTOTE",
                      NewCurve(Symlog1pPWLCurve,
                               {{0, -INF}, {1, 1}, {10, 10}, {100, INF}}));

  // Not enough points to define a curve, because at best we have
  // a slope here (-1 or 1), but no anchor point to go through.
  EXPECT_STATUS_ERROR(
      "NOT_ENOUGH_FINITE_POINTS",
      NewCurve(Symlog1pPWLCurve, {{-INF, INF}, {INF, -INF}}));
  EXPECT_STATUS_ERROR(
      "NOT_ENOUGH_FINITE_POINTS",
      NewCurve(Symlog1pPWLCurve, {{-INF, -INF}, {INF, INF}}));

  // Infinity is only allowed as first or last point of a curve.
  EXPECT_STATUS_ERROR("INFINITE_INTERIOR_Y_COORDINATE",
                      NewCurve(Symlog1pPWLCurve,
                               {{0, 0}, {1, -INF}, {2, 1}, {INF, INF}}));

  // Infinity at the end must have valid sign.
  EXPECT_STATUS_ERROR("WRONG_SIGN_INF_AT_END",
                      NewCurve(Symlog1pPWLCurve,
                               {{-INF, -INF}, {3, 5}, {14, 0}, {15, 1}}));
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(Symlog1pPWLCurve, {{-INF, -INF}, {1, 1}, {INF, -INF}}));
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(Symlog1pPWLCurve, {{-INF, INF}, {5, 10}, {INF, INF}}));
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(Symlog1pPWLCurve, {{1, -INF}, {3, 5}, {14, 0}, {15, 1}}));
  EXPECT_STATUS_ERROR("WRONG_SIGN_INF_AT_END",
                      NewCurve(Symlog1pPWLCurve,
                               {{-INF, INF}, {3, 5}, {15, 1}, {20, INF}}));
}

TEST(InvalidCurveTest, InvalidLog1pPWLCurve) {
  // < -1 x points
  EXPECT_STATUS_ERROR("X_VALUE_BELOW_MINUS_ONE_FOUND",
                      NewCurve(Log1pPWLCurve, {{-5, 1}, {2, 2}, {3, 3}}));

  // < -1 x points with a duplicate y coordinate (which could lead to
  // elimination of the first point)
  EXPECT_STATUS_ERROR(
      "X_VALUE_BELOW_MINUS_ONE_FOUND",
      NewCurve(Log1pPWLCurve, {{-5, 1}, {0, 1}, {2, 2}, {3, 3}}));

  // Infinity is only allowed as first or last point of a curve.
  EXPECT_STATUS_ERROR("INFINITE_INTERIOR_Y_COORDINATE",
                      NewCurve(Log1pPWLCurve, {{0, 1}, {2, INF}, {3, 2}}));

  // Infinity at the end must have valid sign.
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(Log1pPWLCurve, {{-1, INF}, {2, 3}, {3, 1}, {INF, INF}}));
}

TEST(InvalidCurveTest, InvalidLogPWLCurve) {
  // < 0 x points
  EXPECT_STATUS_ERROR("NEGATIVE_X_VALUE_FOUND",
                      NewCurve(LogPWLCurve, {{-5, 1}, {2, 2}, {3, 3}}));

  // < 0 x points with a duplicate y coordinate (which could lead to
  // elimination of the first point)
  EXPECT_STATUS_ERROR("NEGATIVE_X_VALUE_FOUND",
                      NewCurve(LogPWLCurve, {{-5, 1}, {0, 1}, {2, 2}, {3, 3}}));

  // Infinity is only allowed as first or last point of a curve.
  EXPECT_STATUS_ERROR("INFINITE_INTERIOR_Y_COORDINATE",
                      NewCurve(LogPWLCurve, {{1, 1}, {2, INF}, {3, 2}}));

  // Infinity at the end must have valid sign.
  EXPECT_STATUS_ERROR(
      "WRONG_SIGN_INF_AT_END",
      NewCurve(LogPWLCurve, {{0, INF}, {2, 3}, {3, 1}, {INF, INF}}));
}

TEST(InvalidCurveTest, InvalidCurvesSpecifiedInDifferentWays) {
  // While the tests above cover the checking code, there are
  // different ways to trigger and access the validation. We test
  // those here, with one particular curve only.

  EXPECT_STATUS_ERROR("NEGATIVE_X_VALUE_FOUND",
                      NewCurve(LogPWLCurve, {{-1, 2}, {2, -1}}));

  EXPECT_STATUS_ERROR("NEGATIVE_X_VALUE_FOUND",
                      NewCurve("LogPWLCurve({{-1;2};{2;-1}})"));

  EXPECT_STATUS_ERROR(
      "NEGATIVE_X_VALUE_FOUND",
      NewCurveWithSeparator('|', "LogPWLCurve({{-1|2}|{2|-1}})"));

  EXPECT_STATUS_ERROR(
      "NEGATIVE_X_VALUE_FOUND",
      NewCurveWithAllowedSeparators(";|", "LogPWLCurve({{-1|2}|{2|-1}})"));

  auto curve(NewCurve(LogPWLCurve, {{-1, 2}, {2, -1}}));
  EXPECT_EQ(Status(StatusCode::kInvalidArgument, "NEGATIVE_X_VALUE_FOUND"),
            curve.status());

  curve = NewCurve("LogPWLCurve({{-1;2};{2;-1}})");
  EXPECT_EQ(Status(StatusCode::kInvalidArgument, "NEGATIVE_X_VALUE_FOUND"),
            curve.status());

  curve = NewCurveWithSeparator('|', "LogPWLCurve({{-1|2}|{2|-1}})");
  EXPECT_EQ(Status(StatusCode::kInvalidArgument, "NEGATIVE_X_VALUE_FOUND"),
            curve.status());

  curve = NewCurveWithAllowedSeparators(";|", "LogPWLCurve({{-1|2}|{2|-1}})");
  EXPECT_EQ(Status(StatusCode::kInvalidArgument, "NEGATIVE_X_VALUE_FOUND"),
            curve.status());
}

}  // namespace
}  // namespace arolla::pwlcurve
