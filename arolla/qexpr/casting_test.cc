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
#include "arolla/qexpr/casting.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

TEST(CastingTest, FindMatchingSignature) {
  const QTypePtr i32 = GetQType<int32_t>();
  const QTypePtr i64 = GetQType<int64_t>();
  const QTypePtr oi32 = GetOptionalQType<int32_t>();
  const QTypePtr oi64 = GetOptionalQType<int64_t>();
  const QTypePtr f32 = GetQType<float>();
  const QTypePtr f64 = GetQType<double>();
  const QTypePtr of64 = GetOptionalQType<double>();

  // No match.
  EXPECT_THAT(
      FindMatchingSignature({i64, i32}, i32,
                            {QExprOperatorSignature::Get({i32, i32}, i64)},
                            "foo")
          .status(),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("QExpr operator foo(INT64,INT32)->INT32 not found")));

  // No match because output types don't match.
  EXPECT_THAT(
      FindMatchingSignature({i64, i32}, i64,
                            {QExprOperatorSignature::Get({i32, i32}, i32)},
                            "foo")
          .status(),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("QExpr operator foo(INT64,INT32)->INT64 not found")));

  // Single match.
  EXPECT_THAT(
      FindMatchingSignature({i64, i32}, i64,
                            {QExprOperatorSignature::Get({i64, i64}, i64)}, ""),
      IsOkAndHolds(QExprOperatorSignature::Get({i64, i64}, i64)));

  // Multiple matches — we choose the operator with fewer castings needed.
  EXPECT_THAT(
      FindMatchingSignature({i32, i32}, oi32,
                            {// Matches, but the first argument is less
                             // specific than in the next one.
                             QExprOperatorSignature::Get({oi64, i32}, oi32),
                             // The best match.
                             QExprOperatorSignature::Get({i64, i32}, oi32),
                             // Matches, but second argument is less
                             // specific than in the previous one.
                             QExprOperatorSignature::Get({i64, i64}, oi32),
                             // Filtered out by output QType.
                             QExprOperatorSignature::Get({i32, i32}, i32)},
                            ""),
      IsOkAndHolds(QExprOperatorSignature::Get({i64, i32}, oi32)));

  // Exact match, except for derived QTypes.
  EXPECT_THAT(FindMatchingSignature({GetWeakFloatQType()}, GetWeakFloatQType(),
                                    {QExprOperatorSignature::Get({f32}, f64),
                                     QExprOperatorSignature::Get({f64}, f64)},
                                    ""),
              IsOkAndHolds(QExprOperatorSignature::Get({f64}, f64)));
  // No exact match for derived QTypes — returning the most specific one.
  EXPECT_THAT(FindMatchingSignature({GetWeakFloatQType()}, GetWeakFloatQType(),
                                    {QExprOperatorSignature::Get({f32}, f64),
                                     QExprOperatorSignature::Get({of64}, f64)},
                                    ""),
              IsOkAndHolds(QExprOperatorSignature::Get({f32}, f64)));
  // For outputs implicit casting for derived QTypes is not allowed.
  EXPECT_THAT(
      FindMatchingSignature({GetWeakFloatQType()}, GetWeakFloatQType(),
                            {QExprOperatorSignature::Get({f32}, f32),
                             QExprOperatorSignature::Get({f64}, f32)},
                            ""),
      StatusIs(absl::StatusCode::kNotFound,
               HasSubstr("QExpr operator (WEAK_FLOAT)->WEAK_FLOAT not found")));

  // Multiple matches, but no one is the best.
  EXPECT_THAT(
      FindMatchingSignature({i32, i32}, i64,
                            {QExprOperatorSignature::Get({i32, i64}, i64),
                             QExprOperatorSignature::Get({i64, i32}, i64),
                             // Requires more castings then the others, so it is
                             // not included in the resulting error message.
                             QExprOperatorSignature::Get({i32, oi64}, i64),
                             // Output type does not match, so it is not
                             // included in the resulting error message.
                             QExprOperatorSignature::Get({i32, i32}, i32)},
                            "")
          .status(),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("ambiguous overloads for the QExpr operator "
                         "(INT32,INT32)->INT64: provided argument types "
                         "can be cast to the following supported signatures: "
                         "(INT32,INT64)->INT64, (INT64,INT32)->INT64")));
}

}  // namespace
}  // namespace arolla
