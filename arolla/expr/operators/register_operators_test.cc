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
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/status_matchers_backport.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::testing::IsOkAndHolds;
using ::arolla::testing::StatusIs;
using ::testing::HasSubstr;

// Shortener for passing literal values to operators that don't need them.
absl::StatusOr<QTypePtr> GetOutputQType(
    const ExprOperatorPtr& op, absl::Span<const QTypePtr> input_qtypes) {
  std::vector<ExprAttributes> inputs;
  inputs.reserve(input_qtypes.size());
  for (auto* input_qtype : input_qtypes) {
    inputs.emplace_back(input_qtype);
  }
  ASSIGN_OR_RETURN(auto output, op->InferAttributes(inputs));
  return output.qtype();
}

class RegisterOperatorsTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(RegisterOperatorsTest, PresenceAndOr) {
  ASSERT_OK_AND_ASSIGN(auto pand_or,
                       expr::LookupOperator("core._presence_and_or"));
  auto i64 = GetQType<int64_t>();
  auto i32 = GetQType<int32_t>();
  EXPECT_THAT(GetOutputQType(pand_or, {i64, GetQType<OptionalUnit>(), i64}),
              IsOkAndHolds(i64));
  EXPECT_THAT(GetOutputQType(pand_or, {i64, GetQType<OptionalUnit>(), i32}),
              IsOkAndHolds(i64));
  EXPECT_THAT(GetOutputQType(pand_or, {i32, GetQType<OptionalUnit>(), i64}),
              IsOkAndHolds(i64));
  EXPECT_THAT(GetOutputQType(pand_or, {i32, GetQType<OptionalUnit>(), i32}),
              IsOkAndHolds(i32));

  auto oi64 = GetOptionalQType<int64_t>();
  auto oi32 = GetOptionalQType<int32_t>();
  EXPECT_THAT(GetOutputQType(pand_or, {oi32, GetQType<OptionalUnit>(), i64}),
              IsOkAndHolds(i64));
  EXPECT_THAT(GetOutputQType(pand_or, {oi64, GetQType<OptionalUnit>(), i32}),
              IsOkAndHolds(i64));
  EXPECT_THAT(GetOutputQType(pand_or, {i32, GetQType<OptionalUnit>(), oi64}),
              IsOkAndHolds(oi64));

  // Array types are not supported.
  auto daunit = GetDenseArrayQType<Unit>();
  auto dai64 = GetDenseArrayQType<int64_t>();
  EXPECT_THAT(
      GetOutputQType(pand_or, {oi32, daunit, dai64}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("expected all arguments to be scalar or optional scalar, "
                    "but got DENSE_ARRAY_UNIT for 1-th argument")));

  EXPECT_THAT(GetOutputQType(pand_or, {GetQType<Unit>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected 3 but got 1")));
  EXPECT_THAT(GetOutputQType(
                  pand_or, {i64, GetQType<OptionalUnit>(), GetQType<Bytes>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common QType for (INT64,BYTES)")));
}

TEST_F(RegisterOperatorsTest, PresenceAnd) {
  ASSERT_OK_AND_ASSIGN(auto presence_and,
                       expr::LookupOperator("core.presence_and"));
  EXPECT_THAT(
      GetOutputQType(presence_and, {GetQType<int32_t>(), GetQType<bool>()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected scalar type to be UNIT")));
}

TEST_F(RegisterOperatorsTest, ShortCircuitWhere) {
  ASSERT_OK_AND_ASSIGN(auto where,
                       expr::LookupOperator("core._short_circuit_where"));

  EXPECT_THAT(GetOutputQType(where, {GetQType<OptionalUnit>(),
                                     GetQType<int64_t>(), GetQType<int64_t>()}),
              IsOkAndHolds(GetQType<int64_t>()));
  EXPECT_THAT(GetOutputQType(where, {GetQType<OptionalUnit>(),
                                     GetQType<float>(), GetQType<double>()}),
              IsOkAndHolds(GetQType<double>()));
  EXPECT_THAT(GetOutputQType(where, {GetQType<OptionalUnit>(),
                                     GetQType<int64_t>(), GetQType<Bytes>()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common QType for (INT64,BYTES)")));
}

}  // namespace
}  // namespace arolla::expr_operators
