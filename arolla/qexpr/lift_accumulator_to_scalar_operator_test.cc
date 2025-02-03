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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;

TEST(ScalarToScalarGroupLifterTest, AggSum) {
  EXPECT_THAT(InvokeOperator<OptionalValue<int>>(
                  "test.agg_sum", OptionalValue<int>(5), ScalarToScalarEdge()),
              IsOkAndHolds(OptionalValue<int>(5)));
}

TEST(ScalarToScalarGroupLifterTest, Average) {
  EXPECT_THAT(InvokeOperator<float>("test.average", OptionalValue<float>(5.0f),
                                    ScalarToScalarEdge()),
              IsOkAndHolds(5.0f));
}

TEST(ScalarToScalarGroupLifterTest, RankValues) {
  EXPECT_THAT(
      InvokeOperator<OptionalValue<int64_t>>(
          "test.rank_values", OptionalValue<int>(5), ScalarToScalarEdge()),
      IsOkAndHolds(OptionalValue<int64_t>(0)));
}

}  // namespace
}  // namespace arolla
