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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;

using OI = OptionalValue<int>;
using OB = OptionalValue<bool>;

TEST(ComparisonOperatorsTest, Equal) {
  EXPECT_THAT(InvokeOperator<bool>("bool.equal", 57, 57), IsOkAndHolds(true));
  EXPECT_THAT(InvokeOperator<bool>("bool.equal", 57, 2), IsOkAndHolds(false));

  EXPECT_THAT(InvokeOperator<OB>("bool.equal", OI{57}, OI{57}),
              IsOkAndHolds(OB{true}));
  EXPECT_THAT(InvokeOperator<OB>("bool.equal", OI{}, OI{57}),
              IsOkAndHolds(OB{}));
  EXPECT_THAT(InvokeOperator<OB>("bool.equal", OI{57}, OI{}),
              IsOkAndHolds(OB{}));
  EXPECT_THAT(InvokeOperator<OB>("bool.equal", OI{}, OI{}), IsOkAndHolds(OB{}));
}

TEST(ComparisonOperatorsTest, Less) {
  EXPECT_THAT(InvokeOperator<bool>("bool.less", 2, 57), IsOkAndHolds(true));
  EXPECT_THAT(InvokeOperator<bool>("bool.less", 57, 57), IsOkAndHolds(false));
  EXPECT_THAT(InvokeOperator<bool>("bool.less", 57, 2), IsOkAndHolds(false));

  EXPECT_THAT(InvokeOperator<OB>("bool.less", OI{57}, OI{57}),
              IsOkAndHolds(OB{false}));
  EXPECT_THAT(InvokeOperator<OB>("bool.less", OI{}, OI{57}),
              IsOkAndHolds(OB{}));
  EXPECT_THAT(InvokeOperator<OB>("bool.less", OI{57}, OI{}),
              IsOkAndHolds(OB{}));
  EXPECT_THAT(InvokeOperator<OB>("bool.less", OI{}, OI{}), IsOkAndHolds(OB{}));
}

}  // namespace
}  // namespace arolla
