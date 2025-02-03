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
#include "arolla/qtype/any_qtype.h"

#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

TEST(AnyQType, AnyConstructorRegression) {
  Any any;
  Any copy_1 = any;
  Any copy_2(any);
  Any copy_3 = std::move(any);
  Any copy_4(std::move(copy_2));
}

TEST(AnyQType, Any) {
  int v1 = 5;
  std::string v2 = "string";
  TypedValue tv1 = TypedValue::FromValue(Any(v1));
  TypedValue tv2 = TypedValue::FromValue(Any(v2));
  TypedValue tv3 = TypedValue::FromValue(Any());

  ASSERT_OK_AND_ASSIGN(const Any& a1, tv1.As<Any>());
  ASSERT_OK_AND_ASSIGN(const Any& a2, tv2.As<Any>());
  ASSERT_OK_AND_ASSIGN(const Any& a3, tv3.As<Any>());

  EXPECT_THAT(a1.As<int>(), IsOkAndHolds(v1));
  EXPECT_THAT(a1.As<double>(), StatusIs(absl::StatusCode::kFailedPrecondition,
                                        HasSubstr("can not cast Any")));

  ASSERT_OK_AND_ASSIGN(const std::string& v2_res, a2.As<std::string>());
  EXPECT_EQ(v2, v2_res);
  EXPECT_THAT(a2.As<double>(), StatusIs(absl::StatusCode::kFailedPrecondition,
                                        HasSubstr("can not cast Any")));

  EXPECT_THAT(a3.As<double>(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("can not cast an empty ::arolla::Any")));
}

TEST(AnyQType, Fingerprint) {
  Any a = Any(1);
  Any b = Any(1);  // new object with the same value has a different fingerprint
  Any a_copy = a;  // copy has the same fingerprint
  EXPECT_NE(TypedValue::FromValue(a).GetFingerprint(),
            TypedValue::FromValue(b).GetFingerprint());
  EXPECT_EQ(TypedValue::FromValue(a).GetFingerprint(),
            TypedValue::FromValue(a_copy).GetFingerprint());
}

}  // namespace
}  // namespace arolla
