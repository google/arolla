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
#include "arolla/lazy/lazy_qtype.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/lazy/lazy.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::ReprTokenEq;
using ::arolla::testing::TypedValueWith;

class LazyQTypeTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(LazyQTypeTest, Basics) {
  auto qtype = GetLazyQType<QTypePtr>();
  EXPECT_EQ(qtype, GetLazyQType<QTypePtr>());
  EXPECT_EQ(qtype->name(), "LAZY[QTYPE]");
  EXPECT_EQ(qtype->type_info(), typeid(LazyPtr));
  EXPECT_EQ(qtype->type_layout().AllocSize(), sizeof(LazyPtr));
  EXPECT_EQ(qtype->type_layout().AllocAlignment().value, alignof(LazyPtr));
  EXPECT_TRUE(qtype->type_fields().empty());
  EXPECT_EQ(qtype->value_qtype(), GetQTypeQType());
  EXPECT_EQ(qtype->qtype_specialization_key(), "::arolla::LazyQType");
}

TEST_F(LazyQTypeTest, IsLazyQType) {
  EXPECT_TRUE(IsLazyQType(GetLazyQType<QTypePtr>()));
  EXPECT_TRUE(IsLazyQType(GetLazyQType<int32_t>()));
  EXPECT_TRUE(IsLazyQType(GetLazyQType<float>()));
  EXPECT_FALSE(IsLazyQType(GetQTypeQType()));
  EXPECT_FALSE(IsLazyQType(GetQType<int32_t>()));
  EXPECT_FALSE(IsLazyQType(GetQType<float>()));
}

TEST_F(LazyQTypeTest, MakeLazyQValue) {
  auto qvalue = MakeLazyQValue(MakeLazyFromQValue(TypedValue::FromValue(1)));
  EXPECT_THAT(qvalue.GenReprToken(), ReprTokenEq("lazy[INT32]"));
  ASSERT_EQ(qvalue.GetType(), GetLazyQType<int>());
  EXPECT_THAT(qvalue.UnsafeAs<LazyPtr>()->Get(),
              IsOkAndHolds(TypedValueWith<int>(1)));
  EXPECT_EQ(qvalue.GetFingerprint(),
            MakeLazyQValue(MakeLazyFromQValue(TypedValue::FromValue(1)))
                .GetFingerprint());
  EXPECT_NE(qvalue.GetFingerprint(),
            MakeLazyQValue(MakeLazyFromQValue(TypedValue::FromValue(2)))
                .GetFingerprint());
}

}  // namespace
}  // namespace arolla
