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
#include "arolla/qtype/unspecified_qtype.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;

class UnspecifiedQTypeTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(UnspecifiedQTypeTest, UnspecifiedQType) {
  const auto unspecified_qtype = GetUnspecifiedQType();
  EXPECT_EQ(unspecified_qtype->name(), "UNSPECIFIED");
  EXPECT_EQ(unspecified_qtype->type_layout().AllocSize(), 1);
  EXPECT_EQ(unspecified_qtype->type_layout().AllocAlignment().value, 1);
  EXPECT_TRUE(unspecified_qtype->type_fields().empty());
  EXPECT_EQ(unspecified_qtype->value_qtype(), nullptr);
}

TEST_F(UnspecifiedQTypeTest, UnspecifiedQValue) {
  const auto unspecified_qvalue = GetUnspecifiedQValue();
  EXPECT_EQ(unspecified_qvalue.GetType(), GetUnspecifiedQType());
  EXPECT_THAT(unspecified_qvalue.GenReprToken(), ReprTokenEq("unspecified"));
}

}  // namespace
}  // namespace arolla
