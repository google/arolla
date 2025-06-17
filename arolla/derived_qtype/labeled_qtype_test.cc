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
#include "arolla/derived_qtype/labeled_qtype.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

TEST(LabeledDerivedQType, GetLabeledQType) {
  auto unit_qtype = GetQType<Unit>();
  auto int32_qtype = GetQType<int32_t>();

  auto unit_foo_qtype = GetLabeledQType(unit_qtype, "foo");
  auto unit_bar_qtype = GetLabeledQType(unit_foo_qtype, "bar");
  auto int32_foo_qtype = GetLabeledQType(int32_qtype, "foo");

  EXPECT_NE(unit_foo_qtype, unit_qtype);
  EXPECT_NE(unit_bar_qtype, unit_foo_qtype);
  EXPECT_NE(int32_foo_qtype, int32_qtype);

  EXPECT_EQ(unit_foo_qtype, GetLabeledQType(unit_bar_qtype, "foo"));
  EXPECT_EQ(unit_bar_qtype, GetLabeledQType(unit_bar_qtype, "bar"));
  EXPECT_EQ(int32_foo_qtype, GetLabeledQType(int32_qtype, "foo"));

  EXPECT_NE(unit_foo_qtype, unit_bar_qtype);
  EXPECT_NE(unit_foo_qtype, int32_foo_qtype);
  EXPECT_NE(unit_bar_qtype, int32_foo_qtype);

  EXPECT_EQ(DecayDerivedQType(unit_foo_qtype), unit_qtype);
  EXPECT_EQ(DecayDerivedQType(unit_bar_qtype), unit_qtype);
  EXPECT_EQ(DecayDerivedQType(int32_foo_qtype), int32_qtype);

  EXPECT_EQ(unit_foo_qtype->name(), "LABEL[foo]");
  EXPECT_EQ(unit_bar_qtype->name(), "LABEL[bar]");
  EXPECT_EQ(int32_foo_qtype->name(), "LABEL[foo]");
}

TEST(LabeledDerivedQType, GetQTypeLabel) {
  auto unit_qtype = GetQType<Unit>();
  auto int32_qtype = GetQType<int32_t>();

  EXPECT_EQ(GetQTypeLabel(nullptr), "");
  EXPECT_EQ(GetQTypeLabel(unit_qtype), "");
  EXPECT_EQ(GetQTypeLabel(int32_qtype), "");

  auto unit_foo_qtype = GetLabeledQType(unit_qtype, "foo");
  auto unit_bar_qtype = GetLabeledQType(unit_qtype, "bar");
  auto int32_foo_qtype = GetLabeledQType(int32_qtype, "foo");

  EXPECT_EQ(GetQTypeLabel(unit_foo_qtype), "foo");
  EXPECT_EQ(GetQTypeLabel(unit_bar_qtype), "bar");
  EXPECT_EQ(GetQTypeLabel(int32_foo_qtype), "foo");
}

TEST(LabeledDerivedQType, IsLabeledQType) {
  auto unit_qtype = GetQType<Unit>();
  auto int32_qtype = GetQType<int32_t>();

  EXPECT_FALSE(IsLabeledQType(nullptr));
  EXPECT_FALSE(IsLabeledQType(unit_qtype));
  EXPECT_FALSE(IsLabeledQType(int32_qtype));

  auto unit_foo_qtype = GetLabeledQType(unit_qtype, "foo");
  auto unit_bar_qtype = GetLabeledQType(unit_qtype, "bar");
  auto int32_foo_qtype = GetLabeledQType(int32_qtype, "foo");

  EXPECT_TRUE(IsLabeledQType(unit_foo_qtype));
  EXPECT_TRUE(IsLabeledQType(unit_bar_qtype));
  EXPECT_TRUE(IsLabeledQType(int32_foo_qtype));
}

TEST(LabeledDerivedQType, EmptyLabel) {
  auto unit_qtype = GetQType<Unit>();

  EXPECT_EQ(GetLabeledQType(unit_qtype, ""), unit_qtype);

  auto unit_foo_qtype = GetLabeledQType(unit_qtype, "foo");

  EXPECT_EQ(GetLabeledQType(unit_foo_qtype, ""), unit_qtype);
}

TEST(LabeledDerivedQType, GetLabeledQTypeSpecializationKey) {
  auto unit_foo_qtype = GetLabeledQType(GetQType<Unit>(), "foo");
  EXPECT_EQ(unit_foo_qtype->qtype_specialization_key(),
            GetLabeledQTypeSpecializationKey());
  EXPECT_EQ(GetLabeledQTypeSpecializationKey(), "::arolla::LabeledQType");
}

}  // namespace
}  // namespace arolla
