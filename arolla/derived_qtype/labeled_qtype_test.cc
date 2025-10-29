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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;

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

TEST(LabeledDerivedQType, CustomRepr) {
  {
    // Default repr.
    auto qtype = GetLabeledQType(GetOptionalQType<int32_t>(), "foo");
    EXPECT_EQ(UnsafeDowncastDerivedQValue(
                  qtype, TypedRef::FromValue(OptionalValue<int32_t>(2)))
                  .Repr(),
              "LABEL[foo]{optional_int32{2}}");
  }
  {
    // Custom repr.
    ASSERT_OK(RegisterLabeledQTypeReprFn("custom_repr", [](TypedRef value) {
      return ReprToken{"my_custom_repr"};
    }));
    EXPECT_EQ(UnsafeDowncastDerivedQValue(
                  GetLabeledQType(GetOptionalQType<int32_t>(), "custom_repr"),
                  TypedRef::FromValue(OptionalValue<int32_t>(2)))
                  .Repr(),
              "my_custom_repr");
    // Same for all QTypes with this label.
    EXPECT_EQ(UnsafeDowncastDerivedQValue(
                  GetLabeledQType(GetOptionalQType<float>(), "custom_repr"),
                  TypedRef::FromValue(OptionalValue<float>(2.0f)))
                  .Repr(),
              "my_custom_repr");
    // Other labels do not have the custom repr.
    EXPECT_EQ(
        UnsafeDowncastDerivedQValue(
            GetLabeledQType(GetOptionalQType<int32_t>(), "not_custom_repr"),
            TypedRef::FromValue(OptionalValue<int32_t>(2)))
            .Repr(),
        "LABEL[not_custom_repr]{optional_int32{2}}");
  }
  {
    // Fallback to default repr.
    ASSERT_OK(RegisterLabeledQTypeReprFn(
        "custom_repr_none", [](TypedRef value) { return std::nullopt; }));
    EXPECT_EQ(
        UnsafeDowncastDerivedQValue(
            GetLabeledQType(GetOptionalQType<int32_t>(), "custom_repr_none"),
            TypedRef::FromValue(OptionalValue<int32_t>(2)))
            .Repr(),
        "LABEL[custom_repr_none]{optional_int32{2}}");
  }
  {
    ASSERT_OK(RegisterLabeledQTypeReprFn(
        "duplicate_repr", [](TypedRef value) { return std::nullopt; }));
    // Already registered label.
    EXPECT_THAT(
        RegisterLabeledQTypeReprFn("duplicate_repr",
                                   [](TypedRef value) { return std::nullopt; }),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "label 'duplicate_repr' already has a registered repr function"));
    // OK to override.
    EXPECT_OK(RegisterLabeledQTypeReprFn(
        "duplicate_repr",
        [](TypedRef value) { return ReprToken{"my_custom_repr"}; },
        /*override=*/true));
    EXPECT_EQ(
        UnsafeDowncastDerivedQValue(
            GetLabeledQType(GetOptionalQType<int32_t>(), "duplicate_repr"),
            TypedRef::FromValue(OptionalValue<int32_t>(2)))
            .Repr(),
        "my_custom_repr");
  }
  {
    ASSERT_OK(RegisterLabeledQTypeReprFn("nullptr_repr", nullptr));
    EXPECT_EQ(UnsafeDowncastDerivedQValue(
                  GetLabeledQType(GetOptionalQType<int32_t>(), "nullptr_repr"),
                  TypedRef::FromValue(OptionalValue<int32_t>(2)))
                  .Repr(),
              "LABEL[nullptr_repr]{optional_int32{2}}");
    // nullptr is always ok to overwrite.
    ASSERT_OK(RegisterLabeledQTypeReprFn("nullptr_repr", [](TypedRef value) {
      return ReprToken{"my_custom_repr"};
    }));
    EXPECT_EQ(
        UnsafeDowncastDerivedQValue(
            GetLabeledQType(GetOptionalQType<int32_t>(), "nullptr_repr"),
            TypedRef::FromValue(OptionalValue<int32_t>(2)))
            .Repr(),
        "my_custom_repr");
  }
}

}  // namespace
}  // namespace arolla
