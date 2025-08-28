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
#include "arolla/objects/object_qtype.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;
using ::testing::IsEmpty;
using ::testing::Key;
using ::testing::UnorderedElementsAre;

TEST(ObjectTest, Accessors) {
  {
    // Default construction.
    Object obj;
    EXPECT_THAT(obj.attributes(), IsEmpty());
    EXPECT_EQ(obj.prototype(), std::nullopt);
    EXPECT_EQ(obj.GetAttrOrNull("foo"), nullptr);
  }
  {
    // With actual attributes - no prototype.
    Object obj({{"foo", TypedValue::FromValue(1)},
                {"bar", TypedValue::FromValue(2.0f)}});
    EXPECT_THAT(obj.attributes(), UnorderedElementsAre(Key("foo"), Key("bar")));
    EXPECT_EQ(obj.attributes().at("foo").UnsafeAs<int>(), 1);
    EXPECT_EQ(obj.attributes().at("bar").UnsafeAs<float>(), 2.0f);
    EXPECT_EQ(obj.GetAttrOrNull("foo")->UnsafeAs<int>(), 1);
    EXPECT_EQ(obj.GetAttrOrNull("bar")->UnsafeAs<float>(), 2.0f);
    EXPECT_EQ(obj.GetAttrOrNull("baz"), nullptr);
    EXPECT_EQ(obj.prototype(), std::nullopt);
  }
  {
    // Prototype chain.
    Object obj1({{"a", TypedValue::FromValue(1)}});
    Object obj2({{"b", TypedValue::FromValue(2.0f)}}, obj1);
    Object obj3({{"b", TypedValue::FromValue(5.0)},  // shadowing.
                 {"c", TypedValue::FromValue(-1)}},
                obj2);

    // Accessing own attributes.
    EXPECT_THAT(obj3.attributes(), UnorderedElementsAre(Key("b"), Key("c")));
    EXPECT_EQ(obj3.GetAttrOrNull("b")->UnsafeAs<double>(), 5.0);  // Shadowed.
    EXPECT_EQ(obj3.GetAttrOrNull("c")->UnsafeAs<int>(), -1);

    // Accessing prototype attributes.
    EXPECT_EQ(obj3.GetAttrOrNull("a")->UnsafeAs<int>(), 1);

    // Accessing non-existing attributes.
    EXPECT_EQ(obj3.GetAttrOrNull("baz"), nullptr);

    // Accessing prototype.
    const Object& obj3_proto = *obj3.prototype();
    EXPECT_THAT(obj3_proto.attributes(), UnorderedElementsAre(Key("b")));
    EXPECT_EQ(obj3_proto.GetAttrOrNull("b")->UnsafeAs<float>(), 2.0);
    EXPECT_EQ(obj3_proto.GetAttrOrNull("a")->UnsafeAs<int>(), 1);

    const Object& obj2_proto = *obj2.prototype();
    EXPECT_THAT(obj2_proto.attributes(), UnorderedElementsAre(Key("a")));
    EXPECT_EQ(obj2_proto.GetAttrOrNull("a")->UnsafeAs<int>(), 1);
    EXPECT_EQ(obj2_proto.prototype(), std::nullopt);
  }
}

TEST(ObjectTest, GetSortedAttributes) {
  {
    // Empty.
    Object obj;
    EXPECT_THAT(obj.GetSortedAttributes(), IsEmpty());
  }
  {
    // Attributes but no prototype.
    Object obj(
        {{"b", TypedValue::FromValue(1)}, {"a", TypedValue::FromValue(2.0f)}});
    auto sorted_attrs = obj.GetSortedAttributes();
    ASSERT_EQ(sorted_attrs.size(), 2);
    EXPECT_EQ(sorted_attrs[0]->first, "a");
    EXPECT_EQ(sorted_attrs[1]->first, "b");
    EXPECT_EQ(sorted_attrs[0]->second.UnsafeAs<float>(), 2.0f);
    EXPECT_EQ(sorted_attrs[1]->second.UnsafeAs<int>(), 1);
  }
  {
    // Attributes with prototype.
    Object obj1({{"a", TypedValue::FromValue(1)}});
    Object obj2({{"b", TypedValue::FromValue(2.0f)}}, obj1);
    Object obj3({{"b", TypedValue::FromValue(5.0)},  // shadowing.
                 {"c", TypedValue::FromValue(-1)}},
                obj2);
    auto sorted_attrs = obj3.GetSortedAttributes();
    ASSERT_EQ(sorted_attrs.size(), 2);
    EXPECT_EQ(sorted_attrs[0]->first, "b");
    EXPECT_EQ(sorted_attrs[1]->first, "c");
    EXPECT_EQ(sorted_attrs[0]->second.UnsafeAs<double>(), 5.0);
    EXPECT_EQ(sorted_attrs[1]->second.UnsafeAs<int>(), -1);
  }
}

TEST(ObjectTest, QType) {
  QTypePtr type = GetQType<Object>();
  EXPECT_NE(type, nullptr);
  EXPECT_EQ(type->name(), "OBJECT");
  EXPECT_EQ(type->type_info(), typeid(Object));
  EXPECT_EQ(type->value_qtype(), nullptr);
  EXPECT_EQ(type, GetQType<Object>());
  EXPECT_NE(type, GetQTypeQType());
}

TEST(ObjectTest, Fingerprint) {
  {
    // Same attributes, same order -> same fingerprint.
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    EXPECT_EQ(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Same attributes, different order -> same fingerprint.
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    Object obj2(
        {{"b", TypedValue::FromValue(2.0f)}, {"a", TypedValue::FromValue(1)}});
    EXPECT_EQ(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Same value, different attr names -> different fingerprint.
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"c", TypedValue::FromValue(2.0f)}});
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Different values, same attr names -> different fingerprint.
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    Object obj2(
        {{"a", TypedValue::FromValue(2.0f)}, {"b", TypedValue::FromValue(1)}});
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Same attributes, same attrs, same prototype -> same fingerprint.
    Object prototype({{"c", TypedValue::FromValue(1)}});
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}},
        prototype);
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}},
        prototype);
    EXPECT_EQ(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Same attributes, same attrs, different prototype -> different
    // fingerprint.
    Object prototype1({{"c", TypedValue::FromValue(1)}});
    Object prototype2({{"c", TypedValue::FromValue(2.0f)}});
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}},
        prototype1);
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}},
        prototype2);
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Same combined attrs, one with prototype -> different fingerprint.
    Object prototype({{"b", TypedValue::FromValue(1)}});
    Object obj1({{"a", TypedValue::FromValue(1)}}, prototype);
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // Same attributes, same attrs, one with empty prototype -> different
    // fingerprint.
    Object prototype;
    Object obj1(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}},
        prototype);
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // All attributes in object vs in prototype -> different fingerprint.
    Object prototype(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    Object obj1 = Object({}, prototype);
    Object obj2(
        {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}});
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
  {
    // No attributes with prototype vs attribute with name "prototype" ->
    // different fingerprint.
    Object prototype({{"a", TypedValue::FromValue(1)}});
    Object obj1 = Object({}, prototype);
    Object obj2 = Object({{"prototype", TypedValue::FromValue(prototype)}});
    EXPECT_NE(TypedValue::FromValue(obj1).GetFingerprint(),
              TypedValue::FromValue(obj2).GetFingerprint());
  }
}

TEST(ObjectTest, TypedValueRepr) {
  {
    // No attributes - no prototype.
    Object obj;
    EXPECT_THAT(TypedValue::FromValue(obj).GenReprToken(),
                ReprTokenEq("Object{attributes={}}"));
  }
  {
    // Single attribute.
    Object obj({{"a", TypedValue::FromValue(1)}});
    EXPECT_THAT(TypedValue::FromValue(obj).GenReprToken(),
                ReprTokenEq("Object{attributes={a=1}}"));
  }
  {
    // Attributes - no prototype.
    Object obj(
        {{"b", TypedValue::FromValue(1)}, {"a", TypedValue::FromValue(2.0f)}});
    EXPECT_THAT(TypedValue::FromValue(obj).GenReprToken(),
                ReprTokenEq("Object{attributes={a=2., b=1}}"));
  }
  {
    // No attributes - with prototype.
    Object prototype(
        {{"b", TypedValue::FromValue(1)}, {"a", TypedValue::FromValue(2.0f)}});
    Object obj({}, prototype);
    EXPECT_THAT(
        TypedValue::FromValue(obj).GenReprToken(),
        ReprTokenEq(
            "Object{attributes={}, prototype=Object{attributes={a=2., b=1}}}"));
  }
  {
    // Attributes and prototype chain.
    Object obj1({{"a", TypedValue::FromValue(1)}});
    Object obj2({{"b", TypedValue::FromValue(2)}}, obj1);
    Object obj3({{"c", TypedValue::FromValue(3)}}, obj2);
    EXPECT_THAT(
        TypedValue::FromValue(obj3).GenReprToken(),
        ReprTokenEq(
            "Object{attributes={c=3}, prototype=Object{attributes={b=2}, "
            "prototype=Object{attributes={a=1}}}}"));
  }
  {
    // Utf8SafeCHexEscaped.
    Object obj({{"foo\n", TypedValue::FromValue(1)}});
    EXPECT_THAT(TypedValue::FromValue(obj).GenReprToken(),
                ReprTokenEq("Object{attributes={foo\\n=1}}"));
  }
}

TEST(ObjectTest, CopyTo) {
  Object prototype({{"c", TypedValue::FromValue(1)}});
  Object obj(
      {{"a", TypedValue::FromValue(1)}, {"b", TypedValue::FromValue(2.0f)}},
      prototype);
  auto tv = TypedValue::FromValue(obj);
  auto tv_copy = TypedValue(tv.AsRef());  // Copies the raw (void) attributes.
  EXPECT_EQ(tv.GetFingerprint(), tv_copy.GetFingerprint());
}

}  // namespace
}  // namespace arolla
