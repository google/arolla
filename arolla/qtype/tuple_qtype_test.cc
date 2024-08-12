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
#include "arolla/qtype/tuple_qtype.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla::testing {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::ReprTokenEq;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::MatchesRegex;

TEST(TupleQType, Empty) {
  auto qtype = MakeTupleQType({});
  EXPECT_TRUE(IsTupleQType(qtype));
  EXPECT_EQ(qtype->name(), "tuple<>");
  EXPECT_EQ(qtype->type_layout().AllocSize(), 0);
  EXPECT_EQ(qtype->type_layout().AllocAlignment().value, 1);
  auto value = MakeTupleFromFields();
  EXPECT_EQ(value.GetType(), qtype);
  EXPECT_EQ(value.GetFieldCount(), 0);
  EXPECT_THAT(value.GenReprToken(), ReprTokenEq("()"));
}

TEST(TupleQType, EmptyRegression) {
  auto qtype_0 = MakeTupleQType({});
  auto qtype_1 = MakeTupleQType({qtype_0, qtype_0});
  EXPECT_TRUE(IsTupleQType(qtype_1));
  EXPECT_EQ(qtype_1->name(), "tuple<tuple<>,tuple<>>");
  EXPECT_EQ(qtype_1->type_layout().AllocSize(), 0);
  EXPECT_EQ(qtype_1->type_layout().AllocAlignment().value, 1);
  auto value_0 = MakeTupleFromFields();
  auto value_1 = MakeTupleFromFields(value_0, value_0);
  EXPECT_EQ(value_1.GetType(), qtype_1);
  auto copy_1 = TypedValue(value_1.AsRef());
  EXPECT_EQ(value_1.GetFingerprint(), copy_1.GetFingerprint());
  EXPECT_THAT(value_1.GenReprToken(), ReprTokenEq("((), ())"));
}

TEST(TupleQType, Trivial) {
  auto qtype = MakeTupleQType(
      {GetQType<int32_t>(), GetQType<double>(), GetQType<Bytes>()});
  EXPECT_TRUE(IsTupleQType(qtype));
  EXPECT_EQ(qtype->name(), "tuple<INT32,FLOAT64,BYTES>");
  auto value = MakeTupleFromFields(int32_t{34}, double{17}, Bytes("Hello"));
  EXPECT_EQ(value.GetType(), qtype);
  EXPECT_EQ(value.GetFieldCount(), 3);
  EXPECT_THAT(value.GetField(0).As<int32_t>(), IsOkAndHolds(int32_t{34}));
  EXPECT_THAT(value.GetField(1).As<double>(), IsOkAndHolds(double{17.}));
  ASSERT_OK_AND_ASSIGN(Bytes bytes, value.GetField(2).As<Bytes>());
  EXPECT_THAT(bytes, Eq(Bytes("Hello")));
  EXPECT_THAT(value.GenReprToken(), ReprTokenEq("(34, float64{17}, b'Hello')"));
}

TEST(TupleQType, CopyTo) {
  auto qtype = MakeTupleQType(
      {GetQType<int32_t>(), GetQType<double>(), GetQType<Bytes>()});
  EXPECT_TRUE(IsTupleQType(qtype));
  EXPECT_EQ(qtype->name(), "tuple<INT32,FLOAT64,BYTES>");
  auto value = MakeTupleFromFields(int32_t{34}, double{17}, Bytes("Hello"));
  EXPECT_THAT(value.GetField(0).As<int32_t>(), IsOkAndHolds(int32_t{34}));
  EXPECT_THAT(value.GetField(1).As<double>(), IsOkAndHolds(double{17.}));

  auto copy = TypedValue(value.AsRef());
  EXPECT_EQ(value.GetFingerprint(), copy.GetFingerprint());
  EXPECT_THAT(copy.GenReprToken(), ReprTokenEq("(34, float64{17}, b'Hello')"));
}

TEST(TupleQType, QValueFromFields) {
  auto qtype = MakeTupleQType({GetQType<int>(), GetQType<float>()});
  {  // From typed_refs.
    ASSERT_OK_AND_ASSIGN(auto qvalue, TypedValue::FromFields(
                                          qtype, {TypedRef::FromValue(2),
                                                  TypedRef::FromValue(3.14f)}));
    EXPECT_THAT(qvalue.GetField(0).As<int>(), IsOkAndHolds(2));
    EXPECT_THAT(qvalue.GetField(1).As<float>(), IsOkAndHolds(3.14f));
  }
  {  // From typed_values.
    ASSERT_OK_AND_ASSIGN(
        auto qvalue,
        TypedValue::FromFields(
            qtype, {TypedValue::FromValue(2), TypedValue::FromValue(3.14f)}));
    EXPECT_THAT(qvalue.GetField(0).As<int>(), IsOkAndHolds(2));
    EXPECT_THAT(qvalue.GetField(1).As<float>(), IsOkAndHolds(3.14f));
  }
  {  // error: mismatched number of fields
    EXPECT_THAT(TypedValue::FromFields(qtype, {TypedValue::FromValue(2)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected 2 values, got 1; "
                                   "compound_qtype=tuple<INT32,FLOAT32>")));
  }
  {  // error: mismatched field qtype
    EXPECT_THAT(TypedValue::FromFields(qtype, {TypedValue::FromValue(2),
                                               TypedValue::FromValue(3)}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected fields[1]: FLOAT32, got INT32; "
                                   "compound_qtype=tuple<INT32,FLOAT32>")));
  }
}

TEST(NamedTupleQType, Empty) {
  auto tuple_qtype = MakeTupleQType({});
  ASSERT_OK_AND_ASSIGN(auto qtype, MakeNamedTupleQType({}, tuple_qtype));
  EXPECT_TRUE(IsNamedTupleQType(qtype));
  EXPECT_THAT(GetFieldNames(qtype), IsEmpty());
}

TEST(NamedTupleQType, Trivial) {
  auto tuple_qtype = MakeTupleQType(
      {GetQType<int32_t>(), GetQType<double>(), GetQType<Bytes>()});
  ASSERT_OK_AND_ASSIGN(auto qtype,
                       MakeNamedTupleQType({"a", "b", "c"}, tuple_qtype));
  EXPECT_TRUE(IsNamedTupleQType(qtype));
  EXPECT_EQ(qtype->name(), "namedtuple<a=INT32,b=FLOAT64,c=BYTES>");
  EXPECT_EQ(GetFieldIndexByName(nullptr, "a"), std::nullopt);
  EXPECT_EQ(GetFieldIndexByName(qtype, "a"), 0);
  EXPECT_EQ(GetFieldIndexByName(qtype, "b"), 1);
  EXPECT_EQ(GetFieldIndexByName(qtype, "c"), 2);
  EXPECT_EQ(GetFieldIndexByName(qtype, "d"), std::nullopt);
  EXPECT_THAT(GetFieldNames(qtype), ElementsAre("a", "b", "c"));

  EXPECT_EQ(GetFieldQTypeByName(nullptr, "a"), nullptr);
  EXPECT_EQ(GetFieldQTypeByName(qtype, "a"), GetQType<int32_t>());
  EXPECT_EQ(GetFieldQTypeByName(qtype, "b"), GetQType<double>());
  EXPECT_EQ(GetFieldQTypeByName(qtype, "c"), GetQType<Bytes>());
  EXPECT_EQ(GetFieldQTypeByName(qtype, "d"), nullptr);

  auto derived_qtype_interface =
      dynamic_cast<const DerivedQTypeInterface*>(qtype);
  ASSERT_NE(derived_qtype_interface, nullptr);
  EXPECT_EQ(derived_qtype_interface->GetBaseQType(), tuple_qtype);

  {
    ASSERT_OK_AND_ASSIGN(auto qtype2,
                         MakeNamedTupleQType({"a", "b", "c"}, tuple_qtype));
    EXPECT_EQ(qtype, qtype2);
    EXPECT_THAT(GetFieldNames(qtype2), ElementsAre("a", "b", "c"));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto qtype2,
                         MakeNamedTupleQType({"c", "b", "a"}, tuple_qtype));
    EXPECT_EQ(qtype2->name(), "namedtuple<c=INT32,b=FLOAT64,a=BYTES>");
    EXPECT_EQ(GetFieldIndexByName(qtype2, "c"), 0);
    EXPECT_NE(qtype, qtype2);
    EXPECT_THAT(GetFieldNames(qtype2), ElementsAre("c", "b", "a"));
  }
  {
    auto tuple_qtype2 = MakeTupleQType(
        {GetQType<int32_t>(), GetQType<double>(), GetQType<int32_t>()});
    ASSERT_OK_AND_ASSIGN(auto qtype2,
                         MakeNamedTupleQType({"a", "b", "c"}, tuple_qtype2));
    EXPECT_EQ(qtype2->name(), "namedtuple<a=INT32,b=FLOAT64,c=INT32>");
    EXPECT_NE(qtype, qtype2);
    EXPECT_THAT(GetFieldNames(qtype2), ElementsAre("a", "b", "c"));
  }
}

TEST(NamedTupleQType, QValueFromFields) {
  auto tuple_qtype = MakeTupleQType({GetQType<int>(), GetQType<float>()});
  ASSERT_OK_AND_ASSIGN(auto qtype,
                       MakeNamedTupleQType({"a", "b"}, tuple_qtype));
  {  // From typed_refs.
    ASSERT_OK_AND_ASSIGN(auto qvalue, TypedValue::FromFields(
                                          qtype, {TypedRef::FromValue(2),
                                                  TypedRef::FromValue(3.14f)}));
    EXPECT_TRUE(IsNamedTupleQType(qvalue.GetType()));
    EXPECT_EQ(qvalue.GetType(), qtype);
    EXPECT_THAT(qvalue.GetField(0).As<int>(), IsOkAndHolds(2));
    EXPECT_THAT(qvalue.GetField(1).As<float>(), IsOkAndHolds(3.14f));
  }
  {  // From typed_values.
    ASSERT_OK_AND_ASSIGN(
        auto qvalue,
        TypedValue::FromFields(
            qtype, {TypedValue::FromValue(2), TypedValue::FromValue(3.14f)}));
    EXPECT_TRUE(IsNamedTupleQType(qvalue.GetType()));
    EXPECT_EQ(qvalue.GetType(), qtype);
    EXPECT_THAT(qvalue.GetField(0).As<int>(), IsOkAndHolds(2));
    EXPECT_THAT(qvalue.GetField(1).As<float>(), IsOkAndHolds(3.14f));
  }
  {  // error: mismatched number of fields
    EXPECT_THAT(
        TypedValue::FromFields(qtype, {TypedValue::FromValue(2)}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected 2 values, got 1; "
                           "compound_qtype=namedtuple<a=INT32,b=FLOAT32>")));
  }
  {  // error: mismatched field qtype
    EXPECT_THAT(
        TypedValue::FromFields(qtype, {TypedValue::FromValue(2),
                                       TypedValue::FromValue(3)}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected fields[1]: FLOAT32, got INT32; "
                           "compound_qtype=namedtuple<a=INT32,b=FLOAT32>")));
  }
}

TEST(NamedTupleQType, BigTuple) {
  constexpr size_t kFieldCount = 100;
  QTypePtr field_qtype = GetQType<int32_t>();
  auto tuple_qtype =
      MakeTupleQType(std::vector<QTypePtr>{kFieldCount, field_qtype});
  std::vector<std::string> names;
  for (size_t i = 0; i != kFieldCount; ++i) {
    names.push_back(std::to_string(i));
  }
  ASSERT_OK_AND_ASSIGN(auto qtype, MakeNamedTupleQType(names, tuple_qtype));
  EXPECT_TRUE(IsNamedTupleQType(qtype));
  EXPECT_THAT(GetFieldNames(qtype), ElementsAreArray(names));
  EXPECT_EQ(qtype->name(),
            "namedtuple<0=INT32,1=INT32,2=INT32,3=INT32,4=INT32, [95 fields]>");
}

TEST(NamedTupleQType, Errors) {
  EXPECT_THAT(
      MakeNamedTupleQType({"a", "b"}, nullptr).status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(".*NamedTupleQType.*tuple.*found.*nullptr.*")));
  EXPECT_THAT(
      MakeNamedTupleQType({"a", "b"}, GetQType<int32_t>()).status(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(".*NamedTupleQType.*tuple.*found.*INT32.*")));
  auto tuple_qtype = MakeTupleQType(
      {GetQType<int32_t>(), GetQType<double>(), GetQType<Bytes>()});
  EXPECT_THAT(MakeNamedTupleQType({"a", "b"}, tuple_qtype).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex(".*NamedTupleQType.*2 vs 3.*")));
  EXPECT_THAT(MakeNamedTupleQType({"a", "b", "a"}, tuple_qtype).status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex(".*NamedTupleQType.*a.*duplicate.*")));

  EXPECT_THAT(GetFieldNames(nullptr), IsEmpty());
  EXPECT_THAT(GetFieldNames(GetQType<int32_t>()), IsEmpty());
}

TEST(NamedTupleQType, GetFieldByNameAs) {
  ASSERT_OK_AND_ASSIGN(auto named_tuple, MakeNamedTuple(
      {"a", "b"}, {TypedRef::FromValue(2.0f), TypedRef::FromValue(3)}));

  EXPECT_THAT(GetFieldByNameAs<float>(named_tuple.AsRef(), "a"),
              IsOkAndHolds(2.0f));
  EXPECT_THAT(GetFieldByNameAs<float>(named_tuple.AsRef(), "c").status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       MatchesRegex(".*no field named \"c\".*")));
  EXPECT_THAT(
      GetFieldByNameAs<Bytes>(named_tuple.AsRef(), "a").status(),
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          HasSubstr("type mismatch: expected C++ type `float` (FLOAT32), got "
                    "`arolla::Bytes`; while accessing field \"a\"")));
}

TEST(NamedTupleQType, MakeNamedTuple) {
  ASSERT_OK_AND_ASSIGN(auto named_tuple,
                       MakeNamedTuple({"a", "b"}, {TypedRef::FromValue(2.0f),
                                                   TypedRef::FromValue(3)}));
  ASSERT_OK_AND_ASSIGN(
      auto named_tuple_qtype,
      MakeNamedTupleQType(
          {"a", "b"}, MakeTupleQType({GetQType<float>(), GetQType<int>()})));
  EXPECT_EQ(named_tuple.GetType(), named_tuple_qtype);
  EXPECT_THAT(named_tuple.GenReprToken(),
              ReprTokenEq("namedtuple<a=FLOAT32,b=INT32>{(2., 3)}"));
  EXPECT_EQ(named_tuple.GetFieldCount(), 2);
}

TEST(NamedTupleQType, MakeEmptyNamedTuple) {
  ASSERT_OK_AND_ASSIGN(auto named_tuple,
                       MakeNamedTuple({}, absl::Span<const TypedRef>{}));
  ASSERT_OK_AND_ASSIGN(auto named_tuple_qtype,
                       MakeNamedTupleQType({}, MakeTupleQType({})));
  EXPECT_EQ(named_tuple.GetType(), named_tuple_qtype);
  EXPECT_THAT(named_tuple.GenReprToken(), ReprTokenEq("namedtuple<>{()}"));
  EXPECT_EQ(named_tuple.GetFieldCount(), 0);
}

TEST(NamedTupleQtype, MakeNamedTuple_SameFromTypedValueAndTypedRef) {
  ASSERT_OK_AND_ASSIGN(TypedValue named_tuple_from_values,
                       MakeNamedTuple({"a", "b"}, {TypedValue::FromValue(2.0f),
                                                   TypedValue::FromValue(3)}));

  ASSERT_OK_AND_ASSIGN(auto named_tuple_from_refs,
                       MakeNamedTuple({"a", "b"}, {TypedRef::FromValue(2.0f),
                                                   TypedRef::FromValue(3)}));

  EXPECT_EQ(named_tuple_from_values.GetFingerprint(),
            named_tuple_from_refs.GetFingerprint());
}

TEST(NamedTupleQType, MakeNamedTuple_Error) {
  EXPECT_THAT(
      MakeNamedTuple({"a"},
                     {TypedValue::FromValue(2.0f), TypedValue::FromValue(3)}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          MatchesRegex(
              "incorrect NamedTupleQType #field_names != #fields: 1 vs 2")));
}

}  // namespace
}  // namespace arolla::testing
