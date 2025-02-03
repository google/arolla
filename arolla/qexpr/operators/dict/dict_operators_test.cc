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
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::Property;
using ::testing::UnorderedElementsAre;

TEST(DictOperatorsTest, MakeKeyToRowDict) {
  EXPECT_THAT(
      InvokeOperator<KeyToRowDict<int32_t>>("dict._make_key_to_row_dict",
                                            CreateDenseArray<int32_t>({2, 57})),
      IsOkAndHolds(Property(&KeyToRowDict<int32_t>::map,
                            UnorderedElementsAre(Pair(2, 0), Pair(57, 1)))));
  EXPECT_THAT(
      InvokeOperator<KeyToRowDict<Bytes>>(
          "dict._make_key_to_row_dict",
          CreateDenseArray<Bytes>({Bytes("foo"), Bytes("bar")})),
      IsOkAndHolds(Property(
          &KeyToRowDict<Bytes>::map,
          UnorderedElementsAre(Pair(Bytes("foo"), 0), Pair(Bytes("bar"), 1)))));
  EXPECT_THAT(InvokeOperator<KeyToRowDict<Bytes>>(
                  "dict._make_key_to_row_dict",
                  CreateDenseArray<Bytes>({Bytes("foo"), Bytes("foo")})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("duplicated key b'foo' in the dict")));
  // TODO: Prohibit missing keys.
  // EXPECT_THAT(InvokeOperator<KeyToRowDict<Bytes>>(
  //                 "dict._make_key_to_row_dict",
  //                 CreateDenseArray<Bytes>({Bytes("foo"), std::nullopt})),
  //             StatusIs(absl::StatusCode::kInvalidArgument,
  //                      HasSubstr("Missing key in the dict")));
}

TEST(DictOperatorsTest, GetRow) {
  using ORI = OptionalValue<int64_t>;
  using OB = OptionalValue<Bytes>;

  KeyToRowDict<Bytes> dict({{Bytes("foo"), 5}, {Bytes("bar"), 7}});

  EXPECT_THAT(InvokeOperator<ORI>("dict._get_row", dict, Bytes("foo")),
              IsOkAndHolds(Eq(ORI(5))));
  EXPECT_THAT(InvokeOperator<ORI>("dict._get_row", dict, Bytes("unknown")),
              IsOkAndHolds(Eq(ORI())));
  EXPECT_THAT(InvokeOperator<ORI>("dict._get_row", dict, OB(Bytes("foo"))),
              IsOkAndHolds(Eq(ORI(5))));
  EXPECT_THAT(InvokeOperator<ORI>("dict._get_row", dict, OB()),
              IsOkAndHolds(Eq(ORI())));

  DenseArray<Bytes> dense_array =
      CreateDenseArray<Bytes>({Bytes{"foo"}, Bytes{"unknown"}, std::nullopt});
  {
    ASSERT_OK_AND_ASSIGN(auto res, InvokeOperator<DenseArray<int64_t>>(
                                       "dict._get_row", dict, dense_array));
    EXPECT_THAT(res, ElementsAre(5, std::nullopt, std::nullopt));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<int64_t>>("dict._get_row", dict,
                                                 Array<Bytes>(dense_array)));
    EXPECT_THAT(res, ElementsAre(5, std::nullopt, std::nullopt));
  }
}

TEST(DictOperatorsTest, Contains) {
  KeyToRowDict<Bytes> dict({{Bytes("foo"), 5}, {Bytes("bar"), 7}});

  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("dict._contains", dict, Bytes("foo")),
      IsOkAndHolds(Eq(kPresent)));
  EXPECT_THAT(
      InvokeOperator<OptionalUnit>("dict._contains", dict, Bytes("unknown")),
      IsOkAndHolds(Eq(kMissing)));
  EXPECT_THAT(InvokeOperator<OptionalUnit>("dict._contains", dict,
                                           OptionalValue<Bytes>(Bytes("foo"))),
              IsOkAndHolds(Eq(kPresent)));
  EXPECT_THAT(InvokeOperator<OptionalUnit>("dict._contains", dict,
                                           OptionalValue<Bytes>()),
              IsOkAndHolds(Eq(kMissing)));

  DenseArray<Bytes> dense_array =
      CreateDenseArray<Bytes>({Bytes{"foo"}, Bytes{"unknown"}, std::nullopt});
  {
    ASSERT_OK_AND_ASSIGN(auto res, InvokeOperator<DenseArray<Unit>>(
                                       "dict._contains", dict, dense_array));
    EXPECT_THAT(res, ElementsAre(kPresent, kMissing, kMissing));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto res, InvokeOperator<Array<Unit>>("dict._contains", dict,
                                              Array<Bytes>(dense_array)));
    EXPECT_THAT(res, ElementsAre(kPresent, kMissing, kMissing));
  }
}

TEST(DictOperatorsTest, Keys) {
  EXPECT_THAT(InvokeOperator<DenseArray<Bytes>>(
                  "dict._keys",
                  KeyToRowDict<Bytes>({{Bytes("foo"), 1}, {Bytes("bar"), 0}})),
              IsOkAndHolds(ElementsAre("bar", "foo")));
  EXPECT_THAT(
      InvokeOperator<DenseArray<Bytes>>(
          "dict._keys",
          KeyToRowDict<Bytes>({{Bytes("foo"), 57}, {Bytes("bar"), 0}})),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "unexpected row ids in the key-to-row mapping in the dict")));
  EXPECT_THAT(
      InvokeOperator<DenseArray<Bytes>>(
          "dict._keys",
          KeyToRowDict<Bytes>({{Bytes("foo"), -1}, {Bytes("bar"), 0}})),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "unexpected row ids in the key-to-row mapping in the dict")));
  EXPECT_THAT(InvokeOperator<DenseArray<Bytes>>(
                  "dict._keys",
                  KeyToRowDict<Bytes>({{Bytes("foo"), 0}, {Bytes("bar"), 0}})),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("incomplete key-to-row mapping in the dict")));
}

}  // namespace
}  // namespace arolla
