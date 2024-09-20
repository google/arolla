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
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(StringsTest, AsText) {
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", kUnit),
              IsOkAndHolds(Text("present")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", Text("text")),
              IsOkAndHolds(Text("text")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text",
                                   Bytes(std::string({0, 'b', '\'', 'e', 1}))),
              IsOkAndHolds(Text("b'\\x00\\x62\\'e\\x01'")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", false),
              IsOkAndHolds(Text("false")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", int32_t{1}),
              IsOkAndHolds(Text("1")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", int64_t{1}),
              IsOkAndHolds(Text("1")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", 2.3f),
              IsOkAndHolds(Text("2.3")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", 2.3),
              IsOkAndHolds(Text("2.3")));

  // absl::StrFormat("%g") behaves differently on these cases.
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", 14.137167f),
              IsOkAndHolds(Text("14.137167")));
  EXPECT_THAT(InvokeOperator<Text>("strings.as_text", 14.137167),
              IsOkAndHolds(Text("14.137167")));

  // DenseArray<Bytes> does not go through "Bytes" code path, so test it
  // explicitly.
  EXPECT_THAT(
      InvokeOperator<DenseArray<Text>>(
          "strings.as_text", CreateDenseArray<Bytes>(
                                 {Bytes(std::string({0, 'b', '\'', 'e', 1}))})),
      IsOkAndHolds(ElementsAre(Text("b'\\x00\\x62\\'e\\x01'"))));
}

TEST(StringsTest, Decode) {
  EXPECT_THAT(InvokeOperator<Text>("strings.decode", Bytes("text")),
              IsOkAndHolds(Text("text")));
  EXPECT_THAT(InvokeOperator<Text>("strings.decode", Bytes("te\0xt")),
              IsOkAndHolds(Text("te\0xt")));
  // \xEF\xBF\xBD is UTF-8 encoded \xFFFD, which is handled specially by some
  // ICU macros. So we check it explicitly.
  EXPECT_THAT(InvokeOperator<Text>("strings.decode", Bytes("\xEF\xBF\xBD")),
              IsOkAndHolds(Text("\xEF\xBF\xBD")));
  EXPECT_THAT(InvokeOperator<Text>("strings.decode", Bytes("\xA0text")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid UTF-8 sequence at position 0")));
  EXPECT_THAT(InvokeOperator<Text>("strings.decode", Bytes("te\xC0\0xt")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid UTF-8 sequence at position 2")));
  EXPECT_THAT(InvokeOperator<Text>("strings.decode", Bytes("text\x80")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid UTF-8 sequence at position 4")));
}

TEST(StringsTest, Lower) {
  Text input("Hello World.");
  Text expected_output("hello world.");
  EXPECT_THAT(InvokeOperator<Text>("strings.lower", input),
              IsOkAndHolds(expected_output));
}

TEST(StringsTest, LowerOptional) {
  OptionalValue<Text> input(Text("Hello World."));
  OptionalValue<Text> expected_output(Text("hello world."));
  EXPECT_THAT(InvokeOperator<OptionalValue<Text>>("strings.lower", input),
              IsOkAndHolds(expected_output));
}

TEST(StringsTest, LowerWithLocale) {
  Text input("TITLE");
  Text locale("TR_tr");
  EXPECT_THAT(InvokeOperator<Text>("strings.lower", input, locale),
              IsOkAndHolds(Text("tıtle")));
}

TEST(StringsTest, Upper) {
  Text input("Hello World.");
  EXPECT_THAT(InvokeOperator<Text>("strings.upper", input),
              IsOkAndHolds(Text("HELLO WORLD.")));
}

TEST(StringsTest, UpperWithLocale) {
  Text input("istanbul");
  Text locale("TR_tr");
  EXPECT_THAT(InvokeOperator<Text>("strings.upper", input, locale),
              IsOkAndHolds(Text("İSTANBUL")));
}

TEST(StringsTest, BytesLength) {
  EXPECT_THAT(InvokeOperator<int32_t>("strings.length",
                                      Bytes("古池や蛙飛び込む水の音")),
              IsOkAndHolds(33));
}

TEST(StringsTest, TextLength) {
  EXPECT_THAT(
      InvokeOperator<int32_t>("strings.length", Text("古池や蛙飛び込む水の音")),
      IsOkAndHolds(11));
}

TEST(StringsTest, Replace) {
  Text input("Hello ello foo.");
  Text old_sub("ell");
  Text new_sub("XX");
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", input, old_sub, new_sub,
                                   OptionalValue<int32_t>(-1)),
              IsOkAndHolds(Text("HXXo XXo foo.")));
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", input, old_sub, new_sub,
                                   OptionalValue<int32_t>(0)),
              IsOkAndHolds(Text("Hello ello foo.")));
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", input, old_sub, new_sub,
                                   OptionalValue<int32_t>(1)),
              IsOkAndHolds(Text("HXXo ello foo.")));
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", input, old_sub, new_sub,
                                   OptionalValue<int32_t>(2)),
              IsOkAndHolds(Text("HXXo XXo foo.")));
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", input, old_sub, new_sub,
                                   OptionalValue<int32_t>()),
              IsOkAndHolds(Text("HXXo XXo foo.")));

  // Fencing:
  EXPECT_THAT(
      InvokeOperator<Text>("strings.replace", input, Text(), new_sub,
                           OptionalValue<int32_t>(-1)),
      IsOkAndHolds(Text("XXHXXeXXlXXlXXoXX XXeXXlXXlXXoXX XXfXXoXXoXX.XX")));
  EXPECT_THAT(
      InvokeOperator<Text>("strings.replace", input, Text(), new_sub,
                           OptionalValue<int32_t>()),
      IsOkAndHolds(Text("XXHXXeXXlXXlXXoXX XXeXXlXXlXXoXX XXfXXoXXoXX.XX")));
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", input, Text(), new_sub,
                                   OptionalValue<int32_t>(3)),
              IsOkAndHolds(Text("XXHXXeXXllo ello foo.")));
  EXPECT_THAT(InvokeOperator<Text>("strings.replace", Text(), Text(), new_sub,
                                   OptionalValue<int32_t>()),
              IsOkAndHolds(Text("XX")));
}

}  // namespace
}  // namespace arolla
