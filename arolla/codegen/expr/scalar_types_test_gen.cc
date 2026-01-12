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
// Generates C++ code containing assignments for various literal types
// supported by code generation. The generated code is executed as a cc_test.

#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/codegen/expr/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

constexpr absl::string_view kProgFormat = R"RAW_PROG(
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/types.h"

namespace {

TEST(TypeAndLiteralsTest, All) {
%s
}

}  // namespace
)RAW_PROG";

using ::arolla::CreateDenseArray;
using ::arolla::OptionalValue;
using ::arolla::TypedValue;
using ::arolla::codegen::CppLiteralRepr;
using ::arolla::codegen::CppQTypeConstruction;
using ::arolla::codegen::CppTypeName;

void AssignTest(TypedValue typed_value, std::vector<std::string>* tests) {
  std::string literal = CppLiteralRepr(typed_value).value();
  ::arolla::QTypePtr qtype = typed_value.GetType();
  std::string type = CppTypeName(qtype).value();
  std::string qtype_construct = CppQTypeConstruction(qtype).value();
  tests->push_back(absl::StrFormat(R"RAW(
  {
      EXPECT_EQ(%s->name(), "%s");
      // Value: %s
      const auto& v = %s;
      static_assert(std::is_same_v<decltype(v), const %s&>);
  })RAW",
                                   qtype_construct, qtype->name(),
                                   typed_value.Repr(), literal, type));
}

template <class T>
void AssignTest(T value, std::vector<std::string>* tests) {
  AssignTest(TypedValue::FromValue<T>(value), tests);
}

template <class T>
void AssignOptionalTest(const OptionalValue<T>& value,
                        std::vector<std::string>* tests) {
  AssignTest(value, tests);
}

template <class T>
void AssignDenseArrayTest(const std::vector<OptionalValue<T>>& value,
                          std::vector<std::string>* tests) {
  AssignTest(CreateDenseArray<T>(value), tests);
}

template <class T>
void AssignDictTest(const std::vector<std::pair<T, int64_t>>& vec,
                    std::vector<std::string>* tests) {
  ::arolla::KeyToRowDict<T> value({vec.begin(), vec.end()});
  AssignTest(value, tests);
}

template <class... Ts>
void AssignTupleTest(std::vector<std::string>* tests, const Ts&... values) {
  AssignTest(::arolla::MakeTupleFromFields(values...), tests);
}

int main() {
  std::vector<std::string> tests;
  // Scalars
  AssignTest(::arolla::kUnit, &tests);
  // float
  AssignTest(1.0f, &tests);
  AssignTest(std::numeric_limits<float>::infinity(), &tests);
  AssignTest(-std::numeric_limits<float>::infinity(), &tests);
  AssignTest(std::numeric_limits<float>::quiet_NaN(), &tests);
  AssignTest(-std::numeric_limits<float>::quiet_NaN(), &tests);
  AssignTest(-0.0f, &tests);
  AssignTest(1.0f - 1e-7f, &tests);
  // double
  AssignTest(1.0, &tests);
  AssignTest(std::numeric_limits<double>::infinity(), &tests);
  AssignTest(-std::numeric_limits<double>::infinity(), &tests);
  AssignTest(std::numeric_limits<double>::quiet_NaN(), &tests);
  AssignTest(-std::numeric_limits<double>::quiet_NaN(), &tests);
  AssignTest(-0.0, &tests);
  AssignTest(1.0 - 1e-9, &tests);
  // ints
  AssignTest<int>(1, &tests);
  AssignTest<int32_t>(1, &tests);
  AssignTest<int64_t>(9223372036854775807ll, &tests);
  AssignTest<uint64_t>(18446744073709551615ull, &tests);
  // bool
  AssignTest<bool>(true, &tests);
  // strings
  AssignTest<::arolla::Bytes>(::arolla::Bytes("\"bytes\""), &tests);
  AssignTest<::arolla::Text>(::arolla::Text("\"text\""), &tests);

  // Empty optionals:
  AssignOptionalTest<::arolla::Unit>({}, &tests);
  AssignOptionalTest<float>({}, &tests);
  AssignOptionalTest<double>({}, &tests);
  AssignOptionalTest<int>({}, &tests);
  AssignOptionalTest<int32_t>({}, &tests);
  AssignOptionalTest<int64_t>({}, &tests);
  AssignOptionalTest<uint64_t>({}, &tests);
  AssignOptionalTest<bool>({}, &tests);
  AssignOptionalTest<::arolla::Bytes>({}, &tests);
  AssignOptionalTest<::arolla::Text>({}, &tests);

  // Present optionals:
  AssignOptionalTest<::arolla::Unit>(::arolla::kUnit, &tests);
  AssignOptionalTest<float>(1.0f, &tests);
  AssignOptionalTest<double>(2.0, &tests);
  AssignOptionalTest<int>(1, &tests);
  AssignOptionalTest<int32_t>(1, &tests);
  AssignOptionalTest<int64_t>(9223372036854775807ll, &tests);
  AssignOptionalTest<uint64_t>(18446744073709551615ull, &tests);
  AssignOptionalTest<bool>(true, &tests);
  AssignOptionalTest<::arolla::Bytes>(::arolla::Bytes("bytes"), &tests);
  AssignOptionalTest<::arolla::Text>(::arolla::Text("text"), &tests);

  // DenseArrayShape:
  AssignTest<::arolla::DenseArrayShape>(::arolla::DenseArrayShape{5}, &tests);

  // DenseArrayEdge:
  AssignTest<::arolla::DenseArrayEdge>(
      ::arolla::DenseArrayEdge::FromSplitPoints(
          ::arolla::CreateDenseArray<int64_t>({0, 3, 5}))
          .value(),
      &tests);
  AssignTest<::arolla::DenseArrayEdge>(
      ::arolla::DenseArrayEdge::FromMapping(
          ::arolla::CreateDenseArray<int64_t>({0, 0, 1, 1, 3, 4}), 6)
          .value(),
      &tests);
  AssignTest<::arolla::DenseArrayEdge>(
      ::arolla::DenseArrayEdge::FromMapping(
          ::arolla::CreateDenseArray<int64_t>({0, std::nullopt, 1, 1, 3, 4}), 5)
          .value(),
      &tests);

  // DenseArrayGroupScalarEdge:
  AssignTest<::arolla::DenseArrayGroupScalarEdge>(
      ::arolla::DenseArrayGroupScalarEdge(17), &tests);

  // Empty DenseArray:
  AssignDenseArrayTest<float>({}, &tests);
  AssignDenseArrayTest<float>({std::nullopt}, &tests);
  AssignDenseArrayTest<double>({}, &tests);
  AssignDenseArrayTest<double>({std::nullopt, std::nullopt}, &tests);
  AssignDenseArrayTest<int>({}, &tests);
  AssignDenseArrayTest<int32_t>({}, &tests);
  AssignDenseArrayTest<int64_t>({}, &tests);
  AssignDenseArrayTest<uint64_t>({}, &tests);
  AssignDenseArrayTest<bool>({}, &tests);
  AssignDenseArrayTest<::arolla::Bytes>({}, &tests);
  AssignDenseArrayTest<::arolla::Text>({}, &tests);

  // Present DenseArray:
  AssignDenseArrayTest<float>({std::nullopt, 2.0f - 1e-7f}, &tests);
  AssignDenseArrayTest<double>({2.0, std::nullopt, 1.0 - 1e-9}, &tests);
  AssignDenseArrayTest<int>({1, std::nullopt, 3}, &tests);
  AssignDenseArrayTest<int32_t>({1, std::nullopt}, &tests);
  AssignDenseArrayTest<int64_t>({9223372036854775807ll, std::nullopt}, &tests);
  AssignDenseArrayTest<uint64_t>({std::nullopt, 18446744073709551615ull},
                                 &tests);
  AssignDenseArrayTest<bool>({true, false, std::nullopt}, &tests);
  AssignDenseArrayTest<::arolla::Bytes>(
      {std::nullopt, ::arolla::Bytes("bytes")}, &tests);
  AssignDenseArrayTest<::arolla::Text>({::arolla::Text("text"), std::nullopt},
                                       &tests);

  // Empty dicts:
  AssignDictTest<int>({}, &tests);
  AssignDictTest<int32_t>({}, &tests);
  AssignDictTest<int64_t>({}, &tests);
  AssignDictTest<uint64_t>({}, &tests);
  AssignDictTest<bool>({}, &tests);
  AssignDictTest<::arolla::Bytes>({}, &tests);
  AssignDictTest<::arolla::Text>({}, &tests);

  // Present dicts:
  AssignDictTest<int>({{1, 1}, {3, 4}}, &tests);
  AssignDictTest<int32_t>({{6, 7}}, &tests);
  AssignDictTest<int64_t>({{7, 9}}, &tests);
  AssignDictTest<uint64_t>({{6, 1}}, &tests);
  AssignDictTest<bool>({{true, 1}, {false, 2}}, &tests);
  AssignDictTest<::arolla::Bytes>({{::arolla::Bytes("bytes"), 3}}, &tests);
  AssignDictTest<::arolla::Text>({{::arolla::Text("text"), 3}}, &tests);

  // Tuples
  AssignTupleTest(&tests);  // empty
  AssignTupleTest(&tests, int{1});
  AssignTupleTest(&tests, int{1}, ::arolla::Bytes("bytes"));
  AssignTupleTest(&tests, ::arolla::MakeOptionalValue(1),
                  ::arolla::Bytes("bytes"));
  AssignTupleTest(  // Nested tuple
      &tests, ::arolla::MakeTupleFromFields(),
      ::arolla::MakeTupleFromFields(::arolla::MakeTupleFromFields(),
                                    ::arolla::MakeTupleFromFields()),
      ::arolla::MakeTupleFromFields(1));
  // Deeply nested tuple.
  auto nested_tuple = ::arolla::MakeTupleFromFields(
      ::arolla::MakeTupleFromFields(), ::arolla::MakeTupleFromFields());
  for (int i = 0; i < 57; ++i) {
    nested_tuple = ::arolla::MakeTupleFromFields(nested_tuple);
  }
  AssignTupleTest(&tests, nested_tuple, nested_tuple);

  std::cout << absl::StrFormat(kProgFormat, absl::StrJoin(tests, "\n"))
            << std::endl;
}
