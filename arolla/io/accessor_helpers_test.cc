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
#include "arolla/io/accessor_helpers.h"

#include <string>
#include <tuple>
#include <utility>

#include "gtest/gtest.h"

namespace arolla::accessor_helpers_impl {
namespace {

struct TestStruct {
  int a;
  double b;
};

struct GetAConstRef {
  const int& operator()(const TestStruct& s) const { return s.a; }
};

struct GetBValue {
  double operator()(const TestStruct& s) const { return s.b; }
};

TEST(InputLoaderTest, ConvertNameAccessorsPackToNestedTuple) {
  {  // convert from functors
    std::tuple<std::pair<std::string, GetAConstRef>,
               std::pair<std::string, GetBValue>>
        t = ConvertNameAccessorsPackToNestedTuple("a", GetAConstRef{},  //
                                                  "b", GetBValue{});
    EXPECT_EQ(std::get<0>(std::get<0>(t)), "a");
    EXPECT_EQ(std::get<0>(std::get<1>(t)), "b");
    EXPECT_EQ(std::get<1>(std::get<0>(t))(TestStruct{5, 3.5}), 5);
    EXPECT_EQ(std::get<1>(std::get<1>(t))(TestStruct{5, 3.5}), 3.5);
  }
  {  // convert from lambdas
    auto t = ConvertNameAccessorsPackToNestedTuple(
        "a", [](const TestStruct& s) { return s.a; },  //
        "b", [](const TestStruct& s) { return s.b; });
    EXPECT_EQ(std::get<0>(std::get<0>(t)), "a");
    EXPECT_EQ(std::get<0>(std::get<1>(t)), "b");
    EXPECT_EQ(std::get<1>(std::get<0>(t))(TestStruct{5, 3.5}), 5);
    EXPECT_EQ(std::get<1>(std::get<1>(t))(TestStruct{5, 3.5}), 3.5);
  }
  {  // convert from mix of lambdas and functors
    auto t = ConvertNameAccessorsPackToNestedTuple(
        "a", GetAConstRef{},  //
        "b", [](const TestStruct& s) { return s.b; });
    EXPECT_EQ(std::get<0>(std::get<0>(t)), "a");
    EXPECT_EQ(std::get<0>(std::get<1>(t)), "b");
    EXPECT_EQ(std::get<1>(std::get<0>(t))(TestStruct{5, 3.5}), 5);
    EXPECT_EQ(std::get<1>(std::get<1>(t))(TestStruct{5, 3.5}), 3.5);
  }
}

}  // namespace
}  // namespace arolla::accessor_helpers_impl
