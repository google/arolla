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
#include "arolla/codegen/qtype_utils.h"

#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla::codegen {
namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Pair;

TEST(NamedQTypeVectorBuilderTest, NamedQTypeVectorBuilder) {
  {
    SCOPED_TRACE("Empty builder");
    NamedQTypeVectorBuilder builder;
    EXPECT_THAT(std::move(builder).Build(), IsEmpty());
  }
  {
    SCOPED_TRACE("Single element");
    NamedQTypeVectorBuilder builder;
    builder.AddFromCommonPrefixWithPrevious(3, "foo", GetQType<int32_t>());
    EXPECT_THAT(std::move(builder).Build(),
                ElementsAre(Pair("foo", GetQType<int32_t>())));
  }
  {
    SCOPED_TRACE("Many elements no prefix");
    NamedQTypeVectorBuilder builder;
    builder.AddFromCommonPrefixWithPrevious(3, "abc", GetQType<int32_t>());
    builder.AddFromCommonPrefixWithPrevious(4, "defx", GetQType<double>());
    builder.AddFromCommonPrefixWithPrevious(2, "gh",
                                            GetQType<OptionalValue<float>>());
    EXPECT_THAT(std::move(builder).Build(),
                ElementsAre(Pair("abc", GetQType<int32_t>()),
                            Pair("defx", GetQType<double>()),
                            Pair("gh", GetQType<OptionalValue<float>>())));
  }
  {
    SCOPED_TRACE("Many elements common prefix");
    NamedQTypeVectorBuilder builder;
    builder.AddFromCommonPrefixWithPrevious(3, "abc", GetQType<int32_t>());
    builder.AddFromCommonPrefixWithPrevious(4, "de", GetQType<double>());
    builder.AddFromCommonPrefixWithPrevious(5, "gh",
                                            GetQType<OptionalValue<float>>());
    EXPECT_THAT(std::move(builder).Build(),
                ElementsAre(Pair("abc", GetQType<int32_t>()),
                            Pair("abde", GetQType<double>()),
                            Pair("abdgh", GetQType<OptionalValue<float>>())));
  }
}

}  // namespace
}  // namespace arolla::codegen
