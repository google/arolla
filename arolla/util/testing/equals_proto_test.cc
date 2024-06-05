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
#include "arolla/util/testing/equals_proto.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/util/testing/test.pb.h"

namespace arolla::testing {
namespace {

using ::testing::Not;

TEST(EqualsProtoTest, Predicate) {
  {
    TestProto test_proto;
    EXPECT_TRUE(EqualsProto(test_proto, R"pb()pb"));
    EXPECT_FALSE(EqualsProto(test_proto, R"pb(field: 0)pb"));
    EXPECT_FALSE(EqualsProto(test_proto, R"pb(field: 100)pb"));
  }
  {
    TestProto test_proto;
    test_proto.set_field(0);
    EXPECT_FALSE(EqualsProto(test_proto, R"pb()pb"));
    EXPECT_TRUE(EqualsProto(test_proto, R"pb(field: 0)pb"));
    EXPECT_FALSE(EqualsProto(test_proto, R"pb(field: 100)pb"));
  }
  {
    TestProto test_proto;
    test_proto.set_field(100);
    EXPECT_FALSE(EqualsProto(test_proto, R"pb()pb"));
    EXPECT_FALSE(EqualsProto(test_proto, R"pb(field: 0)pb"));
    EXPECT_TRUE(EqualsProto(test_proto, R"pb(field: 100)pb"));
  }
  {
    TestProto test_proto;
    EXPECT_FALSE(EqualsProto(test_proto, R"pb(unknown_field: 0)pb"));
  }
  {
    TestProto test_proto;
    EXPECT_FALSE(EqualsProto(test_proto, "invalid text proto literal"));
  }
}

TEST(EqualsProtoTest, Matcher) {
  {
    TestProto test_proto;
    EXPECT_THAT(test_proto, EqualsProto(R"pb()pb"));
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb(field: 0)pb")));
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb(field: 100)pb")));
  }
  {
    TestProto test_proto;
    test_proto.set_field(0);
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb()pb")));
    EXPECT_THAT(test_proto, EqualsProto(R"pb(field: 0)pb"));
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb(field: 100)pb")));
  }
  {
    TestProto test_proto;
    test_proto.set_field(100);
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb()pb")));
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb(field: 0)pb")));
    EXPECT_THAT(test_proto, EqualsProto(R"pb(field: 100)pb"));
  }
  {
    TestProto test_proto;
    EXPECT_THAT(test_proto, Not(EqualsProto(R"pb(unknown_field: 0)pb")));
  }
  {
    TestProto test_proto;
    EXPECT_THAT(test_proto, Not(EqualsProto("invalid text proto literal")));
  }
}

}  // namespace
}  // namespace arolla::testing
