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
#include "arolla/io/testing/matchers.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/io/accessors_input_loader.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla::testing {
namespace {

using ::testing::DescribeMatcher;
using ::testing::Eq;
using ::testing::Not;
using ::testing::StringMatchResultListener;

struct TestStruct {
  int a;
  double b;
};

template <typename MatcherType, typename Value>
std::string Explain(const MatcherType& m, const Value& x) {
  StringMatchResultListener listener;
  ExplainMatchResult(m, x, &listener);
  return listener.str();
}

TEST(MatchersTest, InputLoaderSupports) {
  ASSERT_OK_AND_ASSIGN(auto input_loader,
                       CreateAccessorsInputLoader<TestStruct>(
                           "a", [](const TestStruct& s) { return s.a; },  //
                           "b", [](const TestStruct& s) { return s.b; }));

  EXPECT_THAT(input_loader, InputLoaderSupports({{"a", GetQType<int>()}}));
  EXPECT_THAT(input_loader, InputLoaderSupports({{"b", GetQType<double>()}}));

  auto a_b_matcher =
      InputLoaderSupports({{"a", GetQType<int>()}, {"b", GetQType<double>()}});
  auto a_c_matcher =
      InputLoaderSupports({{"a", GetQType<int>()}, {"c", GetQType<double>()}});
  EXPECT_THAT(input_loader, a_b_matcher);
  EXPECT_THAT(input_loader, Not(a_c_matcher));

  EXPECT_THAT(DescribeMatcher<InputLoader<TestStruct>>(a_b_matcher),
              Eq("can load all of a: INT32, b: FLOAT64"));
  EXPECT_THAT(DescribeMatcher<InputLoader<TestStruct>>(a_b_matcher,
                                                       /*negation=*/true),
              Eq("cannot load any of a: INT32, b: FLOAT64"));

  EXPECT_THAT(Explain(a_c_matcher, input_loader),
              Eq("does not support input \"c\" (supported: a, b)"));
  EXPECT_THAT(Explain(Not(a_b_matcher), input_loader),
              Eq("supports all the requested inputs"));

  EXPECT_THAT(
      Explain(InputLoaderSupports({{"a", GetQType<double>()}}), input_loader),
      Eq("unexpected type for \"a\": expected FLOAT64, got INT32"));
}

TEST(MatchersTest, SlotListenerSupports) {
  ASSERT_OK_AND_ASSIGN(auto slot_listener,
                       CreateAccessorsSlotListener<TestStruct>(
                           "a", [](int, TestStruct* s) {},  //
                           "b", [](double, TestStruct* s) {}));

  EXPECT_THAT(slot_listener, SlotListenerSupports({{"a", GetQType<int>()}}));
  EXPECT_THAT(slot_listener, SlotListenerSupports({{"b", GetQType<double>()}}));

  auto a_b_matcher =
      SlotListenerSupports({{"a", GetQType<int>()}, {"b", GetQType<double>()}});
  auto a_c_matcher =
      SlotListenerSupports({{"a", GetQType<int>()}, {"c", GetQType<double>()}});
  EXPECT_THAT(slot_listener, a_b_matcher);
  EXPECT_THAT(slot_listener, Not(a_c_matcher));

  EXPECT_THAT(DescribeMatcher<SlotListener<TestStruct>>(a_b_matcher),
              Eq("can load all of a: INT32, b: FLOAT64"));
  EXPECT_THAT(DescribeMatcher<SlotListener<TestStruct>>(a_b_matcher,
                                                        /*negation=*/true),
              Eq("cannot load any of a: INT32, b: FLOAT64"));

  EXPECT_THAT(Explain(a_c_matcher, slot_listener),
              Eq("does not support input \"c\" (supported: a, b)"));
  EXPECT_THAT(Explain(Not(a_b_matcher), slot_listener),
              Eq("supports all the requested inputs"));

  EXPECT_THAT(
      Explain(SlotListenerSupports({{"a", GetQType<double>()}}), slot_listener),
      Eq("unexpected type for \"a\": expected FLOAT64, got INT32"));
}

}  // namespace
}  // namespace arolla::testing
