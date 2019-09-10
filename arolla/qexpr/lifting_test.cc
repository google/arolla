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
#include "arolla/qexpr/lifting.h"

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>

#include "gtest/gtest.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

TEST(Lifting, DoNotLiftTag) {
  static_assert(std::is_same_v<int, DoNotLiftTag<int>::type>);
  static_assert(std::is_same_v<OptionalValue<int>,
                               DoNotLiftTag<OptionalValue<int>>::type>);
}

TEST(LiftingTools, LiftableArgs) {
  static_assert(
      std::is_same_v<LiftingTools<>::LiftableArgs, meta::type_list<>>);
  static_assert(
      std::is_same_v<LiftingTools<int>::LiftableArgs, meta::type_list<int>>);
  static_assert(std::is_same_v<LiftingTools<DoNotLiftTag<int>>::LiftableArgs,
                               meta::type_list<>>);
  static_assert(
      std::is_same_v<LiftingTools<int, DoNotLiftTag<float>>::LiftableArgs,
                     meta::type_list<int>>);
  static_assert(
      std::is_same_v<LiftingTools<DoNotLiftTag<float>, int>::LiftableArgs,
                     meta::type_list<int>>);
  static_assert(
      std::is_same_v<
          LiftingTools<std::string, DoNotLiftTag<float>, int>::LiftableArgs,
          meta::type_list<std::string, int>>);
  static_assert(
      std::is_same_v<LiftingTools<std::string, DoNotLiftTag<float>, int,
                                  DoNotLiftTag<char>, DoNotLiftTag<std::string>,
                                  double>::LiftableArgs,
                     meta::type_list<std::string, int, double>>);
}

template <class T>
struct MyView {
  using type = T;

  T value;
};

TEST(LiftingTools, CreateFnWithDontLiftCaptured) {
  {
    using Tools = LiftingTools<int>;
    auto fn = Tools::CreateFnWithDontLiftCaptured<MyView>(
        [](MyView<int> x) { return x.value; }, nullptr);
    EXPECT_EQ(fn(MyView<int>{5}), 5);
    EXPECT_EQ(Tools::CallOnLiftedArgs(fn, MyView<int>{5}), 5);
    static_assert(std::is_same_v<meta::function_traits<decltype(fn)>::arg_types,
                                 meta::type_list<MyView<int>>>);
    static_assert(
        std::is_same_v<meta::function_traits<decltype(fn)>::return_type, int>);
  }
  const int& kFive = 5;  // captured arguments must be alive.
  {
    using Tools = LiftingTools<DoNotLiftTag<int>>;
    auto fn = Tools::CreateFnWithDontLiftCaptured<MyView>(
        [](int x) { return x; }, kFive);
    EXPECT_EQ(fn(), 5);
    EXPECT_EQ(Tools::CallOnLiftedArgs(fn, nullptr), 5);
    static_assert(std::is_same_v<meta::function_traits<decltype(fn)>::arg_types,
                                 meta::type_list<>>);
    static_assert(
        std::is_same_v<meta::function_traits<decltype(fn)>::return_type, int>);
  }
  {
    using Tools =
        LiftingTools<DoNotLiftTag<int>, float, DoNotLiftTag<std::string>>;
    auto lambda = [](int x, MyView<float> y, std::string z) {
      if (x != 5 || y.value != 2.0f || z != "a") {
        return 0;
      }
      return 1;
    };
    auto fn = Tools::CreateFnWithDontLiftCaptured<MyView>(lambda, kFive,
                                                          nullptr, "a");
    EXPECT_EQ(fn(MyView<float>{2.0f}), 1);
    EXPECT_EQ(
        Tools::CallOnLiftedArgs(fn, nullptr, MyView<float>{2.0f}, nullptr), 1);
    static_assert(std::is_same_v<meta::function_traits<decltype(fn)>::arg_types,
                                 meta::type_list<MyView<float>>>);
    static_assert(
        std::is_same_v<meta::function_traits<decltype(fn)>::return_type, int>);
  }
  {
    using Tools =
        LiftingTools<char, DoNotLiftTag<int>, float, DoNotLiftTag<std::string>>;
    auto lambda = [](MyView<char> q, int x, MyView<float> y, std::string z) {
      if (q.value != 'Q' || x != 5 || y.value != 2.0f || z != "a") {
        return 0;
      }
      return 1;
    };
    std::string kA = "a";  // captured arguments must be alive.
    auto fn = Tools::CreateFnWithDontLiftCaptured<MyView>(lambda, nullptr,
                                                          kFive, nullptr, kA);
    EXPECT_EQ(fn(MyView<char>{'Q'}, MyView<float>{2.0f}), 1);
    EXPECT_EQ(Tools::CallOnLiftedArgs(fn, MyView<char>{'Q'}, nullptr,
                                      MyView<float>{2.0f}, nullptr),
              1);
    static_assert(std::is_same_v<meta::function_traits<decltype(fn)>::arg_types,
                                 meta::type_list<MyView<char>, MyView<float>>>);
    static_assert(
        std::is_same_v<meta::function_traits<decltype(fn)>::return_type, int>);
  }
}

TEST(LiftingTools, CallOnLiftedArgsWithADifferentFunction) {
  using Tools =
      LiftingTools<char, DoNotLiftTag<int>, float, DoNotLiftTag<std::string>>;
  auto fn = [](float x, std::string z) {
    if (x != 1.0f || z != "z") {
      return 0;
    }
    return 1;
  };
  EXPECT_EQ(Tools::CallOnLiftedArgs(fn, 1.0f, nullptr, "z", nullptr), 1);
}

TEST(LiftingTools, CaptureNonCopiable) {
  using Tools = LiftingTools<DoNotLiftTag<std::unique_ptr<int>>>;
  const auto ptr = std::make_unique<int>(5);
  auto fn = Tools::CreateFnWithDontLiftCaptured<MyView>(
      [](const std::unique_ptr<int>& x) { return x == nullptr ? -1 : *x; },
      ptr);
  EXPECT_EQ(fn(), 5);
  EXPECT_EQ(Tools::CallOnLiftedArgs(fn, MyView<int>{-7}), 5);
  static_assert(std::is_same_v<meta::function_traits<decltype(fn)>::arg_types,
                               meta::type_list<>>);
  static_assert(
      std::is_same_v<meta::function_traits<decltype(fn)>::return_type, int>);
}

template <class T>
using ConstRef = const T&;

TEST(LiftingTools, CallNonCopiable) {
  using Tools = LiftingTools<std::unique_ptr<int>>;
  auto fn = Tools::CreateFnWithDontLiftCaptured<ConstRef>(
      [](const std::unique_ptr<int>& x) { return x == nullptr ? -1 : *x; },
      MyView<int>{-13});
  EXPECT_EQ(fn(std::make_unique<int>(5)), 5);
  EXPECT_EQ(Tools::CallOnLiftedArgs(fn, std::make_unique<int>(5)), 5);
  static_assert(std::is_same_v<meta::function_traits<decltype(fn)>::arg_types,
                               meta::type_list<const std::unique_ptr<int>&>>);
  static_assert(
      std::is_same_v<meta::function_traits<decltype(fn)>::return_type, int>);
}

TEST(LiftingTools, CreateFnWithDontLiftCaptured64Args) {
  using Tools = LiftingTools<
      DoNotLiftTag<int>, int, int, int, DoNotLiftTag<int>, int, int, int,  // 8
      DoNotLiftTag<int>, DoNotLiftTag<int>, int, int, int, int, int, int,  // 16
      DoNotLiftTag<int>, int, int, int, DoNotLiftTag<int>, int, int, int,  // 24
      int, DoNotLiftTag<int>, int, int, int, int, DoNotLiftTag<int>, int,  // 32
      int, int, int, DoNotLiftTag<int>, int, DoNotLiftTag<int>, int, int,  // 40
      int, int, int, DoNotLiftTag<int>, DoNotLiftTag<int>, int, int, int,  // 48
      int, DoNotLiftTag<int>, DoNotLiftTag<int>, int, int, int, int, int,  // 56
      int, DoNotLiftTag<int>, int, int, int, int, int, DoNotLiftTag<int>   // 64
      >;
  const int x = -1;
#define TEST_ARGS_8 x, x, x, x, x, x, x, x
#define TEST_ARGS_32 TEST_ARGS_8, TEST_ARGS_8, TEST_ARGS_8, TEST_ARGS_8
#define TEST_ARGS_64 TEST_ARGS_32, TEST_ARGS_32
  auto fn = Tools::CreateFnWithDontLiftCaptured<ConstRef>(
      [](auto... args) { return sizeof...(args); }, TEST_ARGS_64);
  EXPECT_EQ(Tools::CallOnLiftedArgs(fn, TEST_ARGS_64), 64);
  static_assert(
      std::is_same_v<meta::function_traits<decltype(fn)>::return_type, size_t>);
#undef TEST_ARGS_8
#undef TEST_ARGS_32
#undef TEST_ARGS_64
}

}  // namespace
}  // namespace arolla
