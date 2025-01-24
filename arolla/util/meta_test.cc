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
#include "arolla/util/meta.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//status/statusor.h"

namespace arolla::meta {
namespace {

using ::testing::ElementsAre;

TEST(MetaTest, TypeList) {
  static_assert(
      std::is_same_v<head_t<type_list<int64_t, int32_t, int8_t>>, int64_t>);
  static_assert(std::is_same_v<tail_t<type_list<int64_t, int32_t, int8_t>>,
                               type_list<int32_t, int8_t>>);
  static_assert(contains_v<type_list<int64_t, int32_t, int8_t>, int32_t>);
  static_assert(
      contains_v<type_list<int64_t, int32_t, int32_t, int8_t>, int32_t>);
  static_assert(!contains_v<type_list<int64_t, int32_t, int8_t>, float>);
  static_assert(!contains_v<type_list<>, float>);
}

TEST(MetaTest, ForeachType) {
  using meta_int_list =
      type_list<std::integral_constant<int, 1>, std::integral_constant<int, 2>,
                std::integral_constant<int, 4>, std::integral_constant<int, 8>>;
  int value = 0;
  foreach_type<meta_int_list>(
      [&value](auto meta_type) { value ^= decltype(meta_type)::type::value; });
  EXPECT_EQ(value, 15);
}

TEST(MetaTest, FunctionTraits) {
  int64_t someValue = 57;
  auto lambda = [&someValue](int32_t, double) {
    return static_cast<float>(someValue);
  };
  using lambda_traits = function_traits<decltype(lambda)>;

  static_assert(lambda_traits::arity == 2);
  static_assert(std::is_same<typename lambda_traits::arg_types,
                             type_list<int32_t, double>>::value);
  static_assert(
      std::is_same<typename lambda_traits::return_type, float>::value);

  struct ConstFunctor {
    float operator()(int32_t, double) const { return 57; }
  };
  using const_functor_traits = function_traits<ConstFunctor>;

  static_assert(const_functor_traits::arity == 2);
  static_assert(std::is_same<typename const_functor_traits::arg_types,
                             type_list<int32_t, double>>::value);
  static_assert(
      std::is_same<typename const_functor_traits::return_type, float>::value);

  struct NonConstFunctor {
    float operator()(int32_t, double) { return 57; }
  };
  using non_const_functor_traits = function_traits<NonConstFunctor>;

  static_assert(non_const_functor_traits::arity == 2);
  static_assert(std::is_same<typename non_const_functor_traits::arg_types,
                             type_list<int32_t, double>>::value);
  static_assert(std::is_same<typename non_const_functor_traits::return_type,
                             float>::value);

  using ref_traits = function_traits<std::reference_wrapper<ConstFunctor>>;
  static_assert(std::is_same<typename ref_traits::return_type, float>::value);
}

TEST(MetaTest, is_wrapped_with) {
  static_assert(is_wrapped_with<std::optional, std::optional<float>>::value);
  static_assert(
      is_wrapped_with<::absl::StatusOr, ::absl::StatusOr<float>>::value);
}

TEST(MetaTest, concat) {
  static_assert(
      std::is_same_v<concat_t<type_list<>, type_list<>>, type_list<>>);
  static_assert(
      std::is_same_v<concat_t<type_list<>, type_list<void>>, type_list<void>>);
  static_assert(
      std::is_same_v<concat_t<type_list<int>, type_list<>>, type_list<int>>);
  static_assert(std::is_same_v<concat_t<type_list<int>, type_list<void>>,
                               type_list<int, void>>);
  static_assert(std::is_same_v<
                concat_t<type_list<int, char, bool>, type_list<void, char>>,
                type_list<int, char, bool, void, char>>);
}

TEST(MetaTest, foreach_tuple_element) {
  {
    auto tuple = std::tuple();
    meta::foreach_tuple_element(tuple, [](auto&) { FAIL(); });
  }
  {
    std::vector<const void*> result;
    auto tuple = std::tuple(1, 2.5, "foo");
    meta::foreach_tuple_element(tuple, [&](auto& x) { result.push_back(&x); });
    EXPECT_THAT(result, ElementsAre(&std::get<0>(tuple), &std::get<1>(tuple),
                                    &std::get<2>(tuple)));
  }
}

TEST(MetaTest, foreach_tuple_element_type) {
  {
    using Tuple = std::tuple<>;
    meta::foreach_tuple_element_type<Tuple>([](auto) { FAIL(); });
  }
  {
    using Tuple = std::tuple<int, float, const char*>;
    std::vector<std::type_index> result;
    meta::foreach_tuple_element_type<Tuple>([&](auto meta_type) {
      result.push_back(typeid(typename decltype(meta_type)::type));
    });
    EXPECT_THAT(result, ElementsAre(std::type_index(typeid(int)),
                                    std::type_index(typeid(float)),
                                    std::type_index(typeid(const char*))));
  }
}

TEST(MetaTest, is_transparent) {
  EXPECT_TRUE(is_transparent_v<std::equal_to<>>);
  EXPECT_FALSE(is_transparent_v<std::equal_to<int>>);
}

}  // namespace
}  // namespace arolla::meta
