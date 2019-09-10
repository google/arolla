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
#include "arolla/util/indestructible.h"

#include <string>
#include <type_traits>
#include <vector>

#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace arolla {
namespace {

TEST(Indestructible, Constructor) {
  struct T {
    explicit T(absl::string_view value) : value(value) {}
    std::string value;
  };
  constexpr auto kValue = "Hello!";
  static Indestructible<T> instance(kValue);
  EXPECT_EQ(instance->value, kValue);
}

TEST(Indestructible, InitializerListConstructor) {
  static Indestructible<std::vector<int>> instance({0});
  EXPECT_EQ(instance->at(0), 0);
}

TEST(Indestructible, NoDestructor) {
  struct T {
    ~T() = delete;
  };
  [[maybe_unused]] static Indestructible<T> instance;
}

TEST(Indestructible, PrivateConstructor) {
  class T {
   public:
    static T* instance() {
      static Indestructible<T> result([](void* self) { new (self) T; });
      return result.get();
    }

   private:
    T() = default;
  };
  EXPECT_TRUE(T::instance());
}

TEST(Indestructible, Interface) {
  struct T {
    int value;
  };
  constexpr int kValue = 1;
  static Indestructible<T> instance(T{kValue});
  EXPECT_EQ(instance.get()->value, kValue);
  EXPECT_EQ((*instance).value, kValue);
  EXPECT_EQ(instance->value, kValue);
  static_assert(std::is_same_v<T*, decltype(instance.get())>);
  static_assert(std::is_same_v<T&, decltype(*instance)>);
  static_assert(std::is_same_v<int*, decltype(&instance->value)>);
}

TEST(Indestructible, ConstInterface) {
  struct T {
    int value;
  };
  constexpr int kValue = 2;
  static const Indestructible<T> instance(T{kValue});
  EXPECT_EQ(instance.get()->value, kValue);
  EXPECT_EQ((*instance).value, kValue);
  EXPECT_EQ(instance->value, kValue);

  static_assert(std::is_same_v<const T*, decltype(instance.get())>);
  static_assert(std::is_same_v<const T&, decltype(*instance)>);
  static_assert(std::is_same_v<const int*, decltype(&instance->value)>);
}

}  // namespace
}  // namespace arolla
