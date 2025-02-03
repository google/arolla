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
#include "arolla/util/init_arolla_internal.h"

#include <algorithm>
#include <array>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/init_arolla.h"

namespace arolla::init_arolla_internal {
namespace {

using ::absl_testing::StatusIs;

// Most of the tests will use the following graph:
//
//   A ───┐ ┌───► D
//         C
//   B ───┘ └───► E
//

TEST(InitArollaInternalTest, Dependencies) {
  static absl::NoDestructor<std::string> result;
  Initializer a{.name = "A", .deps = {"C"}, .init_fn = [] { *result += "A"; }};
  Initializer b{.name = "B", .deps = {"C"}, .init_fn = [] { *result += "B"; }};
  Initializer c{
      .name = "C", .deps = {"D", "E"}, .init_fn = [] { *result += "C"; }};
  Initializer d{.name = "D", .init_fn = [] { *result += "D"; }};
  Initializer e{.name = "E", .init_fn = [] { *result += "E"; }};
  std::array initializers = {&a, &b, &c, &d, &e};
  std::sort(initializers.begin(), initializers.end());
  do {
    result->clear();
    Coordinator coordinator;
    EXPECT_OK(coordinator.Run(initializers));
    EXPECT_EQ(*result, "DECAB");
  } while (std::next_permutation(initializers.begin(), initializers.end()));
}

TEST(InitArollaInternalTest, ReverseDependencies) {
  static absl::NoDestructor<std::string> result;
  Initializer a{.name = "A", .init_fn = [] { *result += "A"; }};
  Initializer b{.name = "B", .init_fn = [] { *result += "B"; }};
  Initializer c{.name = "C", .reverse_deps = {"A", "B"}, .init_fn = [] {
                  *result += "C";
                }};
  Initializer d{
      .name = "D", .reverse_deps = {"C"}, .init_fn = [] { *result += "D"; }};
  Initializer e{
      .name = "E", .reverse_deps = {"C"}, .init_fn = [] { *result += "E"; }};
  std::array initializers = {&a, &b, &c, &d, &e};
  std::sort(initializers.begin(), initializers.end());
  do {
    result->clear();
    Coordinator coordinator;
    EXPECT_OK(coordinator.Run(initializers));
    EXPECT_EQ(*result, "DECAB");
  } while (std::next_permutation(initializers.begin(), initializers.end()));
}

TEST(InitArollaInternalTest, MixedDependencies) {
  static absl::NoDestructor<std::string> result;
  Initializer a{.name = "A", .init_fn = [] { *result += "A"; }};
  Initializer b{.name = "B", .init_fn = [] { *result += "B"; }};
  Initializer c{.name = "C",
                .deps = {"D", "E"},
                .reverse_deps = {"A", "B"},
                .init_fn = [] { *result += "C"; }};
  Initializer d{.name = "D", .init_fn = [] { *result += "D"; }};
  Initializer e{.name = "E", .init_fn = [] { *result += "E"; }};
  std::array initializers = {&a, &b, &c, &d, &e};
  std::sort(initializers.begin(), initializers.end());
  do {
    result->clear();
    Coordinator coordinator;
    EXPECT_OK(coordinator.Run(initializers));
    EXPECT_EQ(*result, "DECAB");
  } while (std::next_permutation(initializers.begin(), initializers.end()));
}

TEST(InitArollaInternalTest, TwoPhases) {
  static absl::NoDestructor<std::string> result;
  Initializer a{.name = "A", .deps = {"C"}, .init_fn = [] { *result += "A"; }};
  Initializer b{.name = "B", .deps = {"C"}, .init_fn = [] { *result += "B"; }};
  Initializer c{
      .name = "C", .deps = {"D", "E"}, .init_fn = [] { *result += "C"; }};
  Initializer d{.name = "D", .init_fn = [] { *result += "D"; }};
  Initializer e{.name = "E", .init_fn = [] { *result += "E"; }};
  Coordinator coordinator;
  EXPECT_OK(coordinator.Run({&c, &d, &e}));
  EXPECT_EQ(*result, "DEC");
  EXPECT_OK(coordinator.Run({&a, &b}));
  EXPECT_EQ(*result, "DECAB");
}

TEST(InitArollaInternalTest, AnonymousInitializers) {
  static absl::NoDestructor<std::string> result;
  Initializer x{.name = "X", .init_fn = [] { *result += "X"; }};
  Initializer y{.name = "Y", .init_fn = [] { *result += "Y"; }};
  Initializer a0{
      .deps = {"Y"}, .reverse_deps = {"X"}, .init_fn = [] { *result += "0"; }};
  Initializer a1{
      .deps = {"Y"}, .reverse_deps = {"X"}, .init_fn = [] { *result += "1"; }};
  Initializer a2{
      .deps = {"Y"}, .reverse_deps = {"X"}, .init_fn = [] { *result += "2"; }};
  Coordinator coordinator;
  EXPECT_OK(coordinator.Run({&x, &y, &a0, &a1, &a2}));
  EXPECT_EQ(*result, "Y012X");  // stable_sort behaviour expected
}

TEST(InitArollaInternalTest, PhonyInitializers) {
  static absl::NoDestructor<std::string> result;
  Initializer x{
      .name = "X", .deps = {"@phony/name"}, .init_fn = [] { *result += "X"; }};
  Initializer y{.name = "Y", .reverse_deps = {"@phony/name"}, .init_fn = [] {
                  *result += "Y";
                }};
  Initializer a{
      .name = "A", .deps = {"@phony/name"}, .init_fn = [] { *result += "A"; }};
  Initializer b{.name = "B", .reverse_deps = {"@phony/name"}, .init_fn = [] {
                  *result += "B";
                }};
  Coordinator coordinator;
  EXPECT_OK(coordinator.Run({&x, &y}));
  EXPECT_EQ(*result, "YX");
  EXPECT_OK(coordinator.Run({&a, &b}));
  EXPECT_EQ(*result, "YXBA");
}

TEST(InitArollaInternalTest, DanglingReverseDependency) {
  static absl::NoDestructor<std::string> result;
  Initializer x{.name = "X", .reverse_deps = {"undefined_dep"}, .init_fn = [] {
                  *result += "X";
                }};
  Coordinator coordinator;
  EXPECT_OK(coordinator.Run({&x}));
  EXPECT_EQ(*result, "X");
}

TEST(InitArollaInternalTest, InitFn) {
  static absl::NoDestructor<std::string> result;
  Initializer x{
      .name = "X",
      .init_fn = [] { *result += "X"; },
  };
  Initializer y{
      .name = "Y",
      .init_fn =
          [] {
            *result += "Y";
            return absl::OkStatus();
          },
  };
  Coordinator coordinator;
  EXPECT_OK(coordinator.Run({&x, &y}));
  EXPECT_EQ(*result, "XY");  // stable_sort behaviour expected
}

TEST(InitArollaInternalTest, Error_NameCollision) {
  Initializer a1{.name = "A"};
  Initializer a2{.name = "A"};
  {
    Coordinator coordinator;
    EXPECT_THAT(coordinator.Run({&a1, &a2}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         "name collision between arolla initializers: 'A'"));
  }
  {
    Coordinator coordinator;
    EXPECT_OK(coordinator.Run({&a1}));
    EXPECT_THAT(coordinator.Run({&a2}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         "name collision between arolla initializers: 'A'"));
  }
}

TEST(InitArollaInternalTest, Error_PhonyName) {
  Initializer phony{.name = "@phony/name"};
  Coordinator coordinator;
  EXPECT_THAT(coordinator.Run({&phony}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "an initializer name may not start with `@phony` "
                       "prefix: '@phony/name'"));
}

TEST(InitArollaInternalTest, Error_LateReverseDependency) {
  Initializer x{.name = "X"};
  Initializer y{.name = "Y", .reverse_deps = {"X"}};
  Coordinator coordinator;
  EXPECT_OK(coordinator.Run({&x}));
  EXPECT_THAT(
      coordinator.Run({&y}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "the newly registered initializer 'Y' expects to be executed "
               "before the previously registered and executed initializer 'X'. "
               "This is likely due to a missing linkage dependency between the "
               "library providing 'Y' and the library providing 'X'"));
}

TEST(InitArollaInternalTest, Error_UndefinedDependency) {
  Initializer x{.name = "X", .deps = {"Y"}};
  Coordinator coordinator;
  EXPECT_THAT(
      coordinator.Run({&x}),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "the initializer 'X' expects to be executed after the "
               "initializer 'Y', which has not been defined yet. This is "
               "likely due to a missing linkage dependency between the library "
               "providing 'X' and the library providing 'Y'"));
}

TEST(InitArollaInternalTest, Error_CircularDependency) {
  Initializer a{.name = "A", .reverse_deps = {"X"}};
  Initializer x{.name = "X", .reverse_deps = {"Y"}};
  Initializer y{.name = "Y", .reverse_deps = {"Z"}};
  Initializer z{.name = "Z", .reverse_deps = {"X"}};
  Coordinator coordinator;
  EXPECT_THAT(coordinator.Run({&a, &x, &y, &z}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "a circular dependency between initializers: 'X' <- 'Y' "
                       "<- 'Z' <- 'X'"));
}

TEST(InitArollaInternalTest, Error_InitFnFails) {
  Initializer x{.name = "X",
                .init_fn = [] { return absl::InvalidArgumentError("error"); }};
  Coordinator coordinator;
  EXPECT_THAT(coordinator.Run({&x}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "error; while executing initializer 'X'"));
}

}  // namespace
}  // namespace arolla::init_arolla_internal
