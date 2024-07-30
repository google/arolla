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
#ifndef AROLLA_UTIL_INIT_AROLLA_H_
#define AROLLA_UTIL_INIT_AROLLA_H_

#include <initializer_list>
#include <variant>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/string.h"

namespace arolla {

// The function executes all registered initializers. The order of the execution
// is determined by the initializer dependencies.
//
// This helps to address the Static Initialization Order Fiasco in C++
// (https://en.cppreference.com/w/cpp/language/siof) where the order of
// initialization across translation units is unspecified.
//
// To register a function as an initializer, please use the macro:
// AROLLA_INITIALIZER(...).
//
// Note: Calling this function while concurrently loading additional shared
// libraries is unsafe and may lead to undefined behavior.
//
void InitArolla();

// Checks whether InitArolla() has been called. It prints an error message and
// crashes the process if it has not.
void CheckInitArolla();

namespace initializer_dep {

// Common phony dependencies for initializers.
//
// Use as dependencies when consuming, reverse dependencies when registering:
//  * qtypes:
constexpr absl::string_view kQTypes = "@phony/qtypes";
//  * serialisation codecs
constexpr absl::string_view kS11n = "@phony/s11n";
//  * operators (both expr and qexpr)
constexpr absl::string_view kOperators = "@phony/operators";
//  * qexpr operators (when used as a reverse dependency, should be paired with
//    "@phony/operators" )
constexpr absl::string_view kQExprOperators = "@phony/operators:qexpr";

}  // namespace initializer_dep
}  // namespace arolla

// Registers an initialization function to be called by InitArolla().
//
// Example:
//
//   static absl::Status RegisterExprBootstrapOperators() { ... }
//
//   AROLLA_INITIALIZER(.name = "arolla_operators/my_operators",
//                      .deps = {"arolla_operators/standard"},
//                      .reverse_deps = {arolla::initializer_dep::kOperators},
//                      .init_fn = &RegisterMyOperators);
//
//   Here,
//
//     .deps = {"arolla_operators/standard"}
//
//   indicates that `RegisterMyOperators()` will be called after
//   the "arolla_operators/standard" initializer (i.e. when all standard
//   operators are already available). And
//
//     .reverse_deps = {arolla::initializer_dep::kOperators}
//
//   ensures that any initializer depending on "@phony/operators" (that hasn't
//   run yet) will run after "arolla_operators/my_operators".
//
// Supported parameters:
//   .name: string_view
//       A globally unique name (can be left unspecified for anonymous
//       initializers).
//   .deps: initializer_list<string_view>
//       A list of initializer names that must be executed before this one.
//   .reverse_deps: initializer_list<string_view>
//       A list of initializer names that must be executed after this one.
//       Note: If a late-registered initializer mentions a non-phony reverse
//             dependency that has already been executed, it's an error.
//   .init_fn: void(*)() | absl::Status(*)()
//       A function with the initializer action.
//
//   All parameters are optional.
//
// Phony dependencies:
//   Phony dependencies, identifiable by the name prefix '@phony', function
//   exclusively as ordering constraints and are neither executed nor marked as
//   complete.
//
//   They are designed as a mechanism to provide a common name for groups of
//   similar initialization tasks that can be added dynamically at runtime.
//   For example, this may occur when a shared library, providing new types,
//   operators, and optimization rules, is loaded with dlopen().
//
//   Consider a scenario involving two initializers:
//
//     AROLLA_INITIALIZER(.name = "X", .reverse_deps="@phony/name", ...)
//     AROLLA_INITIALIZER(.name = "Y", .deps="@phony/name", ...)
//
//   If "X" and "Y" are loaded simultaneously, "X" will execute before "Y".
//   However, if "X" is dynamically loaded after "Y" has already been executed,
//   "X" will still execute seamlessly.
//
#define AROLLA_INITIALIZER(...)                                                \
  extern "C" {                                                                 \
  __attribute__((constructor(65534))) static void                              \
  AROLLA_INITIALIZER_IMPL_CONCAT(arolla_initializer_register, __COUNTER__)() { \
    static constexpr ::arolla::init_arolla_internal::Initializer initializer = \
        {__VA_ARGS__};                                                         \
    static_assert(!::arolla::starts_with(                                      \
                      initializer.name,                                        \
                      ::arolla::init_arolla_internal::kPhonyNamePrefix),       \
                  "an initializer name may not start with `@phony` prefix");   \
    [[maybe_unused]] static const ::arolla::init_arolla_internal::Registration \
        registration(initializer);                                             \
  }                                                                            \
  __attribute__((constructor(65535))) static void                              \
  AROLLA_INITIALIZER_IMPL_CONCAT(arolla_initializer_secondary_run,             \
                                 __COUNTER__)() {                              \
    ::arolla::init_arolla_internal::InitArollaSecondary();                     \
  }                                                                            \
  } /* extern "C" */

#define AROLLA_INITIALIZER_IMPL_CONCAT_INNER(x, y) x##y
#define AROLLA_INITIALIZER_IMPL_CONCAT(x, y) \
  AROLLA_INITIALIZER_IMPL_CONCAT_INNER(x, y)

namespace arolla::init_arolla_internal {

// The name prefix for phony dependencies.
constexpr absl::string_view kPhonyNamePrefix = "@phony";

// A structure describing an initializer.
//
// Importantly, this structure supports:
//  * `constexpr` construction, i.e. can be created at compile-time
//  * aggregate initialization (e.g., `{.name = "name", .init_fn = &fn}`)
struct Initializer {
  using VoidInitFn = void (*)();
  using StatusInitFn = absl::Status (*)();

  // The name of the initializer.
  absl::string_view name;

  // A list of dependencies required by this initializer.
  std::initializer_list<absl::string_view> deps;

  // A list of initializers that depend on this initializer.
  std::initializer_list<absl::string_view> reverse_deps;

  // The initialization function.
  std::variant<std::monostate, VoidInitFn, StatusInitFn> init_fn;
};

// A structure that linkes an initializer into the InitArolla() list.
struct Registration {
  const Initializer& initializer;
  const Registration* next;

  explicit Registration(const Initializer& initializer);
};

// Triggers execution of the newly registered initializers,
// but only if `InitArolla()` has already been executed.
//
// Note: This function is used by the AROLLA_INITIALIZER macro.
// Client code should not call this function directly.
void InitArollaSecondary();

}  // namespace arolla::init_arolla_internal

#endif  // AROLLA_UTIL_INIT_AROLLA_H_
