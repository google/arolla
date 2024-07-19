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

}  // namespace arolla

// Registers an initialization function to be called by InitArolla().
//
// Example:
//
//   static absl::Status RegisterExprBootstrapOperators() { ... }
//
//   AROLLA_INITIALIZER(.name = "//arolla/expr/operators:bootstrap",
//                      .deps = "//arolla/qexpr/operators/bootstrap",
//                      .reverse_deps = "@phony:expr_operators",
//                      .init_fn = &RegisterExprOperatorsBootstrap);
//
//   Here,
//
//     .deps = "//arolla/qexpr/operators/bootstrap"
//
//   indicates that `RegisterExprOperatorsBootstrap()` will be invoked after the
//   "//arolla/qexpr/operators/bootstrap" initializer. And
//
//     .reverse_deps = "@phony:expr_operators"
//
//   ensures that any initializer depending on "@phony:expr_operators" (that
//   hasn't run yet) will run after "//arolla/qexpr/operators/bootstrap".
//
// Supported parameters:
//   .name: (constexpr string) A globally unique name (can be left unspecified
//       for anonymous initializers).
//   .deps: (constexpr string) A comma or space-separated list of initializer
//       names that must be executed before this one.
//   .reverse_deps: (constexpr string) A comma or space-separated list of
//       initializer names that should be executed after this one.
//       Note: If a late-registered initializer mentions a reverse dependency
//       that has already been executed, it's an error.
//   .init_fn: (callable) A function with signature `void (*)()` or
//       `absl::Status (*)()`.
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
//     AROLLA_INITIALIZER(.name = "X", .reverse_deps="@phony:name", ...)
//     AROLLA_INITIALIZER(.name = "Y", .deps="@phony:name", ...)
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

  // A comma-separated list of dependencies required by the initializer.
  absl::string_view deps;

  // A comma-separated list of initializers that depend on this initializer.
  absl::string_view reverse_deps;

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
// Note: This function is used by the AROLLA_INITIALIZER macro. Client code
// should not call this function directly.
void InitArollaSecondary();

}  // namespace arolla::init_arolla_internal

//
// DEPRECATED
//
// For migration purposes, we replicate the priority-based API on top of
// the API based on dependencies. It will be removed after the migration period.
// Please avoid using it.
//

// Registers an initialization function to be call by InitArolla().
#define AROLLA_REGISTER_INITIALIZER(priority, name_, /*init_fn*/...) \
  AROLLA_INITIALIZER(.name = (#name_), .deps = (#priority "Begin"),  \
                     .reverse_deps = (#priority "End"),              \
                     .init_fn = (__VA_ARGS__))

// Registers an anonymous initialization function to be call by InitArolla().
#define AROLLA_REGISTER_ANONYMOUS_INITIALIZER(priority, /*init_fn*/...) \
  AROLLA_INITIALIZER(.deps = (#priority "Begin"),                       \
                     .reverse_deps = (#priority "End"),                 \
                     .init_fn = (__VA_ARGS__))

#endif  // AROLLA_UTIL_INIT_AROLLA_H_
