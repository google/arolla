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
// TODO: Consider changing the return type to `void`.
absl::Status InitArolla();

}  // namespace arolla

// Registers an initialization function to be called by InitArolla().
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
// All parameters are optional.
//
// Example:
//
//   static absl::Status RegisterExprBootstrapOperators() { ... }
//
//   AROLLA_INITIALIZER(.name = "//arolla/expr/operators:bootstrap",
//                      .deps = "//arolla/qexpr/operators/bootstrap",
//                      .init_fn = &RegisterExprOperatorsBootstrap);
//
//   Here, .deps = "//arolla/qexpr/operators/bootstrap" indicates that
//   `RegisterExprOperatorsBootstrap()` will be invoked after the
//   "//arolla/qexpr/operators/bootstrap" initializer.
//
#define AROLLA_INITIALIZER(...)                                                \
  extern "C" {                                                                 \
  __attribute__((constructor(65534))) static void                              \
  AROLLA_INITIALIZER_IMPL_CONCAT(arolla_initializer_register, __COUNTER__)() { \
    static constexpr ::arolla::init_arolla_internal::Initializer initializer = \
        {__VA_ARGS__};                                                         \
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
