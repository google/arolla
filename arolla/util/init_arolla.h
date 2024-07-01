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

#include "absl/status/status.h"

namespace arolla {

// Initializes Arolla
//
// Usage:
//

// C++ code has to call InitArolla() explicitly,
// e.g. in main() or test's SetUp.
// The function executes all registered initializers. Order of the execution is
// determined by initializers' priorities. If two initializers have the same
// priority, the one with the lexicographically smaller name gets executed
// first.
//
// To register a function as an initializer, please use macro
//
//     AROLLA_REGISTER_INITIALIZER(priority, name, init_fn)
//
// An example:
//
//   static absl::Status InitBootstrapOperators() {
//     // Code to initialize bootstrap operators.
//   }
//
//   AROLLA_REGISTER_INITIALIZER(
//       kHighest, bootstrap_operators, &InitBootstrapOperators);
//
// Here `kHighest` specifies priority (see ArollaInitializerPriority enum),
// and `bootstrap_operators` specifies name.
//
absl::Status InitArolla();

// An initializer priority.
enum class ArollaInitializerPriority {
  kHighest = 0,
  // <-- Please add new priority values here.
  kRegisterQExprOperators,
  kRegisterSerializationCodecs,
  kRegisterExprOperatorsBootstrap,
  kRegisterExprOperatorsStandard,
  kRegisterExprOperatorsStandardCpp,
  kRegisterExprOperatorsExtraLazy,
  kRegisterExprOperatorsExtraJagged,
  kRegisterExprOperatorsLowest,
  // <- Please avoid operator registrations below this line.
  kLowest,
};

static_assert(static_cast<int>(ArollaInitializerPriority::kLowest) < 32,
              "Re-evaluate design when exceeding the threshold.");

// Registers an initialization function to be call by InitArolla().
//
// Args:
//   priority: A priority value (see ArollaInitializerPriority enum)
//   name: A globally unique name.
//   init_fn: A function with signature `void (*)()` or `absl::Status (*)()`.
//
#define AROLLA_REGISTER_INITIALIZER(priority, name, /*init_fn*/...) \
  extern "C" {                                                      \
  static ::arolla::init_arolla_internal::ArollaInitializer          \
      AROLLA_REGISTER_INITIALIZER_IMPL_CONCAT(arolla_initializer_,  \
                                              __COUNTER__)(         \
          (::arolla::ArollaInitializerPriority::priority), (#name), \
          (__VA_ARGS__));                                           \
  }

// Registers an anonymous initialization function to be call by InitArolla().
//
// This macro does not guarantee the deterministic order of execution for init
// functions with the same priority, unlike AROLLA_REGISTER_INITIALIZER. This
// is because anonymous initializers do not have unique names. However, it has a
// smaller binary overhead as it does not store the name.
//
// Args:
//   priority: A priority value (see ArollaInitializerPriority enum)
//   init_fn: A function with signature `void (*)()` or `absl::Status (*)()`.
//
#define AROLLA_REGISTER_ANONYMOUS_INITIALIZER(priority, /*init_fn*/...) \
  extern "C" {                                                          \
  static ::arolla::init_arolla_internal::ArollaInitializer              \
      AROLLA_REGISTER_INITIALIZER_IMPL_CONCAT(arolla_initializer_,      \
                                              __COUNTER__)(             \
          (::arolla::ArollaInitializerPriority::priority), nullptr,     \
          (__VA_ARGS__));                                               \
  }

// Implementation note: By using the `extern "C"` and `static` keywords instead
// of anonymous namespaces, we reduce the size of the binary file.

#define AROLLA_REGISTER_INITIALIZER_IMPL_CONCAT_INNER(x, y) x##y
#define AROLLA_REGISTER_INITIALIZER_IMPL_CONCAT(x, y) \
  AROLLA_REGISTER_INITIALIZER_IMPL_CONCAT_INNER(x, y)

//
// Implementation Internals (most users should ignore).
//
namespace init_arolla_internal {

class ArollaInitializer {
 public:
  using VoidInitFn = void (*)();
  using StatusInitFn = absl::Status (*)();

  static absl::Status ExecuteAll();

  ArollaInitializer(ArollaInitializerPriority priority, const char* name,
                    VoidInitFn init_fn);

  ArollaInitializer(ArollaInitializerPriority priority, const char* name,
                    StatusInitFn init_fn);

 private:
  static bool execution_flag_;
  static const ArollaInitializer* head_;

  const ArollaInitializer* const next_;
  const ArollaInitializerPriority priority_;
  const char* const /*nullable*/ name_;
  const VoidInitFn void_init_fn_ = nullptr;
  const StatusInitFn status_init_fn_ = nullptr;
};

}  // namespace init_arolla_internal
}  // namespace arolla

#endif  // AROLLA_UTIL_INIT_AROLLA_H_
