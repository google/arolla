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
#ifndef AROLLA_UTIL_FAST_DYNAMIC_DOWNCAST_FINAL_H_
#define AROLLA_UTIL_FAST_DYNAMIC_DOWNCAST_FINAL_H_

#include <type_traits>
#include <typeinfo>

#include "absl//base/attributes.h"

namespace arolla {

// Returns a pointer safely converted to the given final sub-class.
//
// This function is a faster alternative for dynamic_cast<T>(S*). It implements
// only a subset of what dynamic_cast supports: pointer downcasting to a final
// subclass T. The implementation is based on typeid() for the type
// identification and static_cast() for the casting, and inherits their
// limitations (e.g. it doesn't support virtual inheritance).
//
// NOTE: The modern compilers don't implement this optimisation for
// dynamic_cast<T*>(src), and we don't know exactly why.
// But we see the performance difference on the benchmarks.
//
template <typename T, typename S>
ABSL_ATTRIBUTE_ALWAYS_INLINE inline T fast_dynamic_downcast_final(S* src) {
  static_assert(std::is_pointer_v<T>);
  static_assert(
      !std::is_const_v<S> || std::is_const_v<std::remove_pointer_t<T>>,
      "cannot cast 'const' to 'non-const'");
  using DecayedT = std::decay_t<std::remove_pointer_t<T>>;
  using DecayedS = std::decay_t<S>;
  static_assert(!std::is_same_v<DecayedS, DecayedT>);
  static_assert(std::is_base_of_v<DecayedS, DecayedT>);
  static_assert(  // public inheritance
      std::is_convertible_v<DecayedT*, DecayedS*>);
  static_assert(std::is_polymorphic_v<DecayedS>);
  static_assert(std::is_final_v<DecayedT>);
  if (src != nullptr && typeid(*src) == typeid(DecayedT)) {
    return static_cast<T>(src);
  }
  return nullptr;
}

}  // namespace arolla

#endif  // AROLLA_UTIL_FAST_DYNAMIC_DOWNCAST_FINAL_H_
