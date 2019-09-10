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
#ifndef AROLLA_UTIL_INDESTRUCTIBLE_H_
#define AROLLA_UTIL_INDESTRUCTIBLE_H_

#include <functional>
#include <initializer_list>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"

namespace arolla {

// A wrapper around an object of type T, that suppress object's destruction.
// The key use case is holding singleton objects:
//
//   static Indestructible<T> singleton;
//
// The object is stored as a value, with no extra indirection is involved.
//
// Since destructor is never called, the object lives on during program exit and
// can be safely accessed by any thread.
//
template <typename T>
class Indestructible final {
 public:
  // Forwards arguments to the T's constructor.
  template <typename... Args>
  explicit constexpr ABSL_ATTRIBUTE_ALWAYS_INLINE Indestructible(
      Args&&... args) {
    new (&storage_) T(std::forward<Args>(args)...);
  }

  // Forwards initializer_list to the T's constructor.
  template <typename U>
  constexpr ABSL_ATTRIBUTE_ALWAYS_INLINE Indestructible(
      std::initializer_list<U> lst) {
    new (&storage_) T(lst);
  }

  // Delegates the object construction to `init_fn`. The primary application is
  // for types with no public constructors. Please see an example in the tests.
  template <typename InitFn,
            typename = std::enable_if_t<std::is_invocable_v<InitFn, void*>>>
  explicit constexpr ABSL_ATTRIBUTE_ALWAYS_INLINE Indestructible(
      InitFn init_fn) {
    std::invoke(init_fn, static_cast<void*>(&storage_));
  }

  // No copying.
  Indestructible(const Indestructible&) = delete;
  Indestructible& operator=(const Indestructible&) = delete;

  // Smart pointer interface.
  constexpr T* get() noexcept { return reinterpret_cast<T*>(&storage_); }
  constexpr const T* get() const noexcept {
    return reinterpret_cast<const T*>(&storage_);
  }

  constexpr T& operator*() noexcept { return *get(); }
  constexpr const T& operator*() const noexcept { return *get(); }

  constexpr T* operator->() noexcept { return get(); }
  constexpr const T* operator->() const noexcept { return get(); }

 private:
  std::aligned_storage_t<sizeof(T), alignof(T)> storage_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_INDESTRUCTIBLE_H_
