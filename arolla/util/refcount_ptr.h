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
#ifndef AROLLA_UTIL_REFCOUNT_PTR_H_
#define AROLLA_UTIL_REFCOUNT_PTR_H_

#include <cstddef>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "arolla/util/refcount.h"

namespace arolla {

template <typename T>
class
        RefcountPtr;

// The base class for the refcounted object.
class RefcountedBase {
  mutable Refcount refcount_;

  template <typename T>
  friend class
          RefcountPtr;
};

// A smart-pointer designed for objects that inherit from RefcountedBase.
//
// We intentionally implement no comparison operators to avoid the ambiguity
// between `obj1 == obj2` and `ptr1 == ptr2`. If you need them in your
// application, please implement them as free functions for your custom
// `using CustomTypePtr = RefcountPtr<CustomType>`.
//
template <typename T>
class
        RefcountPtr {
 public:
  // Constructs a RefcountPtr from the provided arguments.
  template <typename... Args>
  static constexpr RefcountPtr<T> Make(Args&&... args) noexcept {
    return RefcountPtr(
        std::make_unique<T>(std::forward<Args>(args)...).release());
  }

  // Constructs a refcount-ptr from the given unique pointer *without*
  // incrementing the refcounter.
  //
  // Note: It's expected that the given unique pointer has exclusive ownership
  // of the object.
  static constexpr RefcountPtr<T> Own(std::unique_ptr<T>&& ptr) noexcept {
    return RefcountPtr(ptr.release());
  }

  // Constructs a refcount-ptr from the given raw pointer and increments
  // the refcounter. This provides a functionality that is similar to
  // `shared_from_this()` for `std::shared_ptr`.
  static RefcountPtr<T> NewRef(T* ptr) noexcept {
    if (ptr != nullptr) {
      ptr->refcount_.increment();
    }
    return RefcountPtr(ptr);
  }

  constexpr RefcountPtr() noexcept = default;

  constexpr /*implicit*/ RefcountPtr(  // NOLINT(google-explicit-constructor)
      std::nullptr_t) noexcept {}

  RefcountPtr(const RefcountPtr& rhs) noexcept : ptr_(rhs.ptr_) {
    if (ptr_ != nullptr) {
      ptr_->refcount_.increment();
    }
  }

  RefcountPtr(RefcountPtr&& rhs) noexcept : ptr_(rhs.ptr_) {
    rhs.ptr_ = nullptr;
  }

  ~RefcountPtr() noexcept(noexcept(reset())) { reset(); }

  RefcountPtr& operator=(const RefcountPtr& rhs) noexcept(noexcept(reset())) {
    if (ptr_ != rhs.ptr_) {
    // NOTE: Hold the ownership of the old entity during the assignment because
    // it may indirectly own `rhs`.
    const auto tmp = std::move(*this);
      ptr_ = rhs.ptr_;
      if (ptr_ != nullptr) {
        ptr_->refcount_.increment();
      }
    }
    return *this;
  }

  RefcountPtr& operator=(RefcountPtr&& rhs) noexcept {
    T* const tmp = ptr_;  // We avoid using std::swap because we need this
    ptr_ = rhs.ptr_;      // method to be noexcept, and we do not control
    rhs.ptr_ = tmp;       // the std::swap implementation.
    return *this;
  }

  void reset() noexcept(std::is_nothrow_destructible_v<T>) {
    T* const tmp = ptr_;
    ptr_ = nullptr;
    // TODO: Investigate the performance implications of using
    // `skewed_decrement()` here.
    if (tmp != nullptr && !tmp->refcount_.decrement()) {
      delete tmp;
    }
  }

  constexpr bool operator==(std::nullptr_t) const noexcept {
    return ptr_ == nullptr;
  }
  constexpr bool operator!=(std::nullptr_t) const noexcept {
    return ptr_ != nullptr;
  }
  bool operator==(const RefcountPtr& rhs) const noexcept {
    return ptr_ == rhs.ptr_;
  }
  bool operator!=(const RefcountPtr& rhs) const noexcept {
    return ptr_ != rhs.ptr_;
  }

  constexpr T* get() const noexcept { return ptr_; }

  T& operator*() const noexcept { return *ptr_; }

  T* operator->() const noexcept { return ptr_; }

  friend void swap(RefcountPtr& lhs, RefcountPtr& rhs) noexcept {
    T* const tmp = lhs.ptr_;  // We avoid using std::swap because we need this
    lhs.ptr_ = rhs.ptr_;      // method to be noexcept, and we do not control
    rhs.ptr_ = tmp;           // the std::swap implementation.
  }

  using absl_nullability_compatible = void;

 private:
  // Initializes the current instance without increasing the reference count.
  explicit constexpr RefcountPtr(T* ptr) noexcept : ptr_(ptr) {}

  T* ptr_ = nullptr;
};

// Integration with [D]CHECK_{EQ,NE}.
template <typename T>
std::ostream& operator<<(std::ostream& ostream, const RefcountPtr<T>& ptr) {
  return ostream << static_cast<T*>(ptr.get());
}

// Integration with ::testing::Pointer.
template <typename T>
T* GetRawPointer(const RefcountPtr<T>& ptr) {
  return ptr.get();
}

}  // namespace arolla

#endif  // AROLLA_UTIL_REFCOUNT_PTR_H_
