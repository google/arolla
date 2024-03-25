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
// Please don't include this file directly.
//
// This file defines a template BasePyObjectPtr, that is used for the
// classes PyObjectPtr and PyObjectGILSafePtr in py_utils.h.

#ifndef PY_AROLLA_PY_UTILS_PY_OBJECT_PTR_IMPL_H_
#define PY_AROLLA_PY_UTILS_PY_OBJECT_PTR_IMPL_H_

#include <Python.h>

#include <cstddef>
#include <utility>

#include "absl/base/attributes.h"

namespace arolla::python::py_object_ptr_impl_internal {

// Base class for PyObjectPtr and PyObjectGILSafePtr.
//
// This base class has a twofold purpose:
//  * provide a customization point for unit-testing;
//  * share the code between PyObject*Ptr classes.
//
// Template parameters:
//  * SelfType is the full pointer class (inherited from this)
//
//  * Traits::GILGuard is a RAII-style guard for operations with PyObject
//    ref-counter.
//  * Traits::PyObject is a C struct representing a python object.
//  * Traits::inc_ref(ptr) increases the object ref-counter.
//  * Traits::dec_ref(ptr) decreases the object ref-counter.
//
template <typename SelfType, typename Traits>
class BasePyObjectPtr {
  // Unpack traits:
  using GILGuardType = typename Traits::GILGuardType;
  using PyObjectType = typename Traits::PyObjectType;

  ABSL_ATTRIBUTE_ALWAYS_INLINE static void inc_ref(PyObjectType* ptr) {
    Traits().inc_ref(ptr);
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE static void dec_ref(PyObjectType* ptr) {
    Traits().dec_ref(ptr);
  }

 public:
  // Returns a smart-pointer constructed from the given raw pointer to
  // PyObjectType instance *without* increasing the ref-counter.
  [[nodiscard]] static SelfType Own(PyObjectType* ptr) {
    SelfType result;
    result.ptr_ = ptr;
    return result;
  }

  // Returns a smart-pointer constructed from the given raw pointer to
  // PyObjectType instance *with* increasing the ref-counter.
  [[nodiscard]] static SelfType NewRef(PyObjectType* ptr) {
    SelfType result;
    if (ptr != nullptr) {
      GILGuardType gil_guard;
      result.ptr_ = ptr;
      inc_ref(result.ptr_);
    }
    return result;
  }

  // Default-constructible.
  BasePyObjectPtr() = default;
  ~BasePyObjectPtr() { reset(); }

  // Copyable.
  BasePyObjectPtr(const BasePyObjectPtr& other) {
    if (other.ptr_ != nullptr) {
      GILGuardType gil_guard;
      ptr_ = other.ptr_;
      inc_ref(ptr_);
    }
  }

  BasePyObjectPtr& operator=(const BasePyObjectPtr& other) {
    if (ptr_ != other.ptr_) {
      GILGuardType gil_guard;
      PyObjectType* old_ptr = std::exchange(ptr_, other.ptr_);
      if (ptr_ != nullptr) {
        inc_ref(ptr_);
      }
      if (old_ptr != nullptr) {
        dec_ref(old_ptr);
      }
    }
    return *this;
  }

  // Movable.
  BasePyObjectPtr(BasePyObjectPtr&& other) : ptr_(other.release()) {}

  BasePyObjectPtr& operator=(BasePyObjectPtr&& other) {
    PyObjectType* old_ptr = std::exchange(ptr_, other.release());
    if (old_ptr != nullptr) {
      GILGuardType gil_guard;
      dec_ref(old_ptr);
    }
    return *this;
  }

  // Returns a raw pointer to the managed PyObjectType.
  [[nodiscard]] PyObjectType* get() const { return ptr_; }

  bool operator==(std::nullptr_t) const { return ptr_ == nullptr; }
  bool operator!=(std::nullptr_t) const { return ptr_ != nullptr; }

  // Releases the managed object without decrementing the ref-counter.
  [[nodiscard]] PyObjectType* release() { return std::exchange(ptr_, nullptr); }

  // Resets the state of the smart-pointer.
  void reset() {
    if (PyObjectType* old_ptr = release()) {
      GILGuardType gil_guard;
      dec_ref(old_ptr);
    }
  }

 private:
  PyObjectType* ptr_ = nullptr;
};

}  // namespace arolla::python::py_object_ptr_impl_internal

#endif  // PY_AROLLA_PY_UTILS_PY_OBJECT_PTR_IMPL_H_
