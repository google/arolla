// Copyright 2025 Google LLC
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
#ifndef PY_AROLLA_PY_UTILS_PY_OBJECT_BRIDGE_H_
#define PY_AROLLA_PY_UTILS_PY_OBJECT_BRIDGE_H_

#include <Python.h>

#include <concepts>
#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {
namespace py_object_bridge {

// Dispatches an action to be executed under the GIL.
//
// If the current thread holds the GIL, the action may be executed inline
// (ordering across `DispatchAction()` calls is not guaranteed).
//
// IMPORTANT: Actions are expected to be fast! If you need to perform
// a long-running computation, please use the action to schedule work
// on a different thread.
void DispatchAction(absl::AnyInvocable<void() &&> action) noexcept;

// Starts the bridge worker thread, and registers an `atexit` hook.
//
// This function is idempotent and can be safely called from any Python
// extension module that depends on the bridge.
//
// Requires: GIL held.
void Init();

}  // namespace py_object_bridge

// Thread-safe, GIL-independent holder of a Python object.
//
// Safe to move or destroy from any thread without holding the GIL. When
// destroyed, it decrefs `py_obj` inline if the caller holds the GIL; otherwise,
// it delegates the decref to the bridge worker thread (see
// `py_object_bridge::Init()`).
//
// The primary use case for this class is passing Python objects into
// Python-agnostic C++ code where lifetime management is unpredictable
// (e.g., async callbacks or `absl::Status` payloads). If the C++ code
// already uses locks, introducing interactions with the GIL creates a
// risk of deadlocks.
//
// This class mitigates that risk by delegating GIL-dependent operations
// to the bridge worker thread, at the cost of cross-thread communication.
//
// IMPORTANT: Python sub-interpreters are not supported. The bridge assumes
// that all managed objects belong to the main interpreter, because all
// GIL-dependent work is delegated to a single worker thread attached to it.
//
class PyObjectHolder {
 public:
  // Creates an empty holder.
  PyObjectHolder() noexcept = default;

  // Creates a holder that takes ownership of the given `py_obj`.
  //
  // Requires: GIL held.
  explicit PyObjectHolder(PyObjectPtr absl_nullable py_obj) noexcept;

  // If non-empty, the destructor dispatches a decref action.
  ~PyObjectHolder() noexcept;

  // Move-only.
  PyObjectHolder(PyObjectHolder&&) noexcept;
  PyObjectHolder& operator=(PyObjectHolder&&) noexcept;

  // Returns a reference to the managed `py_obj`.
  const PyObjectPtr absl_nullable& py_obj() const noexcept { return py_obj_; }

  // Dispatches an action to be executed under the GIL.
  //
  // The action is called as `std::move(action)(std::move(py_obj))`, where
  // `py_obj` is a non-null `PyObjectPtr`.
  //
  // If the current thread holds the GIL, the action may be executed
  // inline (ordering across `DispatchAction()` calls is not guaranteed).
  //
  // IMPORTANT: Actions are expected to be fast! If you need to perform
  // a long-running computation, please use the action to schedule work
  // on a different thread.
  //
  // Note: The action is type-erased together with the managed object, so
  // small actions (capturing up to one pointer) avoid heap allocations.
  template <typename Action>
    requires(std::invocable<std::decay_t<Action> &&, PyObjectPtr>)
  void DispatchAction(Action&& action) && noexcept {
    DCHECK(py_obj_ != nullptr);
    if (py_obj_ != nullptr) {
      py_object_bridge::DispatchAction(
          [py_obj = std::move(py_obj_),
           action = std::forward<Action>(action)]() mutable {
            std::move(action)(std::move(py_obj));
          });
    }
  }

 private:
  PyObjectPtr absl_nullable py_obj_;
};

}  // namespace arolla::python

#endif  // PY_AROLLA_PY_UTILS_PY_OBJECT_BRIDGE_H_
