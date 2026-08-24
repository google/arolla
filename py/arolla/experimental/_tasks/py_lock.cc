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
#include "py/arolla/experimental/_tasks/py_lock.h"

#include <Python.h>

#include <array>
#include <memory>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/synchronization/mutex.h"
#include "arolla/util/cancellation.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {
namespace {

// Forward declare.
extern PyTypeObject PyLock_Type;

struct PyLockObject final {
  PyObject_HEAD;
  struct Fields : std::enable_shared_from_this<Fields> {
    // Mutex protecting `held` and works as condition variable for waiting.
    absl::Mutex mutex;
    // Whether the lock is held.
    bool held ABSL_GUARDED_BY(mutex) = false;
  };
  using FieldsPtr = std::shared_ptr<Fields>;
  FieldsPtr fields;
};

PyLockObject::FieldsPtr& PyLock_fields(PyObject* self) {
  DCHECK_EQ(Py_TYPE(self), &PyLock_Type);
  return reinterpret_cast<PyLockObject*>(self)->fields;
}

// def Lock.__new__(cls)
PyObject* PyLock_new(PyTypeObject* type, PyObject* /*args*/,
                     PyObject* /*kwargs*/) {
  DCheckPyGIL();
  PyObject* self = type->tp_alloc(type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  auto& self_fields = PyLock_fields(self);
  new (&self_fields)
      PyLockObject::FieldsPtr(std::make_shared<PyLockObject::Fields>());
  return self;
}

void PyLock_dealloc(PyObject* self) {
  PyLock_fields(self).~shared_ptr();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyLock_internal_acquire(PyLockObject::Fields& self_fields,
                                  bool blocking) {
  // Acquire cancellation context.
  PyCancellationScope cancellation_scope;
  auto* cancellation_context =
      CancellationContext::ScopeGuard::current_cancellation_context();

  // Early cancellation check.
  RETURN_IF_ERROR(CheckCancellation()).With(&SetPyErrFromStatus);

  // Fast path: try to acquire without blocking.
  if (self_fields.mutex.try_lock()) {
    if (!self_fields.held) {
      self_fields.held = true;
      self_fields.mutex.unlock();
      Py_RETURN_TRUE;
    }
    self_fields.mutex.unlock();
  }
  if (!blocking) {
    Py_RETURN_FALSE;
  }

  // Slow path: without cancellation context.
  if (cancellation_context == nullptr) {
    {
      ReleasePyGIL guard;
      absl::MutexLock lock(self_fields.mutex);
      self_fields.mutex.Await(absl::Condition(
          +[](bool* held) { return !*held; }, &self_fields.held));
      self_fields.held = true;
    }
    Py_RETURN_TRUE;
  }

  // Slow path with cancellation context.
  auto subscription = cancellation_context->Subscribe(
      [weak_self_fields = self_fields.weak_from_this()]() {
        if (auto self_fields = weak_self_fields.lock()) {
          if (self_fields->mutex.try_lock()) {
            self_fields->mutex.unlock();
          }
        }
      });

  struct WaitState {
    bool& held;
    CancellationContext& cancellation_context;
  };
  WaitState wait_state = {self_fields.held, *cancellation_context};
  bool cancelled = false;
  {
    ReleasePyGIL guard;
    absl::MutexLock lock(self_fields.mutex);
    self_fields.mutex.Await(absl::Condition(
        +[](WaitState* s) {
          return s->cancellation_context.Cancelled() || !s->held;
        },
        &wait_state));
    if (cancellation_context->Cancelled()) {
      cancelled = true;
    } else {
      self_fields.held = true;
    }
  }
  if (cancelled) {
    return SetPyErrFromStatus(cancellation_context->GetStatus());
  }
  Py_RETURN_TRUE;
}

// def Lock.acquire(self, blocking=True) -> bool
PyObject* PyLock_acquire(PyObject* self, PyObject* args, PyObject* kwargs) {
  DCheckPyGIL();
  constexpr std::array<const char*, 2> kwlist = {"blocking", nullptr};
  int blocking = 1;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|p:Lock.acquire",
                                   (char**)kwlist.data(), &blocking)) {
    return nullptr;
  }
  return PyLock_internal_acquire(*PyLock_fields(self), blocking != 0);
}

// def Lock.release(self)
PyObject* PyLock_release(PyObject* self, PyObject* /*args*/) {
  auto& self_fields = *PyLock_fields(self);
  absl::MutexLock lock(self_fields.mutex);
  if (!self_fields.held) {
    PyErr_SetString(PyExc_RuntimeError, "release unlocked lock");
    return nullptr;
  }
  self_fields.held = false;
  Py_RETURN_NONE;
}

// def Lock.__enter__(self) -> bool
PyObject* PyLock_enter(PyObject* self, PyObject* /*args*/) {
  return PyLock_internal_acquire(*PyLock_fields(self), /*blocking=*/true);
}

// def Lock.__exit__(self, exc_type, exc_value, trace) -> None
PyObject* PyLock_exit(PyObject* self, PyObject* /*args*/) {
  return PyLock_release(self, nullptr);
}

PyMethodDef kPyLock_methods[] = {
    {
        .ml_name = "acquire",
        .ml_meth = (PyCFunction)PyLock_acquire,
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = ("acquire(blocking=True, /)\n"
                   "--\n\n"
                   "Acquire the lock.\n\n"
                   "If the current thread has an active cancellation context "
                   "and it gets\n"
                   "cancelled (before or during wait), raises an exception.\n"),
    },
    {
        .ml_name = "release",
        .ml_meth = PyLock_release,
        .ml_flags = METH_NOARGS,
        .ml_doc = ("release()\n"
                   "--\n\n"
                   "Release the lock.\n"),
    },
    {
        .ml_name = "__enter__",
        .ml_meth = PyLock_enter,
        .ml_flags = METH_NOARGS,
        .ml_doc = nullptr,
    },
    {
        .ml_name = "__exit__",
        .ml_meth = PyLock_exit,
        .ml_flags = METH_VARARGS,
        .ml_doc = nullptr,
    },
    {nullptr}, /* sentinel */
};

PyTypeObject PyLock_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.experimental._tasks.Lock",
    .tp_basicsize = sizeof(PyLockObject),
    .tp_dealloc = PyLock_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,  // no inheritance
    .tp_doc = R"""(Cancellation-aware lock.

        This is a cancellation-aware substitute for `threading.Lock`.
        When `acquire()` is called in a thread with an active cancellation
        context, it raises if the context is cancelled (before or during wait).

        Example:
          lock = arolla_tasks.Lock()
          with lock:
            ...
        )""",
    .tp_methods = kPyLock_methods,
    .tp_new = PyLock_new,
};

}  // namespace

PyTypeObject* absl_nullable PyLockType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyLock_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyLock_Type);
  return &PyLock_Type;
}

}  // namespace arolla::python
