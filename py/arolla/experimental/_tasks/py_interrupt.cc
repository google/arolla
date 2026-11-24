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
#include "py/arolla/experimental/_tasks/py_interrupt.h"

#include <Python.h>

#include <array>
#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "py/arolla/py_utils/py_utils.h"

// Py_BEGIN_CRITICAL_SECTION is available in Python 3.13+.
// Earlier versions do not support free threading and rely on the GIL.
#if PY_VERSION_HEX < 0x030d0000
#define Py_BEGIN_CRITICAL_SECTION(op) {
#define Py_END_CRITICAL_SECTION() }
#endif

namespace arolla::python {
namespace {

// Forward declaration.
extern PyTypeObject PyInterruptionScope_Type;

using PyThreadId = decltype(PyThread_get_thread_ident());

struct PyInterruptionScopeObject final {
  PyObject_HEAD;
  enum class State { kInitial, kRunning, kFinished };
  struct Fields {
    State state = State::kInitial;
    PyThreadId py_thread_id = 0;
    bool async_exc_injected = false;
  };
  union {  // Constructed and destroyed manually.
    Fields fields;
  };
};

PyInterruptionScopeObject::Fields& PyInterruptionScope_fields(PyObject* self) {
  DCHECK(Py_TYPE(self) == &PyInterruptionScope_Type);
  return reinterpret_cast<PyInterruptionScopeObject*>(self)->fields;
}

// def InterruptionScope.__new__(cls)
PyObject* PyInterruptionScope_new(PyTypeObject* type, PyObject* args,
                                  PyObject* kwargs) {
  DCheckPyGIL();
  constexpr std::array<const char*, 1> kwlist = {nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                   ":arolla_tasks.InterruptionScope",
                                   const_cast<char**>(kwlist.data()))) {
    return nullptr;
  }
  PyObject* self = type->tp_alloc(type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  new (&PyInterruptionScope_fields(self)) PyInterruptionScopeObject::Fields();
  return self;
}

void PyInterruptionScope_dealloc(PyObject* self) {
  DCheckPyGIL();
  std::destroy_at(&PyInterruptionScope_fields(self));
  Py_TYPE(self)->tp_free(self);
}

// Scope exit runs in C rather than Python because the interpreter checks for
// pending asynchronous exceptions only at bytecode boundaries; once
// the callable returns to C, exit cleanup cannot be interrupted midway.
//
// Thread-state updates stay inside the per-object critical section while
// the target thread is alive: CPython atomically swaps the target thread's
// pending exception before releasing any replaced exception class, preventing
// a concurrent interruption under free threading from landing after exit
// cleanup or hitting a recycled thread identifier. Scope state is updated
// before that call because releasing a replaced exception class may invoke
// weakref callbacks or metaclass finalizers that suspend or re-enter
// the critical section.
//
// def InterruptionScope.run(self, fn, /, *args, **kwargs) -> Any
PyObject* PyInterruptionScope_run(PyObject* self, PyObject* const* args,
                                  Py_ssize_t nargs, PyObject* kwnames) {
  DCheckPyGIL();
  if (nargs < 1) {
    PyErr_SetString(
        PyExc_TypeError,
        "InterruptionScope.run() missing 1 required positional argument: 'fn'");
    return nullptr;
  }
  if (!PyCallable_Check(args[0])) {
    return PyErr_Format(
        PyExc_TypeError,
        "InterruptionScope.run() expected a callable, got fn: %s",
        Py_TYPE(args[0])->tp_name);
  }
  auto& fields = PyInterruptionScope_fields(self);
  bool already_started = false;
  {
    Py_BEGIN_CRITICAL_SECTION(self);
    if (fields.state != PyInterruptionScopeObject::State::kInitial) {
      already_started = true;
    } else {
      fields.state = PyInterruptionScopeObject::State::kRunning;
      fields.py_thread_id = PyThread_get_thread_ident();
    }
    Py_END_CRITICAL_SECTION();
  }
  if (already_started) {
    PyErr_SetString(PyExc_RuntimeError,
                    "InterruptionScope.run() can only be called once");
    return nullptr;
  }

  PyObject* result = PyObject_Vectorcall(args[0], args + 1, nargs - 1, kwnames);
  {
    Py_BEGIN_CRITICAL_SECTION(self);
    fields.state = PyInterruptionScopeObject::State::kFinished;
    if (std::exchange(fields.async_exc_injected, false)) {
      PyThreadState_SetAsyncExc(fields.py_thread_id, nullptr);
    }
    Py_END_CRITICAL_SECTION();
  }
  return result;
}

// def InterruptionScope.interrupt(self, exception_type=None) -> None
PyObject* PyInterruptionScope_interrupt(PyObject* self, PyObject* args,
                                        PyObject* kwargs) {
  DCheckPyGIL();
  constexpr std::array<const char*, 2> kwlist = {"exception_type", nullptr};
  PyObject* py_exc = Py_None;
  if (!PyArg_ParseTupleAndKeywords(
          args, kwargs, "|O:InterruptionScope.interrupt",
          const_cast<char**>(kwlist.data()), &py_exc)) {
    return nullptr;
  }
  if (py_exc == Py_None) {
    py_exc = PyExc_KeyboardInterrupt;
  }
  if (!PyExceptionClass_Check(py_exc)) {
    return PyErr_Format(
        PyExc_TypeError,
        "`exception_type` must be a BaseException subclass, got %R", py_exc);
  }
  auto& fields = PyInterruptionScope_fields(self);
  {
    Py_BEGIN_CRITICAL_SECTION(self);
    if (fields.state == PyInterruptionScopeObject::State::kRunning) {
      fields.async_exc_injected = true;
      PyThreadState_SetAsyncExc(fields.py_thread_id, py_exc);
    }
    Py_END_CRITICAL_SECTION();
  }
  Py_RETURN_NONE;
}

PyMethodDef kPyInterruptionScope_methods[] = {
    {
        .ml_name = "run",
        .ml_meth = reinterpret_cast<PyCFunction>(PyInterruptionScope_run),
        .ml_flags = METH_FASTCALL | METH_KEYWORDS,
        .ml_doc =
            ("run($self, fn, /, *args, **kwargs)\n"
             "--\n\n"
             "Runs `fn(*args, **kwargs)` within this interruption scope, "
             "clearing any unconsumed async exceptions on exit. Each "
             "scope instance can be run at most once, and cannot be reused."),
    },
    {
        .ml_name = "interrupt",
        .ml_meth = reinterpret_cast<PyCFunction>(PyInterruptionScope_interrupt),
        .ml_flags = METH_VARARGS | METH_KEYWORDS,
        .ml_doc = ("interrupt($self, exception_type=None)\n"
                   "--\n\n"
                   "Interrupts the scope by injecting `exception_type` into "
                   "the thread running `run()`. By default, `exception_type` "
                   "is `KeyboardInterrupt`."),
    },
    {nullptr}, /* sentinel */
};

PyTypeObject PyInterruptionScope_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.experimental._tasks.InterruptionScope",
    .tp_basicsize = sizeof(PyInterruptionScopeObject),
    .tp_dealloc = PyInterruptionScope_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = ("InterruptionScope()\n"
               "--\n\n"
               "Scope that binds thread interruption (`interrupt()`) to a "
               "single callable execution."),
    .tp_methods = kPyInterruptionScope_methods,
    .tp_new = PyInterruptionScope_new,
};

}  // namespace

PyTypeObject* absl_nullable PyInterruptionScopeType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyInterruptionScope_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyInterruptionScope_Type);
  return &PyInterruptionScope_Type;
}

}  // namespace arolla::python
