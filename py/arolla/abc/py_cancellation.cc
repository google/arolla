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
#include "py/arolla/abc/py_cancellation.h"

#include <Python.h>

#include <array>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/cancellation.h"
#include "py/arolla/py_utils/py_cancellation_controller.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {
namespace {

// Forward declare.
extern PyTypeObject PyCancellationContext_Type;

// CancellationContext representation for python.
struct PyCancellationContextObject final {
  struct Fields {
    /*absl_nonnull*/ CancellationContextPtr cancellation_context;
  };
  PyObject_HEAD;
  Fields fields;
};

PyCancellationContextObject::Fields& PyCancellationContext_fields(
    PyObject* self) {
  DCHECK_EQ(Py_TYPE(self), &PyCancellationContext_Type);
  return reinterpret_cast<PyCancellationContextObject*>(self)->fields;
}

PyObject* PyCancellationContext_new(
    CancellationContextPtr cancellation_context) {
  if (PyType_Ready(&PyCancellationContext_Type) < 0) {
    return nullptr;
  }
  PyObject* self =
      PyCancellationContext_Type.tp_alloc(&PyCancellationContext_Type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  new (&PyCancellationContext_fields(self))
      PyCancellationContextObject::Fields{std::move(cancellation_context)};
  return self;
}

PyObject* PyCancellationContext_new(PyTypeObject* /*type*/,
                                    PyObject* py_tuple_args,
                                    PyObject* py_dict_kwargs) {
  DCheckPyGIL();
  constexpr std::array<const char*, 1> kwlist = {nullptr};
  if (!PyArg_ParseTupleAndKeywords(py_tuple_args, py_dict_kwargs,
                                   ":arolla.abc.CancellationContext.__new__",
                                   (char**)kwlist.data())) {
    return nullptr;
  }
  return PyCancellationContext_new(CancellationContext::Make());
}

void PyCancellationContext_dealloc(PyObject* self) {
  PyCancellationContext_fields(self).~Fields();
  Py_TYPE(self)->tp_free(self);
}

PyObject* PyCancellationContext_repr(PyObject* self) {
  const auto& cancellation_context =
      PyCancellationContext_fields(self).cancellation_context;
  return PyUnicode_FromFormat(
      "<CancellationContext(addr=%p, cancelled=%s)>",
      cancellation_context.get(),
      (cancellation_context != nullptr && cancellation_context->Cancelled()
           ? "True"
           : "False"));
}

PyObject* PyCancellationContext_cancel(PyObject* self, PyObject* py_tuple_args,
                                       PyObject* py_dict_kwargs) {
  constexpr std::array<const char*, 2> kwlist = {"msg", nullptr};
  const char* msg = "";
  if (!PyArg_ParseTupleAndKeywords(py_tuple_args, py_dict_kwargs,
                                   "|s:arolla.abc.CancellationContext.cancel",
                                   (char**)kwlist.data(), &msg)) {
    return nullptr;
  }
  PyCancellationContext_fields(self).cancellation_context->Cancel(
      absl::CancelledError(msg));
  Py_RETURN_NONE;
}

PyObject* PyCancellationContext_cancelled(PyObject* self,
                                          PyObject* /*py_arg*/) {
  return PyBool_FromLong(
      PyCancellationContext_fields(self).cancellation_context->Cancelled());
}

PyObject* PyCancellationContext_raise_if_cancelled(PyObject* self,
                                                   PyObject* /*py_arg*/) {
  auto status =
      PyCancellationContext_fields(self).cancellation_context->GetStatus();
  if (!status.ok()) {
    return SetPyErrFromStatus(status);
  }
  Py_RETURN_NONE;
}

PyMethodDef kPyCancellationContext_methods[] = {
    {
        "cancel",
        reinterpret_cast<PyCFunction>(&PyCancellationContext_cancel),
        METH_VARARGS | METH_KEYWORDS,
        ("cancel(msg='')\n"
         "--\n\n"
         "Cancels the context."),
    },
    {
        "cancelled",
        &PyCancellationContext_cancelled,
        METH_NOARGS,
        ("cancelled()\n"
         "--\n\n"
         "Returns `True` if the context is cancelled."),
    },
    {
        "raise_if_cancelled",
        &PyCancellationContext_raise_if_cancelled,
        METH_NOARGS,
        ("raise_if_cancelled()\n"
         "--\n\n"
         "Raises an exception if the context is cancelled."),
    },
    {nullptr} /* sentinel */
};

PyTypeObject PyCancellationContext_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.CancellationContext",
    .tp_basicsize = sizeof(PyCancellationContextObject),
    .tp_dealloc = PyCancellationContext_dealloc,
    .tp_repr = PyCancellationContext_repr,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc =
        ("A cancellation context class.\n\n"
         "Cancellation context is a primitive for signaling cancellation "
         "across\n"
         "multiple control flows.\n\n"
         "To make a cancellation context 'current' within the current control "
         "flow,\n"
         "use `arolla.abc.call_with_cancellation()`. To work with the current\n"
         "cancellation context, use:\n\n"
         "  * arolla.abc.current_cancellation_context()\n"
         "  * arolla.abc.raise_if_cancelled()\n"
         "  * arolla.abc.cancelled()\n\n"
         "It's safe to share a cancellation context object between threads."),
    .tp_methods = kPyCancellationContext_methods,
    .tp_new = PyCancellationContext_new,
};

// go/keep-sorted start block=yes newline_separated=yes
// def cancelled() -> bool
PyObject* PyCancelled(PyObject* /*self*/, PyObject* /*py_arg*/) {
  return PyBool_FromLong(Cancelled());
}

// def current_cancellation_context() -> CancellationContext|None
PyObject* PyCurrentCancellationContext(PyObject* /*self*/, PyObject* /*arg*/) {
  auto* cancellation_context =
      CancellationContext::ScopeGuard::current_cancellation_context();
  if (cancellation_context == nullptr) {
    Py_RETURN_NONE;
  }
  return PyCancellationContext_new(
      CancellationContextPtr::NewRef(cancellation_context));
}

// def raise_if_cancelled() -> None
PyObject* PyRaiseIfCancelled(PyObject* /*self*/, PyObject* /*py_arg*/) {
  auto status = CheckCancellation();
  if (!status.ok()) {
    return SetPyErrFromStatus(status);
  }
  Py_RETURN_NONE;
}

// def run_in_cancellation_context(
//     cancellation_context: CancellationContext|None,
//     fn: Callable[..., Any],
//     /,
//     *args: Any,
//     **kwargs: Any
// ) -> Any
PyObject* PyRunInCancellationContext(PyObject* /*self*/, PyObject** py_args,
                                     Py_ssize_t nargs,
                                     PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  // Parse the arguments.
  if (nargs == 0) {
    PyErr_SetString(
        PyExc_TypeError,
        "arolla.abc.run_in_cancellation_context() missing 2 "
        "required positional arguments: 'cancellation_context', 'fn'");
    return nullptr;
  }
  if (nargs == 1) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.run_in_cancellation_context() missing 1 "
                    "required positional argument: 'fn'");
    return nullptr;
  }
  CancellationContextPtr cancellation_context;
  auto* py_cancellation_context = py_args[0];
  if (py_cancellation_context == Py_None) {
    // pass
  } else if (Py_TYPE(py_cancellation_context) == &PyCancellationContext_Type) {
    cancellation_context = PyCancellationContext_fields(py_cancellation_context)
                               .cancellation_context;
  } else {
    return PyErr_Format(
        PyExc_TypeError,
        "arolla.abc.run_in_cancellation_context() expected "
        "CancellationContext or None, got cancellation_context: %s",
        Py_TYPE(py_cancellation_context)->tp_name);
  }
  auto* py_fn = py_args[1];
  if (!PyCallable_Check(py_fn)) {
    return PyErr_Format(PyExc_TypeError,
                        "arolla.abc.run_in_cancellation_context() expected "
                        "a callable, got fn: %s",
                        Py_TYPE(py_fn)->tp_name);
  }
  CancellationContext::ScopeGuard cancellation_scope(
      std::move(cancellation_context));
  // If the context is already cancelled, immediately return an error.
  if (auto status = CheckCancellation(); !status.ok()) {
    return SetPyErrFromStatus(status);
  }
  return PyObject_Vectorcall(py_fn, py_args + 2,
                             (nargs - 2) | PY_VECTORCALL_ARGUMENTS_OFFSET,
                             py_tuple_kwnames);
}

// def run_in_default_cancellation_context(
//     fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any
PyObject* PyRunInDefaultCancellationContext(PyObject* /*self*/,
                                            PyObject** py_args,
                                            Py_ssize_t nargs,
                                            PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  // Parse the arguments.
  if (nargs == 0) {
    PyErr_SetString(
        PyExc_TypeError,
        "arolla.abc.run_in_default_cancellation_context() missing 1 "
        "required positional arguments: 'fn'");
    return nullptr;
  }
  auto* py_fn = py_args[0];
  if (!PyCallable_Check(py_fn)) {
    return PyErr_Format(
        PyExc_TypeError,
        "arolla.abc.run_in_default_cancellation_context() expected "
        "a callable, got fn: %s",
        Py_TYPE(py_fn)->tp_name);
  }
  auto call_py_fn = [&]() -> PyObject* {
    // If the context is already cancelled, immediately return an error.
    if (auto status = CheckCancellation(); !status.ok()) {
      return SetPyErrFromStatus(status);
    }
    return PyObject_Vectorcall(py_fn, py_args + 1,
                               (nargs - 1) | PY_VECTORCALL_ARGUMENTS_OFFSET,
                               py_tuple_kwnames);
  };
  // Instantiate a base python cancellation scope.
  PyCancellationScope py_cancellation_scope;
  if (CancellationContext::ScopeGuard::current_cancellation_context()) {
    // A context is active; execute the function.
    return call_py_fn();
  }
  // No cancellation context found (off Python's main thread?); explicitly
  // create a temporary one.
  CancellationContext::ScopeGuard cancellation_scope;
  return call_py_fn();
}

// def simulate_SIGINT() -> None
PyObject* PySimulateSIGINT(PyObject* /*self*/, PyObject* /*py_arg*/) {
  py_cancellation_controller::SimulateSIGINT();
  Py_RETURN_NONE;
}

// go/keep-sorted end

}  // namespace

PyTypeObject* PyCancellationContextType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyCancellationContext_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyCancellationContext_Type);
  return &PyCancellationContext_Type;
}

// go/keep-sorted start block=yes newline_separated=yes
const PyMethodDef kDefPyCancelled = {
    "cancelled",
    &PyCancelled,
    METH_NOARGS,
    ("cancelled()\n"
     "--\n\n"
     "Returns `True` if the current cancellation context is cancelled.\n\n"
     "Note: If you use this function within a performance-critical loop,\n"
     "consider storing it in a local variable:\n\n"
     "  cancelled = arolla.abc.cancelled\n"
     "  while ... and not cancelled():\n"
     "    ...\n"),
};

const PyMethodDef kDefPyCurrentCancellationContext = {
    "current_cancellation_context",
    &PyCurrentCancellationContext,
    METH_NOARGS,
    ("current_cancellation_context()\n"
     "--\n\n"
     "Returns the current cancellation context or None."),
};

const PyMethodDef kDefPyRaiseIfCancelled = {
    "raise_if_cancelled",
    &PyRaiseIfCancelled,
    METH_NOARGS,
    ("raise_if_cancelled()\n"
     "--\n\n"
     "Raises an exception if the current cancellation context is cancelled.\n\n"
     "Note: If you use this function within a performance-critical loop,\n"
     "consider storing it in a local variable:\n\n"
     "  raise_if_cancelled = arolla.abc.raise_if_cancelled\n"
     "  while ...:\n"
     "    raise_if_cancelled()\n"),
};

const PyMethodDef kDefPyRunInCancellationContext = {
    "run_in_cancellation_context",
    reinterpret_cast<PyCFunction>(&PyRunInCancellationContext),
    METH_FASTCALL | METH_KEYWORDS,
    ("run_in_cancellation_context("
     "cancellation_context, fn, /, *args, **kwargs)\n"
     "--\n\n"
     "Calls `fn(*args, **kwargs)` within the given cancellation context."),
};

const PyMethodDef kDefPyRunInDefaultCancellationContext = {
    "run_in_default_cancellation_context",
    reinterpret_cast<PyCFunction>(&PyRunInDefaultCancellationContext),
    METH_FASTCALL | METH_KEYWORDS,
    ("run_in_default_cancellation_context(fn, /, *args, **kwargs)\n"
     "--\n\n"
     "Runs `fn(*args, **kwargs)` in the default cancellation context.\n\n"
     "The default cancellation context is determined as follows:\n"
     "1) Use the current cancellation context, if available.\n"
     "2) Otherwise, if running on Python's main thread, use a context that\n"
     "   reacts to SIGINT.\n"
     "3) Otherwise, create a new cancellation context."),
};

const PyMethodDef kDefPySimulateSIGINT = {
    "simulate_SIGINT",
    &PySimulateSIGINT,
    METH_NOARGS,
    ("simulate_SIGINT()\n"
     "--\n\n"
     "Simulate the effect of SIGINT on the existing cancellation contexts."),
};

// go/keep-sorted end

}  // namespace arolla::python
