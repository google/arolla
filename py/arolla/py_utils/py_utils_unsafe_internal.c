// In this file, we access some internal Python APIs.
//
// This code is kept separate from the rest of py_utils.cc for two reasons:
// (1) We use plain C here to ensure compatibility with Python's internal
// source. (2) We minimize the amount of code that depends on the potentially
// unstable internal details of the Python interpreter.

#include <Python.h>

#define Py_BUILD_CORE
// _Py_ThreadCanHandleSignals
#include <internal/pycore_pystate.h>
#undef Py_BUILD_CORE

int arolla_python_unsafe_internal_PyErr_CanCallCheckSignal(void) {
  PyInterpreterState* interp = PyThreadState_GET()->interp;
  // Note: We use the private _Py_ThreadCanHandleSignals() because this is what
  // is used in PyErr_CheckSignal(). Unfortunately, we found no public API
  // that provides the same information.
  return _Py_ThreadCanHandleSignals(interp);
}
