// In this file, we access some internal Python APIs.
//
// This code is kept separate from the rest of py_utils.cc for two reasons:
// (1) We use plain C here to ensure compatibility with Python's internal
// source. (2) We minimize the amount of code that depends on the potentially
// unstable internal details of the Python interpreter.

#include <Python.h>

// In CPython 3.12 there is a bug with mixing internal and non-internal headers:
// https://github.com/python/cpython/issues/105268
// The fix is not backported, so using undef as a workaround here.
#if PY_VERSION_HEX >= 0x030c0000
#undef _PyGC_FINALIZED
#endif

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
