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
#ifndef THIRD_PARTY_PY_AROLLA_PY_UTILS_PY_UTILS_H_
#define THIRD_PARTY_PY_AROLLA_PY_UTILS_PY_UTILS_H_

#include <Python.h>

#include <cstddef>
#include <optional>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/cancellation.h"
#include "py/arolla/py_utils/py_object_ptr_impl.h"

namespace arolla::python {

// Sets a python error based on absl::Status. If the provided status has a
// Python exception attached, the status.message() is 1) ignored in the case of
// 'arolla.py_utils.PyException' and 2) used as the main error in the case of
// 'arolla.py_utils.PyExceptionCause'. For more information, please see the
// py_object_as_status_payload.h.
//
// Returns nullptr to make it easier to use with ASSIGN_OR_RETURN and
// RETURN_IF_ERROR.
// Examples:
// ASSIGN_OR_RETURN(auto res, CallFn(), SetPyErrFromStatus(_));
// RETURN_IF_ERROR(CallFn()).With(SetPyErrFromStatus);
// In case a different value needs to be returned, one can use
// ASSIGN_OR_RETURN(auto res, CallFn(), (SetPyErrFromStatus(_), -1));
//
// NOTE: This function expects that !status.ok().
//
// IMPORTANT: This functions requires that the current thread is ready to call
// the Python C API (see AcquirePyGIL for extra information).
std::nullptr_t SetPyErrFromStatus(const absl::Status& status);

// Returns an absl::Status object with the Python exception attached as a
// payload 'arolla.py_utils.PyExceptionCause' (for more information, please see
// the py_object_as_status_payload.h). If there is no active Python exception,
// the function will return absl::OkStatus.
//
// IMPORTANT: This functions requires that the current thread is ready to call
// the Python C API (see AcquirePyGIL for extra information).
//
absl::Status StatusCausedByPyErr(absl::StatusCode code,
                                 absl::string_view message);

// Returns an absl::Status object with the Python exception attached as a
// payload 'arolla.py_utils.PyException' (for more information, please see
// the py_object_as_status_payload.h). If there is no active Python exception,
// the function will return absl::OkStatus.
//
// IMPORTANT: This functions requires that the current thread is ready to call
// the Python C API (see AcquirePyGIL for extra information).
//
absl::Status StatusWithRawPyErr(absl::StatusCode code,
                                absl::string_view message);

// An assertion check that the current thread is ready to call Python C API.
inline void DCheckPyGIL() { DCHECK(PyGILState_Check()); }

// A convenient RAII-style guard to ensure that the current thread is ready to
// call the Python C API.
//
// An analogue of:
//
//   {
//     auto gil_state = PyGILState_Ensure();
//     /* python action */
//     PyGILState_Release(gil_state);
//   }
//
// The implementation is based on PyGILState_Ensure()
//
//   https://docs.python.org/3/c-api/init.html#c.PyGILState_Ensure
//
// and inherits its properties. In particular:
//
//  * You can instantiate nested AcquirePyGIL and ReleasePyGIL guards
//    (it doesn't lead to deadlocks).
//
//  * Instantiating a guard when the runtime is finalizing will terminate
//    the thread.
//
class [[nodiscard]] AcquirePyGIL {
 public:
  AcquirePyGIL() : gil_state_(PyGILState_Ensure()) {}

  ~AcquirePyGIL() { PyGILState_Release(gil_state_); }

  // Non-movable & non-copyable.
  AcquirePyGIL(const AcquirePyGIL&) = delete;
  AcquirePyGIL& operator=(const AcquirePyGIL&) = delete;

 private:
  PyGILState_STATE gil_state_;
};

// A convenience RAII-style guard to lift GIL and allow some other python
// thread to make actions.
//
// An analogue of:
//
//   {
//     auto tstate = PyEval_SaveThread();
//     /* some blocking operation, like I/O */
//     PyEval_RestoreThread(tstate);
//   }
//
// The implementation is based on PyEval_SaveThread()
//
//   https://docs.python.org/3/c-api/init.html#c.PyEval_SaveThread
//
class [[nodiscard]] ReleasePyGIL {
 public:
  // Release previously acquired GIL.
  ReleasePyGIL() : thread_state_(PyEval_SaveThread()) {}

  // Re-acquire GIL.
  ~ReleasePyGIL() { PyEval_RestoreThread(thread_state_); }

  // Non-movable & non-copyable.
  ReleasePyGIL(const ReleasePyGIL&) = delete;
  ReleasePyGIL& operator=(const ReleasePyGIL&) = delete;

 private:
  PyThreadState* thread_state_;
};

// PyObjectPtr is a gil-unaware smart-pointer for ::PyObject.
//
// Methods of this class make no effort to acquire the GIL. Moreover, all
// non-const methods (including the destructor, and excluding the release()
// method) expect that the caller holds the GIL already.
//
// The interface:
//
//   static Own(PyObject* ptr):
//     Returns a smart-pointer constructed from the given raw pointer to
//     PyObject instance *without* increasing the ref-counter.
//
//   static NewRef(PyObject* /*nullable*/ ptr):
//     Returns a smart-pointer constructed from the given raw pointer to
//     PyObject instance *with* increasing the ref-counter.
//
//   get():
//     Returns a raw pointer to the managed PyObject.
//
//   release():
//     Releases the managed object without decrementing the ref-counter.
//
//   reset()
//     Resets the state of the managed object.
//
//   The smart-pointer class is default constructable, movable and copyable.
//   It also supports comparison with nullptr.
//
class PyObjectPtr;

// PyObjectGILSafePtr is a GIL-safe version of PyObjectPtr.
//
// All methods of this class automatically acquire the GIL when needed.
//
// The class has the same interface as PyObjectPtr.
//
class PyObjectGILSafePtr;

namespace py_utils_internal {

struct PyObjectPtrTraits {
  struct GILGuardType {
    GILGuardType() { DCheckPyGIL(); }
  };
  using PyObjectType = PyObject;
  ABSL_ATTRIBUTE_ALWAYS_INLINE void inc_ref(PyObject* ptr) { Py_INCREF(ptr); }
  ABSL_ATTRIBUTE_ALWAYS_INLINE void dec_ref(PyObject* ptr) { Py_DECREF(ptr); }
};

struct PyObjectGILSafePtrTraits {
  using GILGuardType = AcquirePyGIL;
  using PyObjectType = PyObject;
  ABSL_ATTRIBUTE_ALWAYS_INLINE void inc_ref(PyObject* ptr) { Py_INCREF(ptr); }
  ABSL_ATTRIBUTE_ALWAYS_INLINE void dec_ref(PyObject* ptr) { Py_DECREF(ptr); }
};

}  // namespace py_utils_internal

// Definition of PyObject.
class PyObjectPtr final
    : public py_object_ptr_impl_internal::BasePyObjectPtr<
          PyObjectPtr, py_utils_internal::PyObjectPtrTraits> {
 public:
  using py_object_ptr_impl_internal::BasePyObjectPtr<
      PyObjectPtr, py_utils_internal::PyObjectPtrTraits>::BasePyObjectPtr;
};

// Definition of PyObject.
class PyObjectGILSafePtr final
    : public py_object_ptr_impl_internal::BasePyObjectPtr<
          PyObjectGILSafePtr, py_utils_internal::PyObjectGILSafePtrTraits> {
 public:
  using py_object_ptr_impl_internal::BasePyObjectPtr<
      PyObjectGILSafePtr,
      py_utils_internal::PyObjectGILSafePtrTraits>::BasePyObjectPtr;
};

// A cancellation scope for the Python environment.
//
// It's recommended to declare a cancellation scope as one of
// the early steps in a Python function:
//
//   PyObject* PyMyFunction(PyObject* self, PyObject* arg) {
//     DCheckPyGIL();
//     PyCancellationScope cancellation_scope;
//     ...
//   };
//
// If an active cancellation context exists for the current thread, it will be
// retained. Otherwise, if the computation occurs in the main Python thread,
// a cancellation context will be created based on SIGINT handling, similar to
// Python's KeyboardInterrupt handling.
class [[nodiscard]] PyCancellationScope {
 public:
  PyCancellationScope() noexcept;
  ~PyCancellationScope() noexcept;

 private:
  std::optional<CancellationContext::ScopeGuard> scope_;
};

// A wrapper for PyErr_Fetch, PyErr_NormalizeException, and
// PyException_SetTraceback that returns the Python exception
// (including the traceback) stored as a PyObjectPtr.
PyObjectPtr PyErr_FetchRaisedException();

// A wrapper for PyErr_Restore.
std::nullptr_t PyErr_RestoreRaisedException(PyObjectPtr py_exception);

// A wrapper for PyException_SetCause() and PyException_SetContext() that
// assigns both `__cause__` and `__context__`.
void PyException_SetCauseAndContext(
    PyObject* py_exception, PyObjectPtr /*nullable*/ py_exception_cause);

// Returns an attribute of a type's method/attribute through the MRO. This
// function doesn't trigger the attribute's descriptor (the `__get__` method).
//
// If an attribute is not found, this function only returns nullptr.
//
// Note: This method never raises any python exceptions.
PyObjectPtr PyType_LookupMemberOrNull(PyTypeObject* py_type,
                                      PyObject* py_str_attr);

// Returns `true` and initializes `result` to point to the items stored in
// `py_obj` if it is a tuple (or list); otherwise, returns `false`.
//
// Note: This method does not raise any Python exceptions.
bool PyTuple_AsSpan(PyObject* /*nullable*/ py_obj,
                    absl::Span<PyObject*>* result);

// Returns the attribute corresponding to the given "member" and "self".
//
// When you access a method through an object instance in Python, as in
//
//   bound_method = obj.method
//
// what you get is a `<bound method ...>`. You can invoke it like a function
// without additional arguments, using `bound_method()`, and it will retain its
// association with `obj`.
//
// This function helps to replicate this behavior: given a type member
// `type.method` and an instance `obj`, it creates
//
//   bound_method = bind_member(type.member, obj)
//
// which is equivalent to `obj.method`.
//
// (More generically, this function handles the python descriptors:
//  https://docs.python.org/3/howto/descriptor.html)
PyObjectPtr PyObject_BindMember(PyObjectPtr&& py_member, PyObject* self);

// Calls the "member" with the given arguments.
PyObjectPtr PyObject_CallMember(PyObjectPtr&& py_member, PyObject* self,
                                PyObject* args, PyObject* kwargs);

// Calls the "member" with the given arguments; args[0] has to be "self".
// See: PyObject_Vectorcall().
PyObjectPtr PyObject_VectorcallMember(PyObjectPtr&& py_member, PyObject** args,
                                      Py_ssize_t nargsf, PyObject* kwnames);

// Adds a note to the active exception.
//
// Important: This functions should be called only when there is an active
// exception, i.e. PyErr_Occurred() != nullptr.
std::nullptr_t PyErr_AddNote(absl::string_view note);

// An analogue of PyErr_Format() that makes it work like:
//
//   raise NewException(...) from active_exception
//
// Always returns nullptr for compatibility with PyErr_Format().
//
// Important: This functions should be called only when there is an active
// exception, i.e. PyErr_Occurred() != nullptr.
std::nullptr_t PyErr_FormatFromCause(PyObject* py_exc, const char* format, ...);

// Add (function_name, file_name, line) to the traceback of the active
// exception. Return true, if the operation was successful.
bool PyTraceback_Add(const char* function_name, const char* file_name,
                     int line);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_PY_UTILS_PY_UTILS_H_
