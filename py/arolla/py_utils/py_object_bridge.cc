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
// Implementation notes:
//
// * The worker thread is created via the Python API (`threading.Thread`)
//   and operates as a standard Python thread. An `atexit` hook ensures
//   it is cleanly stopped and joined during shutdown.
//
// * Requests from a thread already holding the GIL are executed inline;
//   all other requests are queued for the worker.
//
// * Requests enqueued after the worker stops may leak: handling them requires
//   the GIL, which may no longer be safe to acquire.
//
#include "py/arolla/py_utils/py_object_bridge.h"

#include <Python.h>

#include <atomic>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {
namespace {

// Returns `true` if the current thread can safely call the Python C API.
bool CanUsePyAPI() {
#if PY_VERSION_HEX >= 0x030D0000
  return PyThreadState_GetUnchecked() != nullptr;
#else  // The same function under its pre-3.13 name.
  return _PyThreadState_UncheckedGet() != nullptr;
#endif
}

// Returns `true` if the current thread is attached to the main Python
// interpreter.
bool IsMainPyInterpreter() {
  DCHECK(CanUsePyAPI());
  return PyInterpreterState_Get() == PyInterpreterState_Main();
}

class PyObjectBridge {
 public:
  static PyObjectBridge& GetInstance() {
    static absl::NoDestructor<PyObjectBridge> instance;
    return *instance;
  }

  // NOTE: If called after the worker stopped, enqueued items may leak.
  void DispatchDecref(PyObjectPtr absl_nonnull py_obj) {
    if (CanUsePyAPI() && IsMainPyInterpreter()) {
      py_obj.reset();
      return;
    }
    absl::MutexLock lock(mx_);
    to_decref_.push_back(std::move(py_obj));
  }

  // NOTE: If called after the worker stopped, enqueued items may leak.
  void DispatchAction(PyObjectPtr absl_nonnull py_obj,
                      PyObjectHolder::Action&& action) {
    if (CanUsePyAPI() && IsMainPyInterpreter()) {
      RunAction(py_obj, std::move(action));
      return;
    }
    absl::MutexLock lock(mx_);
    to_execute_.push_back({std::move(py_obj), std::move(action)});
  }

  // Starts the worker thread and registers an `atexit` hook that stops it.
  // Should be called at most once, from the main interpreter.
  static void InitOnce() {
    constexpr absl::string_view kErrorPrefix =
        "arolla::python::PyObjectBridge::InitOnce: ";
    DCHECK(CanUsePyAPI() && IsMainPyInterpreter());
    static const auto kRunMethod = [](PyObject*, PyObject*) {
      GetInstance().Run();
      Py_RETURN_NONE;
    };
    static const auto kStopMethod = [](PyObject*, PyObject*) {
      GetInstance().Stop();
      Py_RETURN_NONE;
    };
    static PyMethodDef kRunMethodDef = {"py_object_bridge_run", kRunMethod,
                                        METH_NOARGS, nullptr};
    static PyMethodDef kStopMethodDef = {"py_object_bridge_stop", kStopMethod,
                                         METH_NOARGS, nullptr};
    auto py_globals = PyObjectPtr::Own(Py_BuildValue(
        "{s:N,s:N}", "run", PyCFunction_New(&kRunMethodDef, nullptr), "stop",
        PyCFunction_New(&kStopMethodDef, nullptr)));
    if (py_globals == nullptr) {
      PyErr_Print();
      LOG(FATAL) << kErrorPrefix << "Py_BuildValue() failed";
    }
    auto py_res = PyObjectPtr::Own(
        PyRun_String("import atexit, threading\n"
                     "thread = threading.Thread(\n"
                     "    target=run,\n"
                     "    name='arolla.py_utils.py_object_bridge_worker',\n"
                     "    daemon=True,\n"
                     ")\n"
                     "thread.start()\n"
                     "atexit.register(lambda: (stop(), thread.join()))\n",
                     Py_file_input, py_globals.get(), py_globals.get()));
    if (py_res == nullptr) {
      PyErr_Print();
      LOG(FATAL) << kErrorPrefix << "PyRun_String() failed";
    }
  }

 private:
  struct PendingAction {
    PyObjectPtr py_obj;
    PyObjectHolder::Action action;
  };

  static void RunAction(const PyObjectPtr& py_obj,
                        PyObjectHolder::Action&& action) {
    DCHECK(CanUsePyAPI());
    std::move(action)(py_obj);
    if (PyErr_Occurred()) {
      PyErr_Clear();
      if (PyErr_WarnEx(PyExc_RuntimeWarning,
                       "arolla::python::PyObjectBridge::RunAction: "
                       "clearing stale Python exception",
                       0) < 0) {
        PyErr_Clear();
      }
    }
  }

  // Runs the worker loop; returns once `Stop()` has been requested.
  void Run() {
    DCHECK(CanUsePyAPI());
    const auto has_work = [this]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mx_) {
      return stopped_ || !to_decref_.empty() || !to_execute_.empty();
    };
    bool stopped = false;
    while (!stopped) {
      std::vector<PyObjectPtr> to_decref;
      std::vector<PendingAction> to_execute;
      {
        ReleasePyGIL guard;
        absl::MutexLock lock(mx_, absl::Condition(&has_work));
        to_decref_.swap(to_decref);
        to_execute_.swap(to_execute);
        stopped = stopped_;
      }
      for (auto& pending : to_execute) {
        RunAction(pending.py_obj, std::move(pending.action));
      }
      // TODO: Consider delegating the destruction of Python
      // objects to the GC, to keep this worker loop light.
    }
  }

  // Signals the worker loop to stop.
  void Stop() {
    absl::MutexLock lock(mx_);
    stopped_ = true;
  }

  absl::Mutex mx_;
  std::vector<PyObjectPtr> to_decref_ ABSL_GUARDED_BY(mx_);
  std::vector<PendingAction> to_execute_ ABSL_GUARDED_BY(mx_);
  bool stopped_ ABSL_GUARDED_BY(mx_) = false;
};

}  // namespace

PyObjectHolder::PyObjectHolder() = default;

PyObjectHolder::PyObjectHolder(PyObjectPtr absl_nullable py_obj)
    : py_obj_(std::move(py_obj)) {
  DCHECK(CanUsePyAPI() && IsMainPyInterpreter())
      << "arolla::python::PyObjectHolder: "
         "must be called in the main interpreter";
}

PyObjectHolder::~PyObjectHolder() noexcept {
  if (py_obj_ != nullptr) {
    PyObjectBridge::GetInstance().DispatchDecref(std::move(py_obj_));
  }
}

PyObjectHolder::PyObjectHolder(PyObjectHolder&& other) noexcept
    : py_obj_(std::move(other.py_obj_)) {}

PyObjectHolder& PyObjectHolder::operator=(PyObjectHolder&& other) noexcept {
  if (this != &other) {
    if (py_obj_ != nullptr) {
      PyObjectBridge::GetInstance().DispatchDecref(std::move(py_obj_));
    }
    py_obj_ = std::move(other.py_obj_);
  }
  return *this;
}

const PyObjectPtr absl_nullable& PyObjectHolder::py_obj() const {
  return py_obj_;
}

void PyObjectHolder::DispatchAction(Action&& action) && {
  DCHECK(py_obj_ != nullptr);
  if (py_obj_ != nullptr) {
    PyObjectBridge::GetInstance().DispatchAction(std::move(py_obj_),
                                                 std::move(action));
  }
}

namespace py_object_bridge {

void Init() {
  DCHECK(CanUsePyAPI() && IsMainPyInterpreter())
      << "arolla::python::py_object_bridge::Init: "
         "must be called in the main interpreter";
  static std::atomic_flag once;
  if (!once.test_and_set(std::memory_order_relaxed)) {
    PyObjectBridge::InitOnce();
  }
}

}  // namespace py_object_bridge
}  // namespace arolla::python
