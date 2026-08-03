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
#include "py/arolla/experimental/_tasks/python_callback_bridge.h"

#include <Python.h>

#include <cstddef>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/gil.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "pybind11/warnings.h"

namespace arolla::python {

namespace py = pybind11;

// The internal implementation of `PythonCallbackBridge`.
//
// This class owns a thread and manages the lifetime of python callbacks. Upon
// destruction of the public `PythonCallbackBridge` object, this class waits for
// the remaining callbacks and then stops the worker thread.
//
// NOTE: This class has an internal mutex `mx_`, and to prevent potential
// deadlocks, it never calls any Python C API while holding the mutex
// (including manipulating object's reference counters).
class PythonCallbackBridge::Impl {
 public:
  class Handle;
  using CallbackId = void*;

  ~Impl() { DCHECK(registry_.empty()); }

  // Runs the worker loop.
  void Run() {
    constexpr auto has_work = [](Impl* self) -> bool {
      self->mx_.AssertReaderHeld();
      return self->closed_ || !self->to_remove_.empty() ||
             !self->to_execute_.empty();
    };
    while (true) {
      std::vector<py::handle> to_remove;
      py::object next_py_cb;
      {  // Read pending actions (e.g. callbacks to execute or remove).
        absl::MutexLock lock(mx_);
        if (closed_) {
          return;
        }
        for (py::handle py_cb : to_remove_) {
          Unregister(py_cb);
        }
        to_remove_.swap(to_remove);
        if (!to_execute_.empty()) {
          next_py_cb = py::reinterpret_steal<py::object>(to_execute_.front());
          to_execute_.pop_front();
          Unregister(next_py_cb);
        }
      }
      if (to_remove.empty() && !next_py_cb) {  // No work to do, wait.
        py::gil_scoped_release release;
        absl::MutexLock lock(mx_);
        mx_.Await(absl::Condition(+has_work, this));
        continue;
      }
      // Execute pending actions.
      for (py::handle py_cb : to_remove) {
        py_cb.dec_ref();
      }
      if (next_py_cb && PyObject_CallNoArgs(next_py_cb.ptr()) == nullptr) {
        PyErr_Clear();
        PyErr_WarnEx(PyExc_RuntimeWarning,
                     "[PythonCallbackBridge] unhandled exception in callback",
                     0);
      }
    }
  }

  // Releases all callbacks registered with this bridge and signals the worker
  // loop to stop.
  //
  // The handles connected to this bridge can safely outlive this call, but
  // they will no longer execute callbacks.
  void Close() {
    DCheckPyGIL();
    Registry registry;
    {
      absl::MutexLock lock(mx_);
      closed_ = true;
      to_execute_.clear();
      to_remove_.clear();
      registry_.swap(registry);
    }
    for (auto& [py_cb, count] : registry) {
      for (size_t i = 0; i < count; ++i) {
        Py_DECREF(py_cb);
      }
    }
  }

 private:
  void Register(py::handle py_cb) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mx_) {
    ++registry_[py_cb.ptr()];
  }

  void Unregister(py::handle py_cb) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mx_) {
    auto it = registry_.find(py_cb.ptr());
    DCHECK(it != registry_.end());
    if (it != registry_.end() && --it->second == 0) {
      registry_.erase(it);
    }
  }

  absl::Mutex mx_;

  bool closed_ ABSL_GUARDED_BY(mx_) = false;

  // NOTE: Since there is no `absl::flat_hash_multimap`, we use
  // `absl::flat_hash_map` and a counter to support registering the same
  // callback multiple times.
  using Registry = absl::flat_hash_map<PyObject*, size_t>;
  Registry registry_ ABSL_GUARDED_BY(mx_);

  std::vector<py::handle> to_remove_ ABSL_GUARDED_BY(mx_);
  std::deque<py::handle> to_execute_ ABSL_GUARDED_BY(mx_);
};

class PythonCallbackBridge::Impl::Handle {
 public:
  Handle(py::function py_cb,
         const std::shared_ptr<PythonCallbackBridge::Impl> absl_nonnull& impl) {
    DCheckPyGIL();
    absl::MutexLock lock(impl->mx_);
    if (!impl->closed_) {
      py_cb_ = py_cb;
      weak_impl_ = impl;
      impl->Register(py_cb.release());
    }
  }

  ~Handle() {
    if (py_cb_) {
      if (auto impl = weak_impl_.lock()) {
        absl::MutexLock lock(impl->mx_);
        if (!impl->closed_) {
          impl->to_remove_.push_back(py_cb_);
        }
      }
    }
  }

  Handle(Handle&& other) {
    std::swap(py_cb_, other.py_cb_);
    std::swap(weak_impl_, other.weak_impl_);
  }

  Handle& operator=(Handle&& other) {
    Handle tmp = std::move(*this);
    std::swap(py_cb_, other.py_cb_);
    std::swap(weak_impl_, other.weak_impl_);
    return *this;
  }

  void operator()() && {
    if (py_cb_) {
      if (auto impl = weak_impl_.lock()) {
        absl::MutexLock lock(impl->mx_);
        if (!impl->closed_) {
          impl->to_execute_.push_back(py_cb_);
        }
      }
    }
    py_cb_ = {};
    weak_impl_.reset();
  }

 private:
  py::handle py_cb_;
  std::weak_ptr<Impl> weak_impl_;
};

PythonCallbackBridge::PythonCallbackBridge() {
  auto impl = std::make_shared<Impl>();
  py::cpp_function worker_fn = [impl] { impl->Run(); };
  impl_ = std::move(impl);
  worker_thread_ = py::module_::import("threading")
                       .attr("Thread")(py::arg("target") = std::move(worker_fn),
                                       py::arg("daemon") = true);
  worker_thread_.attr("start")();
}

PythonCallbackBridge::~PythonCallbackBridge() {
  if (impl_) {
    impl_->Close();
  }
}

void PythonCallbackBridge::Close() {
  if (impl_) {
    impl_->Close();
    impl_ = {};
  }
  if (worker_thread_) {
    worker_thread_.attr("join")();
    worker_thread_ = {};
  }
}

absl::AnyInvocable<void() &&> PythonCallbackBridge::WrapPythonCallback(
    py::function py_cb) {
  if (impl_) {
    return Impl::Handle(std::move(py_cb), impl_);
  }
  return [] {};
}

}  // namespace arolla::python
