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
#include "py/arolla/py_utils/py_cancellation_controller.h"

#include <Python.h>
#include <signal.h>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <thread>  // NOLINT(build/c++11)
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/signal_safe_event.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python::py_cancellation_controller {
namespace {

// Python main thread flag.
//
// `AcquirePyCancellationContext()` returns a cancellation_context only for
// Python's main thread, matching `PyErr_CheckSignals()` semantics.
thread_local constinit bool is_python_main_thread = false;

// This singleton worker, executing in its own thread, is responsible for
// updating the `cancellation_context` for the Python main thread in response to
// SIGINT delivery.
//
// Updating the `cancellation_context` directly from the signal handler is not
// feasible, as it would require additional synchronization and memory
// allocations, which are unsafe in a signal handler context.
//
// The worker receives notifications from the signal handler via
// `SignalSafeEvent`.
//
class Worker final {
  struct PrivateConstructorTag {};

 public:
  explicit Worker(PrivateConstructorTag) {}

  // Returns `false` if the initialization failed.
  [[nodiscard]] static bool Init() {
    static bool ok = instance().InitOnce();
    return ok;
  }

  static CancellationContextPtr acquire_cancellation_context() {
    if (!is_python_main_thread) {
      return nullptr;
    }
    auto& self = instance();
    if (self.cancellation_context_->Cancelled()) [[unlikely]] {
      auto cancellation_context = CancellationContext::Make();
      absl::MutexLock lock(self.mutex_);
      using std::swap;
      swap(self.cancellation_context_, cancellation_context);
    }
    return self.cancellation_context_;
  }

  // This method is safe for use in a signal handler.
  static void asynchronous_notify() { instance().wakeup_.Notify(); }

  static void synchronous_notify() {
    auto& self = instance();
    CancellationContextPtr cancellation_context;
    {
      absl::MutexLock lock(self.mutex_);
      cancellation_context = self.cancellation_context_;
    }
    if (!cancellation_context->Cancelled()) {
      cancellation_context->Cancel(absl::CancelledError("interrupted"));
    }
  }

 private:
  static Worker& instance() {
    static absl::NoDestructor<Worker> result(PrivateConstructorTag{});
    return *result;
  }

  bool InitOnce() {
    // Block all signal handling within the worker thread by masking signals
    // before spawning it (the child thread inherits the signal mask) and then
    // restoring the caller's mask.
    //
    // This thread might be the only one not owned by Python in the process.
    // So, we try to gracefully step aside and let Python handle signals as if
    // this thread were not present.
    //
    // This is not strictly required -- there are likely many threads in
    // the process, some of which didn't block the signals and may not even
    // be aware of Python. The Python interpreter is designed to be okay with
    // that situation anyway.
    sigset_t mask;
    sigset_t old_mask;
    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, &old_mask);
    std::thread(&Worker::Loop, this).detach();
    pthread_sigmask(SIG_SETMASK, &old_mask, nullptr);
    return true;
  }

  ~Worker() = delete;

  void Loop() {
    for (;;) {
      wakeup_.WaitAndConsume();
      synchronous_notify();
    }
  }

  SignalSafeEvent wakeup_;

  // Note: Only the Python main thread can change the `cancellation_context_`
  // pointer.
  absl_nonnull CancellationContextPtr cancellation_context_ =
      CancellationContext::Make();
  absl::Mutex mutex_;
};

// Installs a signal handler for SIGINT.
//
// The handler supports forwarding control to the preceding handler.
// If third-party code overrides the handler, signals will not be received.
//
void InstallSignalHandler() {
  static void (*original_sig_handler_fn)(int signo) = nullptr;
  static void (*original_sig_action_fn)(int signo, siginfo_t* info,
                                        void* context) = nullptr;
  constexpr auto sig_action_fn = [](int signo, siginfo_t* info, void* context) {
    if (signo == SIGINT) {
      Worker::asynchronous_notify();
    }
    if (original_sig_handler_fn != nullptr) {
      original_sig_handler_fn(signo);
    } else if (original_sig_action_fn != nullptr) {
      original_sig_action_fn(signo, info, context);
    }
  };

  // We rely on the GIL to constrain activity in other threads, particularly
  // to prevent concurrent signal handler setup.
  DCheckPyGIL();

  // We expect to be on Python's main thread.
  CHECK(is_python_main_thread);

  static bool called_once = false;
  CHECK(!called_once) << "InstallSignalHandler must be called at most once!";
  called_once = true;

  // Collect information about the current SIGINT handler.
  struct sigaction original_action;
  if (sigaction(SIGINT, nullptr, &original_action) < 0) {
    constexpr auto message =
        "arolla::python::py_cancellation_controller::InstallSignalHandler: "
        "sigaction failed";
    int errno_copy = errno;
    LOG(ERROR) << message << ": " << strerror(errno_copy);
    PyErr_WarnEx(PyExc_RuntimeWarning, message, 0);
    return;
  }
  if (original_action.sa_handler == SIG_IGN ||
      original_action.sa_handler == SIG_DFL ||
      (original_action.sa_flags & SA_RESETHAND)) {
    // It looks like python interpreter installed no SIGINT handler. This is
    // unexpected, so we avoid installing our SIGINT handler too.
    constexpr auto message =
        "arolla::python::py_cancellation_controller::InstallSignalHandler: "
        "python interpreter installed no SIGINT handler; arolla follows its "
        "lead";
    LOG(ERROR) << message;
    PyErr_WarnEx(PyExc_RuntimeWarning, message, 0);
    return;
  }

  // Record pointer to the original handler (depends on SA_SIGINFO).
  if (original_action.sa_flags & SA_SIGINFO) {
    original_sig_action_fn = original_action.sa_sigaction;
  } else {
    original_sig_handler_fn = original_action.sa_handler;
  }

  // Install our signal handler reusing the flags of the previous handler.
  struct sigaction action = original_action;
  action.sa_flags |= SA_SIGINFO;
  action.sa_sigaction = sig_action_fn;
  struct sigaction previous_action;
  if (sigaction(SIGINT, &action, &previous_action) < 0) {
    constexpr auto message =
        "arolla::python::py_cancellation_controller::InstallSignalHandler: "
        "sigaction failed";
    int errno_copy = errno;
    LOG(ERROR) << message << ": " << strerror(errno_copy);
    PyErr_WarnEx(PyExc_RuntimeWarning, message, 0);
    return;
  }

  // Sanity check: ensure the previous handler was the one we expected.
  if (previous_action.sa_flags != original_action.sa_flags ||
      previous_action.sa_sigaction != original_action.sa_sigaction) {
    constexpr auto message =
        "arolla::python::py_cancellation_controller::InstallSignalHandler: "
        "signal handler has unexpectedly changed during installation of "
        "the new handler";
    LOG(ERROR) << message;
    PyErr_WarnEx(PyExc_RuntimeWarning, message, 0);
  }
}

// Note: Must be called on Python's main thread.
int InitOnce(void*) {
  DCheckPyGIL();
  is_python_main_thread = true;
  if (Worker::Init()) {
    InstallSignalHandler();
  }
  return 0;
}

}  // namespace

void Init() {
  DCheckPyGIL();
  static bool done = false;
  if (done) {
    return;
  }
  if (Py_AddPendingCall(InitOnce, nullptr) < 0) {
    LOG(ERROR) << "arolla::python::py_cancellation_controller::Init: "
               << "Py_AddPendingCall failed";
    PyErr_WarnEx(PyExc_RuntimeWarning,
                 "arolla::python::py_cancellation_controller::Init"
                 "Py_AddPendingCall failed",
                 0);
    return;
  }
  done = true;
}

absl_nullable CancellationContextPtr AcquirePyCancellationContext() {
  if (CancellationContext::ScopeGuard::has_active_cancellation_scope()) {
    return nullptr;
  }
  return Worker::acquire_cancellation_context();
}

void SimulateSIGINT() { Worker::synchronous_notify(); }

bool UnsafeOverrideSignalHandler() {
  DCheckPyGIL();
  if (!is_python_main_thread) {
    PyErr_SetString(PyExc_ValueError,
                    "unsafe_set_signal_handler only works in main thread");
    return false;
  }
  if (!Worker::Init()) {
    PyErr_SetString(PyExc_RuntimeWarning,
                    "py_cancellation_controller is non-functional");
    return false;
  }
  constexpr auto handler_fn = [](int signo) {
    if (signo == SIGINT) {
      Worker::asynchronous_notify();
    }
    PyErr_SetInterruptEx(signo);
  };
  struct sigaction action = {};
  action.sa_handler = handler_fn;
  action.sa_flags = SA_ONSTACK;
  if (sigaction(SIGINT, &action, nullptr) < 0) {
    int errno_copy = errno;
    PyErr_Format(PyExc_RuntimeError, "sigaction failed: %s",
                 strerror(errno_copy));
    return false;
  }
  return true;
}

}  // namespace arolla::python::py_cancellation_controller
