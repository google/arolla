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
#ifndef AROLLA_UTIL_PERMANENT_EVENT_H_
#define AROLLA_UTIL_PERMANENT_EVENT_H_

#include <atomic>

#include "absl/base/nullability.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "arolla/util/refcount_ptr.h"

namespace arolla {

// `PermanentEvent` manages a flag that can be set and waited on.
// All methods of this class are thread-safe.
//
// This class is modeled after `absl::Notification`, but it's safe to call
// `Notify()` multiple times (unlike `absl::Notification::Notify()`). In this
// regard, it behaves much like `threading.Event` in Python.
class PermanentEvent final : public RefcountedBase {
  struct PrivateConstructorTag {};

 public:
  // Factory function.
  static RefcountPtr<PermanentEvent> /*absl_nonnull*/ Make() {
    return RefcountPtr<PermanentEvent>::Make(PrivateConstructorTag{});
  }

  explicit PermanentEvent(PrivateConstructorTag) {}

  // Disallow copy and move.
  PermanentEvent(const PermanentEvent&) = delete;
  PermanentEvent& operator=(const PermanentEvent&) = delete;

  // Returns `true` if the internal flag has been set.
  [[nodiscard]] bool HasBeenNotified() const {
    return notification_.HasBeenNotified();
  }

  // Sets the internal flag to `true` and wakes any waiting threads.
  void Notify() {
    if (!once_.test_and_set(std::memory_order_relaxed)) {
      notification_.Notify();
    }
  }

  // Blocks until the internal flag has been set. Returns immediately if the
  // internal flag is already set.
  void Wait() const { notification_.WaitForNotification(); }

  // Blocks until the internal flag has been set or the specified deadline has
  // passed.
  //
  // Returns `true` if the internal flag was set; otherwise, returns `false`
  // indicating a timeout.
  [[nodiscard]] bool WaitWithDeadline(absl::Time deadline) const {
    return notification_.WaitForNotificationWithDeadline(deadline);
  }

 private:
  std::atomic_flag once_ = ATOMIC_FLAG_INIT;
  absl::Notification notification_;
};

using PermanentEventPtr = RefcountPtr<PermanentEvent>;

}  // namespace arolla

#endif  // AROLLA_UTIL_PERMANENT_EVENT_H_
