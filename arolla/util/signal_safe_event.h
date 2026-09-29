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
#ifndef AROLLA_UTIL_SIGNAL_SAFE_EVENT_H_
#define AROLLA_UTIL_SIGNAL_SAFE_EVENT_H_

#include <atomic>

namespace arolla {

// A single-consumer, multiple-producer event.
//
// Unlike `arolla::PermanentEvent`, `absl::Notification` and `absl::CondVar`,
// this class is async-signal-safe, i.e., a notification is safe to be posted
// from a signal handler.
//
// Events do not accumulate: `WaitAndConsume()` consumes every event posted
// before it returns. Crucially, it does not block if an event was posted since
// the previous `WaitAndConsume()` returned, which keeps the usual "drain the
// queue, then wait" loop free of lost wakeups without any extra locking:
//
//     // Producer:                       // Consumer:
//     Enqueue(item);                     for (;;) {
//     event.Notify();                      DrainQueue();
//                                          event.WaitAndConsume();
//                                        }
//
// Thread safety: `Notify()` may be called concurrently from any number of
// threads and from signal handlers. `WaitAndConsume()` and `TryConsume()` are
// for a single consumer: at most one thread may call them at a time.
//
// The caller must ensure that no thread is blocked in `WaitAndConsume()` when
// the instance is destroyed.
//
class SignalSafeEvent {
 public:
  SignalSafeEvent();
  ~SignalSafeEvent();

  // Disallow copy and move.
  SignalSafeEvent(const SignalSafeEvent&) = delete;
  SignalSafeEvent& operator=(const SignalSafeEvent&) = delete;

  // Posts an event, unblocking the consumer if it is waiting.
  //
  // Async-signal-safe: takes no locks and allocates nothing.
  void Notify();

  // Blocks until an event is pending, then consumes it.
  //
  // Returns immediately if an event is already pending.
  void WaitAndConsume();

  // Consumes a pending event if there is one, and reports whether there was.
  //
  // Never blocks. This exists so that a consumer can avoid the cost of
  // preparing to block (e.g., releasing a lock) when an event is already
  // pending.
  [[nodiscard]] bool TryConsume();

 private:
  enum State : int {
    // No pending event, and no blocked consumer.
    kIdle = 0,
    // An event is pending.
    kPending = 1,
    // The consumer is blocked, or about to block, on `event_fd_`.
    kWaiting = 2,
  };

  std::atomic<State> state_{kIdle};
  int event_fd_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_SIGNAL_SAFE_EVENT_H_
