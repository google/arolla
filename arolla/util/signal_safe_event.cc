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
#include "arolla/util/signal_safe_event.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"

namespace arolla {

SignalSafeEvent::SignalSafeEvent() {
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };
  event_fd_ = eventfd(0, EFD_CLOEXEC);
  PCHECK(event_fd_ != -1) << "arolla::SignalSafeEvent: eventfd() failed";
}

SignalSafeEvent::~SignalSafeEvent() {
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };
  // NOTE: In Linux, `close()` always releases the descriptor, even when it
  // reports EINTR, so a retry could close an unrelated descriptor that another
  // thread opened in the meantime. See CAVEATS in close(2).
  close(event_fd_);
}

void SignalSafeEvent::Notify() {
  if (state_.exchange(kPending, std::memory_order_seq_cst) != kWaiting) {
    return;  // Nobody is blocked; the state alone is enough.
  }
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };
  // NOTE: eventfd(2) requires writing exactly an 8-byte integer (uint64_t).
  uint64_t val = 1;
  (void)write(event_fd_, &val, sizeof(val));
}

bool SignalSafeEvent::TryConsume() {
  return state_.exchange(kIdle, std::memory_order_seq_cst) == kPending;
}

void SignalSafeEvent::WaitAndConsume() {
  // Announce that we are about to block. Since `kWaiting` only exists while
  // inside `WaitAndConsume()`, `state_` on entry is either `kIdle` or
  // `kPending`; if it is `kPending`, consume it and return without blocking.
  State expected = kIdle;
  if (!state_.compare_exchange_strong(expected, kWaiting,
                                      std::memory_order_seq_cst,
                                      std::memory_order_seq_cst)) {
    state_.store(kIdle, std::memory_order_seq_cst);
    return;
  }
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };

  // NOTE: eventfd(2) requires reading exactly an 8-byte integer (uint64_t).
  uint64_t val;
  while (read(event_fd_, &val, sizeof(val)) !=
         static_cast<ssize_t>(sizeof(val))) {
    // A blocking eventfd can only fail with EINTR here; anything else would
    // turn this loop into a spin.
    PCHECK(errno == EINTR) << "arolla::SignalSafeEvent: read() failed";
  }
  // Consume the event that unblocked us, together with any that arrived while
  // we were waking up.
  state_.store(kIdle, std::memory_order_seq_cst);
}

}  // namespace arolla
