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

#ifndef AROLLA_USE_EVENTFD
#define AROLLA_USE_EVENTFD __has_include(<sys/eventfd.h>)
#endif

#if AROLLA_USE_EVENTFD
#include <sys/eventfd.h>
#else
#include <fcntl.h>
#endif  // AROLLA_USE_EVENTFD

#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"

namespace arolla {

SignalSafeEvent::SignalSafeEvent() {
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };
#if AROLLA_USE_EVENTFD
  fd_[0] = eventfd(0, EFD_CLOEXEC);
  PCHECK(fd_[0] != -1) << "arolla::SignalSafeEvent: eventfd() failed";
  fd_[1] = fd_[0];
#else
  PCHECK(pipe(fd_) == 0) << "arolla::SignalSafeEvent: pipe() failed";
  for (int fd : fd_) {
    int flags = fcntl(fd, F_GETFD);
    PCHECK(flags != -1 && fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != -1)
        << "arolla::SignalSafeEvent: fcntl() failed";
  }
#endif  // AROLLA_USE_EVENTFD
}

SignalSafeEvent::~SignalSafeEvent() {
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };
  // NOTE: In Linux, `close()` always releases the descriptor, even when it
  // reports EINTR, so a retry could close an unrelated descriptor that another
  // thread opened in the meantime. See CAVEATS in close(2).
  close(fd_[0]);
  if (fd_[1] != fd_[0]) {
    close(fd_[1]);
  }
}

void SignalSafeEvent::Notify() {
  if (state_.exchange(kPending, std::memory_order_seq_cst) != kWaiting) {
    return;  // Nobody is blocked; the state alone is enough.
  }
  absl::Cleanup restore_errno = [errno_value = errno] { errno = errno_value; };
  // NOTE: eventfd(2) requires writing an 8-byte integer (uint64_t).
  uint64_t val = 1;
  (void)write(fd_[1], &val, sizeof(val));
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
  // NOTE: eventfd(2) requires a buffer of at least 8 bytes, and for pipe(2) a
  // larger buffer drains multiple raced writes in a single wakeup.
  char buf[512];
  while (read(fd_[0], buf, sizeof(buf)) <= 0) {
    // A blocking read can only fail with EINTR here; anything else would
    // turn this loop into a spin.
    PCHECK(errno == EINTR) << "arolla::SignalSafeEvent: read() failed";
  }
  // Consume the event that unblocked us, together with any that arrived while
  // we were waking up.
  state_.store(kIdle, std::memory_order_seq_cst);
}

}  // namespace arolla
