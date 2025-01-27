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
#ifndef AROLLA_UTIL_CANCELLATION_CONTEXT_H_
#define AROLLA_UTIL_CANCELLATION_CONTEXT_H_

#include <cstdint>
#include <memory>

#include "absl//functional/any_invocable.h"
#include "absl//status/status.h"
#include "absl//time/clock.h"
#include "absl//time/time.h"

namespace arolla {

// An interface for checking whether an operation has been cancelled.
//
// The `SoftCheck()` method is a rate-limited wrapper for `Check()`, designed to
// reduce overhead by skipping checks unless sufficient time (`cooldown_period`)
// has elapsed since the last one. Additionally, the `countdown_period`
// distributes the cost of accessing the clock across multiple `SoftCheck()`
// calls.
//
// IMPORTANT: The methods of `CancellationContext` are not thread-safe.
// To forward a cancellation signal to a different thread, you must create
// a specialized context.
//
class CancellationContext {
 public:
  // Factory function.
  //
  // NOTE: This factory function is primarily intended for prototyping. Directly
  // implementing the interface may help to reduce a fraction of the overhead.
  static std::unique_ptr<CancellationContext> Make(
      absl::Duration cooldown_period, int countdown_period,
      absl::AnyInvocable<absl::Status()> do_check_fn);

  // Base class constructor.
  CancellationContext(absl::Duration cooldown_period, int countdown_period)
      : countdown_(countdown_period),
        countdown_period_(countdown_period),
        cooldown_period_ns_(absl::ToInt64Nanoseconds(cooldown_period)) {
    cooldown_ns_ = absl::GetCurrentTimeNanos() + cooldown_period_ns_;
  }

  virtual ~CancellationContext() = default;

  // Disable copy and move semantics.
  CancellationContext(const CancellationContext&) = delete;
  CancellationContext& operator=(const CancellationContext&) = delete;

  // Returns `true` if the operation has not been canceled; otherwise,
  // returns `false` and updates `status` with the reason for cancellation.
  //
  // This is a rate-limited wrapper over Check().
  bool SoftCheck() {
    if (status_.ok()) [[likely]] {
      if (--countdown_ >= 0) [[likely]] {
        return true;
      }
      countdown_ = countdown_period_;
      auto now_ns = absl::GetCurrentTimeNanos();
      if (now_ns < cooldown_ns_) [[likely]] {
        return true;
      }
      cooldown_ns_ = absl::GetCurrentTimeNanos() + cooldown_period_ns_;
      return Check();
    }
    return false;
  }

  // Returns `true` if the operation has not been canceled; otherwise,
  // returns `false` and updates `status` with the reason for cancellation.
  bool Check() {
    if (status_.ok()) [[likely]] {
      status_ = DoCheck();
      return status_.ok();
    }
    return false;
  }

  const absl::Status& status() const { return status_; }

 protected:
  virtual absl::Status DoCheck() = 0;

 private:
  int countdown_;
  const int countdown_period_;
  int64_t cooldown_ns_;
  const int64_t cooldown_period_ns_;
  absl::Status status_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_CANCELLATION_CONTEXT_H_
