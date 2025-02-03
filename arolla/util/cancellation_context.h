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

#include "absl/base/attributes.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace arolla {

// An interface for checking whether an operation has been cancelled.
//
// The `SoftCheck()` method is a rate-limited wrapper for `Check()`, designed to
// reduce overhead by skipping checks unless sufficient time (`cooldown_period`)
// has elapsed since the last one. Additionally, there is a `kCountdownPeriod`
// to distribute the cost of accessing the clock across multiple `SoftCheck()`
// calls.
//
// IMPORTANT: The methods of `CancellationContext` are not thread-safe.
// To forward a cancellation signal to a different thread, you must create
// a specialized context.
//
class CancellationContext {
 public:
  // A platform-specific countdown period designed to make timer access overhead
  // negligible.
  static constexpr uint64_t kCountdownPeriod = 16;

  // Factory function.
  //
  // NOTE: This factory function is primarily intended for
  // prototyping. Directly implementing the interface may help to
  // reduce a fraction of the overhead.
  static std::unique_ptr<CancellationContext> Make(
      absl::Duration cooldown_period,
      absl::AnyInvocable<absl::Status()> do_check_fn);

  // Base class constructor.
  explicit CancellationContext(absl::Duration cooldown_period)
      : cooldown_period_ns_(absl::ToInt64Nanoseconds(cooldown_period)) {
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
  //
  // `decrement` allows grouping multiple SoftCheck() calls into a single
  // SoftCheck(n) call, which can be more efficient, particularly allowing
  // moving the check out of performance-critical loops.
  ABSL_ATTRIBUTE_HOT inline bool SoftCheck(uint64_t decrement = 1) {
    if (countdown_ > decrement) [[likely]] {
      countdown_ -= decrement;
      return status_.ok();
    }
    countdown_ = kCountdownPeriod;
    auto now_ns = absl::GetCurrentTimeNanos();
    if (cooldown_ns_ > now_ns) [[likely]] {
      return status_.ok();
    }
    cooldown_ns_ = now_ns + cooldown_period_ns_;
    return Check();
  }

  // Returns `true` if the operation has not been cancelled; otherwise,
  // returns `false` and updates `status` with the reason for cancellation.
  ABSL_ATTRIBUTE_COLD bool Check();

  const absl::Status& status() const { return status_; }

 protected:
  virtual absl::Status DoCheck() = 0;

 private:
  uint_fast16_t countdown_ = kCountdownPeriod;
  int64_t cooldown_ns_;
  const int64_t cooldown_period_ns_;
  absl::Status status_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_CANCELLATION_CONTEXT_H_
