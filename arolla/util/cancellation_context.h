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
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "arolla/util/api.h"

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
class AROLLA_API CancellationContext {
 public:
  class [[nodiscard]] AROLLA_API ScopeGuard;

  // A platform-specific countdown period designed to make timer access overhead
  // negligible.
  static constexpr uint64_t kCountdownPeriod = 16;

  // Constructs a cancellation context with the given `cooldown_period`.
  explicit CancellationContext(absl::Duration cooldown_period) noexcept
      : cooldown_period_ns_(absl::ToInt64Nanoseconds(cooldown_period)) {
    cooldown_ns_ = absl::GetCurrentTimeNanos() + cooldown_period_ns_;
  }

  virtual ~CancellationContext() noexcept = default;

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
  ABSL_ATTRIBUTE_HOT inline bool SoftCheck(uint64_t decrement = 1) noexcept {
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
  ABSL_ATTRIBUTE_COLD bool Check() noexcept;

  // Returns the current status of the cancellation context, without doing
  // an actual check.
  const absl::Status& status() const noexcept { return status_; }

 protected:
  virtual absl::Status DoCheck() noexcept = 0;

 private:
  uint_fast16_t countdown_ = kCountdownPeriod;
  int64_t cooldown_ns_;
  const int64_t cooldown_period_ns_;
  absl::Status status_;
};

// Starts a scope where the given cancellation context is active.
// The context object must outlive the scope scope.
//
// IMPORTANT: The implementation uses thread_local storage.
class AROLLA_API [[nodiscard]] CancellationContext::ScopeGuard {
 public:
  explicit ScopeGuard(
      absl::Nullable<CancellationContext*> cancellation_context) noexcept
      : previous_cancellation_context_(std::exchange(
            active_cancellation_context_, cancellation_context)) {}

  ~ScopeGuard() noexcept {
    active_cancellation_context_ = previous_cancellation_context_;
  }

  // Disable copy and move semantics.
  ScopeGuard(const ScopeGuard&) = delete;
  ScopeGuard& operator=(const ScopeGuard&) = delete;

  // Returns the active cancellation context.
  static ABSL_ATTRIBUTE_HOT inline absl::Nullable<CancellationContext*>
  active_cancellation_context() noexcept {
    return active_cancellation_context_;
  }

 private:
  absl::Nullable<CancellationContext*> previous_cancellation_context_;

  static thread_local absl::Nullable<CancellationContext*>
      active_cancellation_context_;
};

// A convenience wrapper for `!active_cancellation_context->SoftCheck()`.
//
// IMPORTANT: The implementation uses thread_local storage.
ABSL_ATTRIBUTE_HOT inline bool IsCancelled(uint64_t decrement = 1) noexcept {
  auto* const cancellation_context =
      CancellationContext::ScopeGuard::active_cancellation_context();
  return cancellation_context != nullptr &&
         !cancellation_context->SoftCheck(decrement);
}

// A convenience wrapper for `active_cancellation_context()->SoftCheck()`;
// returns `active_cancellation_context->status()`.
//
// IMPORTANT: The implementation uses thread_local storage.
ABSL_ATTRIBUTE_HOT inline absl::Status CheckCancellation(
    uint64_t decrement = 1) noexcept {
  auto* const cancellation_context =
      CancellationContext::ScopeGuard::active_cancellation_context();
  if (cancellation_context == nullptr) {
    return absl::OkStatus();
  }
  if (cancellation_context->SoftCheck(decrement)) [[likely]] {
    return absl::OkStatus();
  }
  return cancellation_context->status();
}

}  // namespace arolla

#endif  // AROLLA_UTIL_CANCELLATION_CONTEXT_H_
