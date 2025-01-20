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
#ifndef AROLLA_QEXPR_EVAL_CONTEXT_H_
#define AROLLA_QEXPR_EVAL_CONTEXT_H_

#include <cstdint>
#include <type_traits>
#include <utility>

#include "absl//functional/any_invocable.h"
#include "absl//log/check.h"
#include "absl//status/status.h"
#include "arolla/memory/raw_buffer_factory.h"

namespace arolla {

// EvaluationContext contains all the data QExpr operator may need in runtime.
class EvaluationContext {
 public:
  using CheckInterruptFn = absl::AnyInvocable<absl::Status()>;

  EvaluationContext() = default;
  explicit EvaluationContext(RawBufferFactory* buffer_factory,
                             CheckInterruptFn* check_interrupt_fn = nullptr)
      : buffer_factory_(*buffer_factory),
        check_interrupt_fn_(check_interrupt_fn) {
    DCHECK(buffer_factory);
  }

  EvaluationContext(const EvaluationContext&) = delete;
  EvaluationContext& operator=(const EvaluationContext&) = delete;

  // A status field that a function can use to report a failure when
  // `absl::Status` and `absl::StatusOr<T>` may not be used as a return type
  // (usually for performance reasons).
  //
  // The convention:
  //
  // * The caller SHOULD guarantee that ctx->status.ok() is true before passing
  //   context to a function. Please pay attention that "moved from" state
  //   is not ok.
  //
  // * If a function returns `absl::OkStatus()` (or value in `absl::StatusOr`),
  //   and `ctx->status.ok()` was true before the call, `ctx->status.ok()`
  //   SHALL be true after the call. If the function returns an error, it
  //   MAY also set `ctx->status` field.
  //
  const absl::Status& status() const& { return status_; }
  absl::Status&& status() && { return std::move(status_); }

  // Sets status to the context and raises the signal_received() flag if it
  // is not ok().
  //
  // Usage examples:
  //   ctx->set_status(ReturnsStatus());
  //   ctx->set_status(std::move(lvalue_status));
  //   ctx->set_status(absl::Status(const_lvalue_status));
  //
  // Can be also used inside ASSIGN_OR_RETURN macro:
  //   ASSIGN_OR_RETURN(
  //       auto value, ReturnsStatusOr(), ctx->set_status(std::move(_)));
  //
  template <typename StatusLike>
  auto set_status(StatusLike&& status_like) ->
      // Some template magic to prohibit passing lvalue Status and so unexpected
      // copying.
      std::enable_if_t<
          !std::is_lvalue_reference_v<StatusLike> &&
              !std::is_const_v<std::remove_reference_t<StatusLike>>,
          std::void_t<decltype(static_cast<absl::Status>(
              std::forward<StatusLike>(status_like)))>> {
    status_ = static_cast<absl::Status>(std::forward<StatusLike>(status_like));
    signal_received_ = signal_received_ || !status_.ok();
  }

  // StatusBuilder adapter that calls this->set_status(...).
  //
  // Usage example:
  //   RETURN_IF_ERROR(ReturnsStatus()).With(ctx->set_status());
  //
  auto set_status() {
    return [this](auto&& status_like) {
      this->set_status(std::forward<decltype(status_like)>(status_like));
    };
  }

  RawBufferFactory& buffer_factory() { return buffer_factory_; }

  // requested_jump tells the evaluation engine to jump by the given (positive
  // or negative) number of operators. One must take into account that the
  // instruction pointer is shifted by 1 after every instruction, so e.g. to
  // repeat the same operator requested_jump must be set to -1. The feature is
  // supported by dynamic eval, but may not be supported by other engines.
  int64_t requested_jump() const { return jump_; }
  void set_requested_jump(int64_t jump) {
    signal_received_ = true;
    jump_ = jump;
  }

  // The flag indicates to the evaluation engine that the linear evaluation flow
  // is interrupted and it must check `status` or `requested_jump` values. After
  // checking, the engine must reset the flag by calling ResetSignals().
  bool signal_received() const { return signal_received_; }

  // Resets status, requested_jump and signal_received.
  void ResetSignals() {
    signal_received_ = false;
    jump_ = 0;
    status_ = absl::OkStatus();
  }

  bool has_check_interrupt_fn() const { return check_interrupt_fn_ != nullptr; }
  void check_interrupt(int64_t ops_evaluated) {
    constexpr int64_t kCheckInterruptPeriod = 128;
    check_interrupt_counter_ += ops_evaluated;
    if (check_interrupt_counter_ >= kCheckInterruptPeriod &&
        check_interrupt_fn_ && status_.ok()) {
      set_status((*check_interrupt_fn_)());
      check_interrupt_counter_ = 0;
    }
  }

 private:
  bool signal_received_ = false;
  int64_t jump_ = 0;
  absl::Status status_;
  RawBufferFactory& buffer_factory_ = *GetHeapBufferFactory();  // Not owned.
  CheckInterruptFn* check_interrupt_fn_ = nullptr;
  int64_t check_interrupt_counter_ = 0;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_EVAL_CONTEXT_H_
