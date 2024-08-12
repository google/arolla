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

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/raw_buffer_factory.h"

namespace arolla {

// RootEvaluationContext is an EvaluationContext that also creates and owns
// MemoryAllocation for a provided FrameLayout. For convenience it also provides
// shortcuts for the allocation frame's Get/GetMutable/Set methods.
//
// Usage example:
// Given an AddOperator defined as:
//
//   class AddOperator {
//    public:
//     AddOperator(FrameLayout::Slot<double> op1_slot,
//                 FrameLayout::Slot<double> op2_slot,
//                 FrameLayout::Slot<double> result_slot) :
//       op1_slot_(op1_slot), op2_slot_(op2_slot), result_slot_(result_slot) {}
//
//     void operator()(EvaluationContext* ctx, FramePtr frame) const {
//       double op1 = frame.Get(op1_slot_);
//       double op2 = frame.Get(op2_slot_);
//       frame.Set(result_slot_, op1 + op2);
//     }
//    private:
//     FrameLayout::Slot<double> op1_slot_;
//     FrameLayout::Slot<double> op2_slot_;
//     FrameLayout::Slot<double> result_slot_;
//   };
//
// We can construct a memory layout, and construct instances of the
// AddOperator as:
//
//   FrameLayout::Builder bldr;
//   auto op1_slot = bldr.AddSlot<double>();
//   auto op2_slot = bldr.AddSlot<double>();
//   auto tmp_slot = bldr.AddSlot<double>();
//   AddOperator add_op1(op1_slot, op2_slot, tmp_slot);
//   auto op3_slot = bldr.AddSlot<double>();
//   auto result_slot = bldr.AddSlot<double>();
//   AddOperator add_op2(tmp_slot, op3_slot, result_slot);
//   FrameLayout layout = std::move(bldr).Build();
//
// Then, in order to evaluate the expression, we create an EvaluationContext,
// populate the inputs, and invoke the operators in their reverse dependency
// order:
//
//   RootEvaluationContext ctx(&layout);
//   ctx.Set(op1_slot, 1.0);
//   ctx.Set(op2_slot, 2.0);
//   ctx.Set(op3_slot, 3.0);
//   add_op1(&ctx);                         // ctx.tmp = ctx.op1 + ctx.op2
//   add_op2(&ctx);                         // ctx.result = ctx.tmp + ctx.op3
//   double result = ctx.Get(result_slot);  // 6.0
//
// This becomes more interesting when we are working with a large DAG of
// operators, in which the result of one operator becomes the input to another,
// and we can simply invoke the operators in the appropriate sequence to
// compute the final results.
//
// Note: Take care to index an EvaluationContext object using only Slots created
// with same FrameLayout::Builder which was used to create the
// EvaluationContext's FrameLayout.
//
class RootEvaluationContext {
 public:
  // Constructs an empty (IsValid() == false) evaluation context.
  RootEvaluationContext() : buffer_factory_(GetHeapBufferFactory()) {}

  // Construct EvaluationContext with the provided FrameLayout. The provided
  // layout must remain valid for the lifetime of this EvaluationContext.
  // buffer_factory, if not null, must remain valid for the lifetime of this
  // EvaluationContext. If null will be set to HeapBufferFactory.
  explicit RootEvaluationContext(const FrameLayout* layout,
                                 RawBufferFactory* buffer_factory = nullptr)
      : alloc_(layout),
        buffer_factory_(buffer_factory == nullptr ? GetHeapBufferFactory()
                                                  : buffer_factory) {}

  RootEvaluationContext(const RootEvaluationContext&) = delete;
  RootEvaluationContext& operator=(const RootEvaluationContext&) = delete;

  RootEvaluationContext(RootEvaluationContext&&) = default;
  RootEvaluationContext& operator=(RootEvaluationContext&&) = default;

  // Gets a mutable pointer to value in given slot. Behavior is undefined
  // if the slot does not match the FrameLayout used to create this
  // EvaluationContext.
  template <typename T>
  T* GetMutable(FrameLayout::Slot<T> slot) {
    return frame().GetMutable(slot);
  }

  // Sets value in given slot. Behavior is undefined if the slot does not match
  // the FrameLayout used to create this EvaluationContext.
  template <typename T, typename S = T>
  void Set(FrameLayout::Slot<T> slot, S&& value) {
    return frame().Set(slot, std::forward<S>(value));
  }

  // Gets value from given slot. Behavior is undefined if the slot does not
  // match the FrameLayout used to create this EvaluationContext.
  template <typename T>
  const T& Get(FrameLayout::Slot<T> slot) const {
    return frame().Get(slot);
  }

  FramePtr frame() { return alloc_.frame(); }
  ConstFramePtr frame() const { return alloc_.frame(); }
  RawBufferFactory& buffer_factory() const { return *buffer_factory_; }

  bool IsValid() const { return alloc_.IsValid(); }

 private:
  MemoryAllocation alloc_;
  RawBufferFactory* buffer_factory_ = nullptr;  // Not owned.
};

// EvaluationContext contains all the data QExpr operator may need in runtime.
class EvaluationContext {
 public:
  using CheckInterruptFn = absl::AnyInvocable<absl::Status()>;

  EvaluationContext() = default;
  explicit EvaluationContext(RootEvaluationContext& root_ctx)
      : buffer_factory_(root_ctx.buffer_factory()) {}
  explicit EvaluationContext(RawBufferFactory* buffer_factory,
                             CheckInterruptFn* check_interrupt_fn = nullptr)
      : buffer_factory_(*buffer_factory),
        check_interrupt_fn_(check_interrupt_fn) {
    DCHECK(buffer_factory);
  }

  EvaluationContext(const EvaluationContext&) = delete;
  EvaluationContext& operator=(const EvaluationContext&) = delete;
  EvaluationContext(EvaluationContext&&) = delete;
  EvaluationContext& operator=(EvaluationContext&&) = delete;

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
  //   ASSIGN_OR_RETURN(auto value, ReturnsStatusOr(), ctx->set_status(_));
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

  bool has_check_interrupt_fn() const {
    return check_interrupt_fn_ != nullptr;
  }
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
