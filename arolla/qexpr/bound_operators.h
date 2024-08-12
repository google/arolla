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
#ifndef AROLLA_QEXPR_BOUND_OPERATORS_H_
#define AROLLA_QEXPR_BOUND_OPERATORS_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

namespace internal {

template <bool CheckInterrupt>
inline int64_t RunBoundOperatorsImpl(
    absl::Span<const std::unique_ptr<BoundOperator>> ops,
    EvaluationContext* ctx, FramePtr frame) {
  DCHECK_OK(ctx->status());
  DCHECK_EQ(ctx->requested_jump(), 0);
  DCHECK(!ctx->signal_received());
  size_t ip = 0;
  size_t last_check_interrupt_ip = 0;
  for (; ip < ops.size(); ++ip) {
    ops[ip]->Run(ctx, frame);
    // NOTE: consider making signal_received a mask once we have more than two
    // signals.
    if (ABSL_PREDICT_FALSE(ctx->signal_received())) {
      if constexpr (CheckInterrupt) {
        ctx->check_interrupt(ip - last_check_interrupt_ip + 1);
      }
      if (ctx->requested_jump() != 0) {
        ip += ctx->requested_jump();
        DCHECK_LT(ip, ops.size());
        if constexpr (CheckInterrupt) {
          last_check_interrupt_ip = ip + 1;
        }
      }
      if (!ctx->status().ok()) {
        return ip;
      }
      ctx->ResetSignals();
    }
  }
  if constexpr (CheckInterrupt) {
    ctx->check_interrupt(ip - last_check_interrupt_ip);
  }
  return ip - 1;
}
}  // namespace internal

// Runs a sequence of bound operators with associated display names for each op.
// Returns the index of the operator at which the execution stops. This would be
// the operator which failed if the execution returned an error,
// or the last operator if the execution finished successfully.
inline int64_t RunBoundOperators(
    absl::Span<const std::unique_ptr<BoundOperator>> ops,
    EvaluationContext* ctx, FramePtr frame) {
  return ctx->has_check_interrupt_fn()
             ? internal::RunBoundOperatorsImpl<true>(ops, ctx, frame)
             : internal::RunBoundOperatorsImpl<false>(ops, ctx, frame);
}

// Implementation of BoundOperator interface based on the provided functor.
template <typename Functor>
class FunctorBoundOperator final : public BoundOperator {
 public:
  explicit FunctorBoundOperator(Functor functor)
      : functor_(std::move(functor)) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const final {
    static_assert(std::is_same_v<void, decltype(functor_(ctx, frame))>,
                  "functor(ctx, frame) must return void");
    functor_(ctx, frame);
  }

 public:
  Functor functor_;
};

template <typename Functor>
FunctorBoundOperator(Functor) -> FunctorBoundOperator<Functor>;

// Create a bound operator implemented by the provided functor.
template <typename Functor>
std::unique_ptr<BoundOperator> MakeBoundOperator(Functor functor) {
  // By explicit creation of unique_ptr to the base class we avoid instantiation
  // of std::unique_ptr<FunctorBoundOperator<Functor>>. As this code is intended
  // to be used by many arolla operators (each with a different Functor),
  // reduction of the application binary file size is significant.
  return std::unique_ptr<BoundOperator>(
      new FunctorBoundOperator<Functor>(std::move(functor)));
}

// Bound operator that resets a target value to its initial state.
class ResetBoundOperator : public BoundOperator {
 public:
  explicit ResetBoundOperator(TypedSlot target_slot)
      : target_slot_(target_slot) {}

  void Run(EvaluationContext*, FramePtr frame) const override {
    target_slot_.Reset(frame);
  }

 private:
  TypedSlot target_slot_;
};

// An operator that unconditionally jumps by `jump` steps.
std::unique_ptr<BoundOperator> JumpBoundOperator(int64_t jump);

// An operator that jumps by `jump` steps if cond_slot contains missing.
std::unique_ptr<BoundOperator> JumpIfNotBoundOperator(
    FrameLayout::Slot<bool> cond_slot, int64_t jump);

// BoundOperator which sets `output_presence_slot` to the logical-and of
// the given `cond_slots`, and if this value is true, additionally invokes
// the provided `true_op`.
//
// This bound operator allows bound operators accepting a set of non-optional
// operands to be applied on a mix of optional and non-optional operands.
template <typename TrueOp>
class WhereAllBoundOperator : public BoundOperator {
 public:
  WhereAllBoundOperator(absl::Span<const FrameLayout::Slot<bool>> cond_slots,
                        FrameLayout::Slot<bool> output_presence_slot,
                        TrueOp true_op)
      : cond_slots_(cond_slots.begin(), cond_slots.end()),
        output_presence_slot_(output_presence_slot),
        true_op_(std::move(true_op)) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    if (std::all_of(cond_slots_.begin(), cond_slots_.end(),
                    [frame](FrameLayout::Slot<bool> slot) {
                      return frame.Get(slot);
                    })) {
      frame.Set(output_presence_slot_, true);
      true_op_.Run(ctx, frame);
    } else {
      frame.Set(output_presence_slot_, false);
    }
  }

 private:
  absl::InlinedVector<FrameLayout::Slot<bool>, 4> cond_slots_;
  FrameLayout::Slot<bool> output_presence_slot_;
  TrueOp true_op_;
};

// deduction guide for WhereAllBoundOperator.
template <typename TrueOp>
WhereAllBoundOperator(absl::Span<const FrameLayout::Slot<bool>>,
                      TrueOp) -> WhereAllBoundOperator<TrueOp>;

}  // namespace arolla

#endif  // AROLLA_QEXPR_BOUND_OPERATORS_H_
