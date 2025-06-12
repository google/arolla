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
#include "arolla/qexpr/bound_operators.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/cancellation.h"

namespace arolla {
namespace {

template <bool kHasCancellationContext>
inline int64_t RunBoundOperatorsImpl(
    absl::Span<const std::unique_ptr<BoundOperator>> ops,
    EvaluationContext* ctx, FramePtr frame,
    [[maybe_unused]] CancellationContext* cancellation_context) {
  DCHECK_OK(ctx->status());
  DCHECK_EQ(ctx->requested_jump(), 0);
  DCHECK(!ctx->signal_received());
  for (size_t ip = 0; ip < ops.size(); ++ip) {
    ops[ip]->Run(ctx, frame);
    // NOTE: consider making signal_received a mask once we have more than two
    // signals.
    if (ctx->signal_received()) [[unlikely]] {
      if (!ctx->status().ok()) [[unlikely]] {
        return ip;
      }
      if (ctx->requested_jump() != 0) [[likely]] {
        ip += ctx->requested_jump();
        DCHECK_LT(ip, ops.size());
      }
      ctx->ResetSignals();
    }
    if constexpr (kHasCancellationContext) {
      if (cancellation_context->Cancelled()) [[unlikely]] {
        ctx->set_status(cancellation_context->GetStatus());
        return ip;
      }
    }
  }
  return ops.size() - 1;
}

}  // namespace

int64_t RunBoundOperators(absl::Span<const std::unique_ptr<BoundOperator>> ops,
                          EvaluationContext* ctx, FramePtr frame) {
  auto* cancellation_context =
      CancellationContext::ScopeGuard::current_cancellation_context();
  if (cancellation_context == nullptr) [[likely]] {
    return RunBoundOperatorsImpl<false>(ops, ctx, frame, cancellation_context);
  } else {
    return RunBoundOperatorsImpl<true>(ops, ctx, frame, cancellation_context);
  }
}

std::unique_ptr<BoundOperator> JumpBoundOperator(int64_t jump) {
  return MakeBoundOperator([=](EvaluationContext* ctx, FramePtr frame) {
    ctx->set_requested_jump(jump);
  });
}

std::unique_ptr<BoundOperator> JumpIfNotBoundOperator(
    FrameLayout::Slot<bool> cond_slot, int64_t jump) {
  return MakeBoundOperator([=](EvaluationContext* ctx, FramePtr frame) {
    if (!frame.Get(cond_slot)) {
      ctx->set_requested_jump(jump);
    }
  });
}

}  // namespace arolla
