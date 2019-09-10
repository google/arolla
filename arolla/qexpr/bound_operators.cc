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
#include "arolla/qexpr/bound_operators.h"

#include <cstdint>
#include <memory>

#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"

namespace arolla {

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
