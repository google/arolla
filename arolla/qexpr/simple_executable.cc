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
#include "arolla/qexpr/simple_executable.h"

#include <memory>

#include "absl//status/status.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"

namespace arolla {

void SimpleBoundExpr::InitializeLiterals(EvaluationContext* ctx,
                                         FramePtr frame) const {
  RunBoundOperators(init_ops_, ctx, frame);
}
void SimpleBoundExpr::Execute(EvaluationContext* ctx, FramePtr frame) const {
  RunBoundOperators(eval_ops_, ctx, frame);
}

void CombinedBoundExpr::InitializeLiterals(EvaluationContext* ctx,
                                           FramePtr frame) const {
  for (const auto& e : subexprs_) {
    if (e->InitializeLiterals(ctx, frame); !ctx->status().ok()) {
      break;
    }
  }
}
void CombinedBoundExpr::Execute(EvaluationContext* ctx, FramePtr frame) const {
  for (const auto& e : subexprs_) {
    if (e->Execute(ctx, frame); !ctx->status().ok()) {
      break;
    }
  }
}

}  // namespace arolla
