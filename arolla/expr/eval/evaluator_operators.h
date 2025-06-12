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
#ifndef AROLLA_EXPR_EVAL_EVALUATOR_OPERATORS_H_
#define AROLLA_EXPR_EVAL_EVALUATOR_OPERATORS_H_

#include <memory>
#include <utility>

#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"

namespace arolla::expr {

// Bound operator that executes the provided BoundExpr.
class ExecutingBoundOperator : public BoundOperator {
 public:
  explicit ExecutingBoundOperator(std::shared_ptr<const BoundExpr> executable)
      : executable_(std::move(executable)) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    executable_->Execute(ctx, frame);
  }

 private:
  std::shared_ptr<const BoundExpr> executable_;
};

// Bound operator that calls InitializeLiterals of the provided BoundExpr.
class InitializeAstLiteralsBoundOperator : public BoundOperator {
 public:
  explicit InitializeAstLiteralsBoundOperator(
      std::shared_ptr<const BoundExpr> executable)
      : executable_(std::move(executable)) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    executable_->InitializeLiterals(ctx, frame);
  }

 private:
  std::shared_ptr<const BoundExpr> executable_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_EVALUATOR_OPERATORS_H_
