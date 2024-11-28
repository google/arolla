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
#ifndef AROLLA_EXPR_EVAL_CASTING_H_
#define AROLLA_EXPR_EVAL_CASTING_H_

#include "absl//status/statusor.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr_node.h"

namespace arolla::expr::eval_internal {

// The transformation inserts necessary casts to the expression. It expects the
// expression to be already lowered to the lowest level.
absl::StatusOr<ExprNodePtr> CastingTransformation(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr expr);

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_CASTING_H_
