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
#ifndef AROLLA_EXPR_EVAL_COMPILE_WHILE_OPERATOR_H_
#define AROLLA_EXPR_EVAL_COMPILE_WHILE_OPERATOR_H_

#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/operators/while_loop/while_loop.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr::eval_internal {

// Compiles WhileLoopOperator into the executable_builder.
absl::Status CompileWhileOperator(
    const DynamicEvaluationEngineOptions& options,
    const expr_operators::WhileLoopOperator& while_op,
    absl::Span<const TypedSlot> input_slots, TypedSlot output_slot,
    ExecutableBuilder& executable_builder);

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_COMPILE_WHILE_OPERATOR_H_
