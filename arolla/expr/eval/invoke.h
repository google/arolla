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
#ifndef AROLLA_EXPR_EVAL_INVOKE_H_
#define AROLLA_EXPR_EVAL_INVOKE_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::expr {

// Compiles and invokes an expression on given inputs using QExpr backend.
//
// Do not use this function in computationally intensive situations because
// expression compilation is expensive.
absl::StatusOr<TypedValue> Invoke(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, TypedValue>& leaf_values,
    DynamicEvaluationEngineOptions options = DynamicEvaluationEngineOptions());

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_INVOKE_H_
