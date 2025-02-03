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
#ifndef AROLLA_EXPR_OPTIMIZATION_OPTIMIZER_H_
#define AROLLA_EXPR_OPTIMIZATION_OPTIMIZER_H_

#include <functional>
#include <memory>

#include "absl/status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"

namespace arolla::expr {

// A function performing optimizations on the expression.
using Optimizer = std::function<absl::StatusOr<ExprNodePtr>(ExprNodePtr)>;

// Creates Optimizer from the given PeepholeOptimizer. The PeepholeOptimizer can
// be applied several times during one optimizer invocation.
Optimizer MakeOptimizer(std::unique_ptr<PeepholeOptimizer> peephole_optimizer);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_OPTIMIZER_H_
