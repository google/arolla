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
#ifndef AROLLA_EXPR_OPTIMIZATION_DEFAULT_DEFAULT_OPTIMIZER_H_
#define AROLLA_EXPR_OPTIMIZATION_DEFAULT_DEFAULT_OPTIMIZER_H_

#include "absl/status/statusor.h"
#include "arolla/expr/optimization/optimizer.h"

namespace arolla::expr {

// Optimizer performing the default set of optimizations.
absl::StatusOr<Optimizer> DefaultOptimizer();

// Optimizer performing the default set of optimizations for codegeneration.
absl::StatusOr<Optimizer> CodegenOptimizer();

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_DEFAULT_DEFAULT_OPTIMIZER_H_
