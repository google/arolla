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
#ifndef AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZATIONS_SHORT_CIRCUIT_WHERE_H_
#define AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZATIONS_SHORT_CIRCUIT_WHERE_H_

#include "absl/status/statusor.h"
#include "arolla/expr/optimization/peephole_optimizer.h"

namespace arolla::expr {

// Transforms "core.where" calls into "core._short_circuit_where" for scalar
// conditions. Eliminates on branch completely if the condition is known on
// compilation time.
absl::StatusOr<PeepholeOptimizationPack> ShortCircuitWhereOptimizations();

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZATIONS_SHORT_CIRCUIT_WHERE_H_
