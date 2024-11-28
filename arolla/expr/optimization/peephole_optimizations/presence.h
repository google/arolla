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
#ifndef AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZATIONS_PRESENCE_H_
#define AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZATIONS_PRESENCE_H_

#include "absl//status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"

namespace arolla::expr {

namespace presence_impl {

// Returns true if expr type is presence type.
// E.g., DenseArray<Unit> or OptionalUnit
bool IsPresenceType(const ExprNodePtr& expr);

// Returns true if expr is always present.
// E.g., has non optional type (int32 or float32)
// or being Literal(OptionalValue<int32_t>(5.0f)).
bool IsAlwaysPresent(const ExprNodePtr& expr);
}  // namespace presence_impl

// Optimizations for `core.has`, `core.presence_*`, `core._to_optional` and
// other operations related to presence.
absl::StatusOr<PeepholeOptimizationPack> PresenceOptimizations();

// Additional presence optimizations that are useful in case of absence of
// shorcircuit where optimizations.
absl::StatusOr<PeepholeOptimizationPack> CodegenPresenceOptimizations();

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_OPTIMIZATION_PEEPHOLE_OPTIMIZATIONS_PRESENCE_H_
