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
#include "arolla/expr/optimization/default/default_optimizer.h"

#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/statusor.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/expr/optimization/peephole_optimizations/arithmetic.h"
#include "arolla/expr/optimization/peephole_optimizations/bool.h"
#include "arolla/expr/optimization/peephole_optimizations/const_with_shape.h"
#include "arolla/expr/optimization/peephole_optimizations/dict.h"
#include "arolla/expr/optimization/peephole_optimizations/presence.h"
#include "arolla/expr/optimization/peephole_optimizations/reduce.h"
#include "arolla/expr/optimization/peephole_optimizations/short_circuit_where.h"
#include "arolla/expr/optimization/peephole_optimizations/tuple.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

// Optimizer performing the default set of optimizations.
absl::StatusOr<Optimizer> DefaultOptimizer() {
  static const absl::NoDestructor<absl::StatusOr<Optimizer>> optimizer(
      []() -> absl::StatusOr<Optimizer> {
        ASSIGN_OR_RETURN(auto peephole_optimizer,
                         CreatePeepholeOptimizer({
                             ArithmeticOptimizations,
                             DictOptimizations,
                             PresenceOptimizations,
                             BoolOptimizations,
                             ReduceOptimizations,
                             TupleOptimizations,
                             ConstWithShapeOptimizations,
                             ShortCircuitWhereOptimizations,
                         }));
        return MakeOptimizer(std::move(peephole_optimizer));
      }());
  return *optimizer;
}

absl::StatusOr<Optimizer> CodegenOptimizer() {
  static const absl::NoDestructor<absl::StatusOr<Optimizer>> optimizer(
      []() -> absl::StatusOr<Optimizer> {
        ASSIGN_OR_RETURN(auto peephole_optimizer,
                         CreatePeepholeOptimizer({
                             ArithmeticOptimizations,
                             DictOptimizations,
                             PresenceOptimizations,
                             CodegenPresenceOptimizations,
                             BoolOptimizations,
                             ReduceOptimizations,
                             ConstWithShapeOptimizations,
                             TupleOptimizations,
                         }));
        return MakeOptimizer(std::move(peephole_optimizer));
      }());
  return *optimizer;
}

}  // namespace arolla::expr
