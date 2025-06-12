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
#include "arolla/expr/optimization/peephole_optimizations/tuple.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

// Optimizes `get_nth[i](make_tuple(..., ith_expr, ...))` to `ith_expr`.
absl::StatusOr<ExprNodePtr> OptimizeTupleGet(ExprNodePtr expr) {
  static Fingerprint make_tuple_fingerprint = MakeTupleOperator().fingerprint();
  if (!expr->is_op()) {
    return expr;
  }
  auto get_nth_operator =
      fast_dynamic_downcast_final<const GetNthOperator*>(expr->op().get());
  if (get_nth_operator == nullptr) {
    return expr;
  }
  if (expr->node_deps().size() != 1) {
    return expr;
  }
  auto tuple_expr = expr->node_deps()[0];
  if (!tuple_expr->is_op()) {
    return expr;
  }
  ASSIGN_OR_RETURN(auto tuple_op, DecayRegisteredOperator(tuple_expr->op()));
  if (tuple_op->fingerprint() != make_tuple_fingerprint ||
      tuple_expr->node_deps().size() <= get_nth_operator->index()) {
    return expr;
  }
  return tuple_expr->node_deps()[get_nth_operator->index()];
}

// Optimizes get_n(make_tuple(...)) operations.
absl::Status AppendGetNOptimizations(PeepholeOptimizationPack& optimizations) {
  ASSIGN_OR_RETURN(
      optimizations.emplace_back(),
      PeepholeOptimization::CreateTransformOptimization(OptimizeTupleGet));
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<PeepholeOptimizationPack> TupleOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(AppendGetNOptimizations(optimizations));
  return optimizations;
}

}  // namespace arolla::expr
