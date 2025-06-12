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
#ifndef AROLLA_EXPR_EVAL_SIDE_OUTPUT_H_
#define AROLLA_EXPR_EVAL_SIDE_OUTPUT_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/io/slot_listener.h"

namespace arolla::expr {

struct ExprWithSideOutputs {
  ExprNodePtr expr;
  absl::flat_hash_map<std::string, ExprNodePtr> side_outputs;
};

// Extracts subexpressions annotated with `annotation.export` annotation into a
// separate map. Returns both expr and side outputs cleaned from export
// annotations.
// This function is useful to prevent exported nodes to be removed during
// compilation process. Operators in expressions generally assumes absent of
// side effects, so `core.get_first(core.make_tuple(x, annotation.export(y)))`
// will be transformed to just `x` during compilation.
absl::StatusOr<ExprWithSideOutputs> ExtractSideOutputs(ExprNodePtr expr);

// Filters named expressions, leaving only the ones mentioned in `types`.
// Inserts an optional type casting operator if the type of an expression does
// not match the requested.
absl::StatusOr<absl::flat_hash_map<std::string, ExprNodePtr>>
PrepareSideOutputsForListener(
    const absl::flat_hash_map<std::string, ExprNodePtr>& side_outputs,
    const SlotListenerBase& slot_listener);

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_SIDE_OUTPUT_H_
