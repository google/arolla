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
#include "arolla/expr/eval/side_output.h"

#include <string>
#include <utility>

#include "absl//container/flat_hash_map.h"
#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_cat.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/operators/bootstrap_operators.h"
#include "arolla/io/slot_listener.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

absl::StatusOr<ExprWithSideOutputs> ExtractSideOutputs(ExprNodePtr expr) {
  ExprWithSideOutputs result;
  ASSIGN_OR_RETURN(
      result.expr,
      Transform(expr, [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        if (!IsExportAnnotation(node)) {
          return node;
        }
        DCHECK_GE(node->node_deps().size(), 2);  // Tested by IsExportAnnotation
        auto unwrapped_node = node->node_deps()[0];
        auto tag = ReadExportAnnotationTag(node);
        auto value_expr = ReadExportAnnotationValue(node);
        DCHECK_NE(unwrapped_node, nullptr);
        DCHECK_NE(value_expr, nullptr);
        if (auto [it, inserted] = result.side_outputs.emplace(tag, value_expr);
            !inserted) {
          return absl::FailedPreconditionError(absl::StrCat(
              "duplicated export name ", tag, ": ", GetDebugSnippet(value_expr),
              " vs ", GetDebugSnippet(it->second)));
        }
        return unwrapped_node;
      }));
  return result;
}

absl::StatusOr<absl::flat_hash_map<std::string, ExprNodePtr>>
PrepareSideOutputsForListener(
    const absl::flat_hash_map<std::string, ExprNodePtr>& side_outputs,
    const SlotListenerBase& slot_listener) {
  absl::flat_hash_map<std::string, ExprNodePtr> result;
  for (auto [name, expr] : side_outputs) {
    if (auto qtype = slot_listener.GetQTypeOf(name); qtype != nullptr) {
      // TODO: This casting happens regardless of the
      // allow_side_outputs_casting option.
      ASSIGN_OR_RETURN(expr, expr_operators::CoreCast(expr, Literal(qtype)));
    }
    result.emplace(name, std::move(expr));
  }
  return result;
}

}  // namespace arolla::expr
