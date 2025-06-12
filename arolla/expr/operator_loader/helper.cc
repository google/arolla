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
#include "arolla/expr/operator_loader/helper.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"

namespace arolla::operator_loader {

absl::StatusOr<expr::ExprNodePtr> ReplacePlaceholdersWithLeaves(
    const expr::ExprNodePtr& expr) {
  const auto leave_keys = expr::GetLeafKeys(expr);
  if (!leave_keys.empty()) {
    return absl::InvalidArgumentError("expected no leaf nodes, found: L." +
                                      absl::StrJoin(leave_keys, ", L."));
  }
  return expr::Transform(
      expr, [](expr::ExprNodePtr node) -> absl::StatusOr<expr::ExprNodePtr> {
        if (node->is_placeholder()) {
          return expr::Leaf(node->placeholder_key());
        }
        return node;
      });
}

}  // namespace arolla::operator_loader
