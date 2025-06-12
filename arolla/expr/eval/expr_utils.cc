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
#include "arolla/expr/eval/expr_utils.h"

#include <functional>
#include <stack>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {

absl::StatusOr<ExprNodePtr> ExtractLambda(
    const ExprNodePtr& expr,
    std::function<absl::StatusOr<bool>(const ExprNodePtr&)> is_in_lambda) {
  struct Task {
    enum class Stage { kPreorder, kPostorder };

    ExprNodePtr node;
    Stage stage;
  };
  std::vector<ExprNodePtr> lambda_args;
  ExprOperatorSignature lambda_signature;
  absl::flat_hash_set<Fingerprint> previsited;
  absl::flat_hash_map<Fingerprint, ExprNodePtr> new_nodes;

  std::stack<Task> tasks;
  tasks.push(Task{.node = expr, .stage = Task::Stage::kPreorder});

  int next_placeholder = 0;

  while (!tasks.empty()) {
    auto [node, stage] = std::move(tasks.top());
    tasks.pop();

    if (stage == Task::Stage::kPreorder) {
      // NOTE: we can push the node to the stack several times if it occurs
      // several times in the expression. But it is important to process it for
      // the topmost stack entry because the kPostorder entries below rely on
      // it. So we just ignore the other stack entries if the node is already
      // processed.
      if (!previsited.insert(node->fingerprint()).second) {
        continue;
      }

      ASSIGN_OR_RETURN(bool in_lambda, is_in_lambda(node));
      if (in_lambda) {
        tasks.push(Task{.node = node, .stage = Task::Stage::kPostorder});
        // Pushing them to stack in reverse order so they are processed in the
        // natural order.
        for (auto dep = node->node_deps().rbegin();
             dep != node->node_deps().rend(); ++dep) {
          tasks.push(Task{.node = *dep, .stage = Task::Stage::kPreorder});
        }
      } else {
        auto [it, inserted] = new_nodes.insert({node->fingerprint(), nullptr});
        if (inserted) {
          it->second = Placeholder(absl::StrCat("_", next_placeholder++));
          lambda_args.emplace_back(node);
          lambda_signature.parameters.push_back(
              ExprOperatorSignature::Parameter{
                  .name = it->second->placeholder_key()});
        }
      }
    } else {
      std::vector<ExprNodePtr> new_deps;
      new_deps.reserve(node->node_deps().size());
      for (const auto& dep : node->node_deps()) {
        new_deps.push_back(new_nodes.at(dep->fingerprint()));
      }
      ASSIGN_OR_RETURN(new_nodes[node->fingerprint()],
                       WithNewDependencies(node, new_deps));
    }
  }

  ASSIGN_OR_RETURN(
      ExprOperatorPtr lambda,
      MakeLambdaOperator(lambda_signature, new_nodes.at(expr->fingerprint())));
  return MakeOpNode(lambda, lambda_args);
}

}  // namespace arolla::expr::eval_internal
