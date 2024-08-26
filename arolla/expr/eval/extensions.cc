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
#include "arolla/expr/eval/extensions.h"

#include <optional>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr::eval_internal {

CompilerExtensionRegistry& CompilerExtensionRegistry::GetInstance() {
  static absl::NoDestructor<CompilerExtensionRegistry> instance;
  return *instance;
}

CompilerExtensionSet CompilerExtensionRegistry::GetCompilerExtensionSet()
    const {
  absl::ReaderMutexLock lock(&mutex_);

  return CompilerExtensionSet{
      .node_transformation_fn =
          [node_transformation_fns = node_transformation_fns_](
              const DynamicEvaluationEngineOptions& options,
              ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
        for (const auto& fn : node_transformation_fns) {
          ASSIGN_OR_RETURN(auto new_node, fn(options, node));
          if (new_node->fingerprint() != node->fingerprint()) {
            return new_node;
          }
          node = std::move(new_node);
        }
        return node;
      },
      .compile_operator_fn = [compile_operator_fns = compile_operator_fns_](
                                 const CompileOperatorFnArgs& args)
          -> std::optional<absl::Status> {
        for (const auto& fn : compile_operator_fns) {
          std::optional<absl::Status> result = fn(args);
          if (result.has_value()) {
            return result;
          }
        }
        return std::nullopt;
      }};
}

void CompilerExtensionRegistry::RegisterNodeTransformationFn(
    NodeTransformationFn fn) {
  absl::MutexLock lock(&mutex_);
  node_transformation_fns_.push_back(fn);
}

void CompilerExtensionRegistry::RegisterCompileOperatorFn(
    CompileOperatorFn fn) {
  absl::MutexLock lock(&mutex_);
  compile_operator_fns_.push_back(fn);
}

}  // namespace arolla::expr::eval_internal
