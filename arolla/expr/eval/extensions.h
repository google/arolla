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
#ifndef AROLLA_EXPR_EVAL_EXTENSIONS_H_
#define AROLLA_EXPR_EVAL_EXTENSIONS_H_

#include <functional>
#include <optional>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/prepare_expression.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {

// Arguments to CompileOperatorFn.
struct CompileOperatorFnArgs {
  const DynamicEvaluationEngineOptions& options;
  const ExprOperatorPtr& decayed_op;
  const ExprNodePtr& node;
  absl::Span<const TypedSlot> input_slots;
  TypedSlot output_slot;
  ExecutableBuilder* executable_builder;
};

// Callback to compile an operator into executable builder. The function must
// return one of:
//   - std::nullopt - if the given operator is not supported. In this case the
//     executable_builder must be left untouched.
//   - OkStatus — if the given operator is supported and was compiled
//     suffessfully.
//   - error — if the given operator is supported, but compilation failed.
using CompileOperatorFn = std::function<std::optional<absl::Status>(
    const CompileOperatorFnArgs& args)>;

// A set of compiler extensions.
struct CompilerExtensionSet {
  // Function to transform the expression during the preparation stage, combines
  // all the registered NodeTransformationFns.
  NodeTransformationFn node_transformation_fn;
  // Function to compile operators into an ExecutableExpr, combines all the
  // registered CompileOperatorFns.
  CompileOperatorFn compile_operator_fn;
};

// Global registry of NodeTransformationFn's and CompileOperatorFn's, that will
// be applied on every compilation.
class CompilerExtensionRegistry {
 public:
  // Constructs an empty registry. Use GetInstance instead.
  CompilerExtensionRegistry() = default;

  // Get the singletone.
  static CompilerExtensionRegistry& GetInstance();

  // Get the set of registered extensions.
  CompilerExtensionSet GetCompilerExtensionSet() const;

  // Register a callback to prepare a node for compilation. See
  // NodeTransformationFn doc.
  void RegisterNodeTransformationFn(NodeTransformationFn fn);

  // Register a callback to compile a node for compilation. See
  // CompileOperatorFn doc.
  void RegisterCompileOperatorFn(CompileOperatorFn fn);

 private:
  mutable absl::Mutex mutex_;
  std::vector<NodeTransformationFn> node_transformation_fns_
      ABSL_GUARDED_BY(mutex_);
  std::vector<CompileOperatorFn> compile_operator_fns_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_EXTENSIONS_H_
