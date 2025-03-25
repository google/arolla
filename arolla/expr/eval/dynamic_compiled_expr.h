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
#ifndef AROLLA_EXPR_EVAL_DYNAMIC_COMPILED_EXPR_H_
#define AROLLA_EXPR_EVAL_DYNAMIC_COMPILED_EXPR_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/eval/expr_stack_trace.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::eval_internal {

// BoundExpr implementation for dynamic evaluation.
class DynamicBoundExpr : public BoundExpr {
 public:
  using BoundExpr::BoundExpr;

  // Descriptions of the operations performed during init stage.
  virtual absl::Span<const std::string> init_op_descriptions() const = 0;

  // Descriptions of the operations performed during eval stage.
  virtual absl::Span<const std::string> eval_op_descriptions() const = 0;
};

// CompiledExpr implementation for dynamic evaluation.
class DynamicCompiledExpr : public CompiledExpr {
 public:
  // Constructs CompiledExpr for `prepared_expr`.
  //
  // NOTE: The function is internal and there are quite a few assumptions about
  // the arguments. `prepared_expr` must be preprocessed via
  // PrepareExpression(). If the expression contains side outputs, its root must
  // be InternalRootOperator and all its arguments except the first one must
  // correspond to side_output_names. `types` must contain deduced types for
  // each node in `prepared_expr`.
  //
  DynamicCompiledExpr(
      DynamicEvaluationEngineOptions options,
      absl::flat_hash_map<std::string, QTypePtr> input_types,
      QTypePtr output_type,
      absl::flat_hash_map<std::string, QTypePtr> named_output_types,
      ExprNodePtr prepared_expr, std::vector<std::string> side_output_names,
      absl::flat_hash_map<Fingerprint, QTypePtr> types,
      BoundExprStackTraceFactory stack_trace_factory = nullptr);

  absl::StatusOr<std::unique_ptr<BoundExpr>> Bind(
      FrameLayout::Builder* layout_builder,
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      std::optional<TypedSlot> output_slot) const final;

  // Appends the expression init/eval operators to the given ExecutableBuilder.
  absl::Status BindToExecutableBuilder(
      ExecutableBuilder& executable_builder,
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      TypedSlot output_slot) const;

 private:
  DynamicEvaluationEngineOptions options_;
  ExprNodePtr prepared_expr_;
  std::vector<std::string> side_output_names_;
  absl::flat_hash_map<Fingerprint, QTypePtr> types_;
  BoundExprStackTraceFactory stack_trace_factory_;  // Can be nullptr.
};

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_DYNAMIC_COMPILED_EXPR_H_
