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
#ifndef AROLLA_EXPR_EVAL_EVAL_H_
#define AROLLA_EXPR_EVAL_EVAL_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/optimization/optimizer.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr {

struct DynamicEvaluationEngineOptions {
  struct PreparationStage {
    static constexpr uint64_t kAll = ~uint64_t{0};

    static constexpr uint64_t kPopulateQTypes = 1 << 0;
    static constexpr uint64_t kToLower = 1 << 1;
    static constexpr uint64_t kLiteralFolding = 1 << 2;
    static constexpr uint64_t kStripAnnotations = 1 << 3;
    static constexpr uint64_t kBackendCompatibilityCasting = 1 << 4;
    static constexpr uint64_t kOptimization = 1 << 5;
    static constexpr uint64_t kExtensions = 1 << 6;
    static constexpr uint64_t kWhereOperatorsTransformation = 1 << 7;
  };
  // A mask of model preparation stages to perform before binding the operators.
  // For a general usage all the stages are mandatory, but the internal
  // implementation can skip some that are surely not needed in the context.
  uint64_t enabled_preparation_stages = PreparationStage::kAll;

  // Populate the init_op_descriptions() / eval_op_descriptions() in the
  // generated DynamicBoundExpr. Use it for debug/testing only.
  bool collect_op_descriptions = false;

  // An function to apply optimizations to the expression on each iteration of
  // preprocessing. The function must keep the expression unchanged if no
  // optimizations can be applied, otherwise the compiler can fail due to
  // exceeded limit on the preprocessing iterations.
  // If missing, no optimizations will be applied.
  std::optional<Optimizer> optimizer = std::nullopt;

  // If true, the compiled expression can override the input slots once it does
  // not need them anymore. Use this option only when the compiled expression is
  // the only (or last) reader of the input slots.
  bool allow_overriding_input_slots = false;

  // QExpr operator directory to use. Defaults to the global operator registry.
  // If other value is specified, it must remain valid until objects generated
  // by DynamicEvaluationEngine are bound by a CompiledExpr::Bind call.
  const OperatorDirectory* operator_directory = nullptr;

  // If set to true, will track node transformations
  // during expression compilation, map them to BoundOperators during binding,
  // and output the detailed trace if an error is thrown during evaluation.
  bool enable_expr_stack_trace = true;
};

// Compiles the given expression for dynamic evaluation. The expression must not
// contain placeholders, all the leaves must be either type annotated, or
// mentioned in input_types.
// If `side_outputs` are provided, the resulting CompiledExpr will evaluate them
// unconditionally.
absl::StatusOr<std::unique_ptr<CompiledExpr>> CompileForDynamicEvaluation(
    const DynamicEvaluationEngineOptions& options, const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types = {},
    const absl::flat_hash_map<std::string, ExprNodePtr>& side_outputs = {});

// Compiles the given expression for dynamic evaluation and binds it to the
// frame layout.
absl::StatusOr<std::unique_ptr<BoundExpr>> CompileAndBindForDynamicEvaluation(
    const DynamicEvaluationEngineOptions& options,
    FrameLayout::Builder* layout_builder, const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    std::optional<TypedSlot> output_slot = {},
    const absl::flat_hash_map<std::string, ExprNodePtr>& side_outputs = {});

// Compiles and binds an expr operator, returning BoundExpr that evaluates
// it on the given slots.
absl::StatusOr<std::shared_ptr<BoundExpr>> CompileAndBindExprOperator(
    const DynamicEvaluationEngineOptions& options,
    FrameLayout::Builder* layout_builder, const ExprOperatorPtr& op,
    absl::Span<const TypedSlot> input_slots,
    std::optional<TypedSlot> output_slot = {});

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_EVAL_H_
