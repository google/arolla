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
#ifndef AROLLA_EXPR_EVAL_COMPILE_WHERE_OPERATOR_H_
#define AROLLA_EXPR_EVAL_COMPILE_WHERE_OPERATOR_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/dynamic_compiled_operator.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/executable_builder.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {

// The lower version of WhereOp that stores both precompiled branches inside.
// The operator is designed to exist only during compilation. The operator
// accepts `1 + true_arg_count() + false_arg_count()` arguments. The first one
// must be the condition `where` operator. Other arguments are passed to the
// branches correspondingly.
// TODO: Extract a base class for operators with fixed QType and no
// ToLower.
class PackedWhereOp final : public BuiltinExprOperatorTag,
                            public ExprOperatorWithFixedSignature {
  struct PrivateConstructorTag {};

 public:
  static absl::StatusOr<ExprOperatorPtr> Create(
      DynamicCompiledOperator true_op, DynamicCompiledOperator false_op);

  PackedWhereOp(PrivateConstructorTag, DynamicCompiledOperator true_op,
                DynamicCompiledOperator false_op);

  const DynamicCompiledOperator& true_op() const { return true_op_; }
  const DynamicCompiledOperator& false_op() const { return false_op_; }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final;

 private:
  DynamicCompiledOperator true_op_;
  DynamicCompiledOperator false_op_;
};

// Converts where operators in expression to PackedWhere, hiding their branches
// from leaves-to-root compilation.
absl::StatusOr<ExprNodePtr> WhereOperatorGlobalTransformation(
    const DynamicEvaluationEngineOptions& options, ExprNodePtr node);

// Compiles PackedWhere operator into a sequence of init and bound operators.
// input_slots should correspond to where_op.leaf_keys(). Returns a slot for the
// operator result.
absl::StatusOr<TypedSlot> CompileWhereOperator(
    const DynamicEvaluationEngineOptions& options,
    const PackedWhereOp& where_op, absl::Span<const TypedSlot> input_slots,
    TypedSlot output_slot,
    eval_internal::ExecutableBuilder* executable_builder);

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_COMPILE_WHERE_OPERATOR_H_
