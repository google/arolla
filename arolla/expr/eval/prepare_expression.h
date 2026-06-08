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
#ifndef AROLLA_EXPR_EVAL_PREPARE_EXPRESSION_H_
#define AROLLA_EXPR_EVAL_PREPARE_EXPRESSION_H_

// Tools to prepare expression to compilation. The file is not intended to be
// used directly by the users.

#include <functional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/eval.h"
#include "arolla/expr/eval/expr_stack_trace.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::eval_internal {

// Callback to prepare a node for compilation. Must either return the node
// untouched, or transform it to a state supported by the QExpr compiler.
//
// The `decayed_op` parameter is guaranteed to be
// `DecayRegisteredOperator(node->op())`.
using NodeTransformationFn =
    std::function<absl::StatusOr<ExprNodePtr absl_nonnull>(
        const DynamicEvaluationEngineOptions& options,
        const ExprNodePtr absl_nonnull& node,
        const ExprOperatorPtr absl_nullable& decayed_op)>;

// Prepares expression for compilation. The resulting expression is at lowest
// level, with all the optimizations applied.
absl::StatusOr<ExprNodePtr> PrepareExpression(
    const ExprNodePtr& expr,
    const absl::flat_hash_map<std::string, QTypePtr>& input_types,
    const DynamicEvaluationEngineOptions& options,
    ExprStackTrace* absl_nullable stack_trace = nullptr);

// Operator accepting any number of arguments and returning the
// first one.
// This operator can be applied before PreparedExpression and has a special
// handling in DynamicEvaluationEngine and EvalVisitor in order to process
// independent from root side outputs.
// The operator behavior is similar to core.get_first(core.make_tuple(*args)).
// The only difference that DynamicEvaluationEngine is not creating a tuple and
// any additional slots for computations.
// The operator is only supposed to be used as "fake" root of the expression.
const ExprOperatorPtr& InternalRootOperator();

// Saves node QTypes into resulting_types and strips the qtype annotations.
//
// NOTE: This function expects that `prepared_expr` contains no annotations
// other than qtype annotations.
absl::StatusOr<ExprNodePtr> ExtractQTypesForCompilation(
    const ExprNodePtr& prepared_expr,
    absl::flat_hash_map<Fingerprint, QTypePtr>* resulting_types,
    ExprStackTrace* absl_nullable stack_trace = nullptr);

// Saves node QTypes into resulting_types and strips the qtype annotations.
//
// Similar to the function above, but accepts PostOrder instead of ExprNodePtr.
absl::StatusOr<ExprNodePtr> ExtractQTypesForCompilation(
    const PostOrder& prepared_post_order,
    absl::flat_hash_map<Fingerprint, QTypePtr>* resulting_types,
    ExprStackTrace* absl_nullable stack_trace = nullptr);

// Finds types for the side outputs.
// If side_output_names is empty, returns and empty map.
// Otherwise assumes that prepared_expr has
// InternalRootOperator and side_output_names.size() + 1
// dependencies. node_deps()[i + 1] correspond to side_output_names[i].
absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>>
LookupNamedOutputTypes(
    const ExprNodePtr& prepared_expr,
    const std::vector<std::string>& side_output_names,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& node_types);

// Looks up QTypes for the node in the map.
absl::StatusOr<QTypePtr> LookupQType(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& types);

// Looks up QTypes for all the expression leaves in the map.
absl::StatusOr<absl::flat_hash_map<std::string, QTypePtr>> LookupLeafQTypes(
    absl::Span<const std::string> leaf_keys,
    const absl::flat_hash_map<Fingerprint, QTypePtr>& types);

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_PREPARE_EXPRESSION_H_
