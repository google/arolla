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
#include <optional>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "arolla/decision_forest/expr_operator/decision_forest_operator.h"
#include "arolla/decision_forest/qexpr_operator/batched_operator.h"
#include "arolla/decision_forest/qexpr_operator/pointwise_operator.h"
#include "arolla/expr/eval/extensions.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::arolla::expr::eval_internal::CompileOperatorFnArgs;

std::optional<absl::Status> CompileDecisionForestOperator(
    const CompileOperatorFnArgs& args) {
  const auto* forest_op =
      dynamic_cast<const DecisionForestOperator*>(args.op.get());
  if (forest_op == nullptr) {
    return std::nullopt;
  }

  auto input_types = SlotsToTypes(args.input_slots);
  auto output_type = args.output_slot.GetType();
  auto forest_op_signature =
      QExprOperatorSignature::Get(input_types, output_type);

  if (!IsTupleQType(args.output_slot.GetType()) ||
      output_type->type_fields().empty()) {
    return absl::InternalError(
        absl::StrFormat("incorrectly deduced DecisionForest output type: %s",
                        output_type->name()));
  }
  bool is_pointwise =
      !IsArrayLikeQType(output_type->type_fields()[0].GetType());

  OperatorPtr op;
  if (is_pointwise) {
    ASSIGN_OR_RETURN(op, CreatePointwiseDecisionForestOperator(
                             forest_op->forest(), forest_op_signature,
                             forest_op->tree_filters()));
  } else {
    ASSIGN_OR_RETURN(op, CreateBatchedDecisionForestOperator(
                             forest_op->forest(), forest_op_signature,
                             forest_op->tree_filters()));
  }

  return args.executable_builder
      ->BindEvalOp(*op, args.input_slots, args.output_slot)
      .status();
}

AROLLA_INITIALIZER(
        .reverse_deps =
            {
                ::arolla::initializer_dep::kOperators,
                ::arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = [] {
          arolla::expr::eval_internal::CompilerExtensionRegistry::GetInstance()
              .RegisterCompileOperatorFn(CompileDecisionForestOperator);
        })

}  // namespace
}  // namespace arolla
