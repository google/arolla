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
#include "arolla/expr/operator_loader/qtype_inference.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operator_loader/helper.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/operator_loader/qtype_constraint.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using expr::ExprNodePtr;
using expr::GetLeafKeys;
using expr::PopulateQTypes;
using expr::ToDebugString;

// Checks that the given expr returns QTYPE.
absl::StatusOr<ExprNodePtr> NormalizeQTypeInferenceExpr(ExprNodePtr expr) {
  ASSIGN_OR_RETURN(auto result, ReplacePlaceholdersWithLeaves(expr));
  absl::flat_hash_map<std::string, QTypePtr> leaf_qtypes;
  for (const auto& leaf_key : GetLeafKeys(result)) {
    leaf_qtypes[leaf_key] = GetQTypeQType();
  }
  const QType* output_qtype = nullptr;
  if (const auto annotated_expr = PopulateQTypes(result, leaf_qtypes);
      annotated_expr.ok()) {
    output_qtype = (*annotated_expr)->qtype();
  }
  if (output_qtype == GetQType<QTypePtr>()) {
    return result;
  }
  if (output_qtype == nullptr) {
    return absl::InvalidArgumentError(
        "Error while computing output QType of a QType inference expression: " +
        ToDebugString(expr));
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "expected a qtype inference expression to return %s, got %s: %s",
      GetQTypeQType()->name(), output_qtype->name(), ToDebugString(expr)));
}

}  // namespace

absl::StatusOr<QTypeInferenceFn> MakeQTypeInferenceFn(
    absl::Span<const QTypeConstraint> qtype_constraints,
    ExprNodePtr qtype_inference_expr) {
  ASSIGN_OR_RETURN(auto normalized_qtype_inference_expr,
                   NormalizeQTypeInferenceExpr(qtype_inference_expr));
  std::vector<std::string> required_args =
      GetLeafKeys(normalized_qtype_inference_expr);
  ASSIGN_OR_RETURN(auto qtype_constraint_fn,
                   MakeQTypeConstraintFn(qtype_constraints));
  ASSIGN_OR_RETURN(auto executor, MakeParameterQTypeModelExecutor(std::move(
                                      normalized_qtype_inference_expr)));
  return
      [qtype_constraint_fn = std::move(qtype_constraint_fn),
       executor = std::move(executor),
       qtype_inference_expr = std::move(qtype_inference_expr),
       required_args =
           std::move(required_args)](const ParameterQTypes& parameter_qtypes)
          -> absl::StatusOr<const QType* /*nullable*/> {
        ASSIGN_OR_RETURN(bool constraints_result,
                         qtype_constraint_fn(parameter_qtypes));
        if (!constraints_result) {
          return nullptr;
        }
        for (const std::string& name : required_args) {
          if (!parameter_qtypes.contains(name)) {
            return nullptr;
          }
        }
        ASSIGN_OR_RETURN(auto qtype_typed_value, executor(parameter_qtypes));
        DCHECK_EQ(
            qtype_typed_value.GetType(),  // It's safe because we has checked
            GetQTypeQType());  // output_qtype in NormalizeQTypeInferenceExpr.
        auto* qtype = qtype_typed_value.UnsafeAs<QTypePtr>();
        if (qtype == nullptr || qtype == GetNothingQType()) {
          return absl::InvalidArgumentError(absl::StrFormat(
              "qtype inference expression produced no qtype: %s, %s",
              ToDebugString(qtype_inference_expr),
              FormatParameterQTypes(parameter_qtypes)));
        }
        return qtype;
      };
}

}  // namespace arolla::operator_loader
