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
#include "arolla/expr/operator_loader/qtype_constraint.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/eval/thread_safe_model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/operator_loader/helper.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::arolla::expr::BindOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::GetLeafKeys;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeTupleOperator;
using ::arolla::expr::PopulateQTypes;
using ::arolla::expr::ToDebugString;

// Replaces placeholders with leaves and constructs an addition expr checking
// that all required arguments are present. Returns the pair
// <predicate, presence> of two exprs with OptionalUnit output.
absl::StatusOr<std::pair<ExprNodePtr, ExprNodePtr>>
PreprocessQTypeConstraint(ExprNodePtr expr) {
  ExprNodePtr nothing_literal = Literal(GetNothingQType());
  ASSIGN_OR_RETURN(auto predicate_expr, ReplacePlaceholdersWithLeaves(expr));
  ExprNodePtr presence_expr = nullptr;
  absl::flat_hash_map<std::string, QTypePtr> leaf_qtypes;
  for (const auto& leaf_key : GetLeafKeys(predicate_expr)) {
    leaf_qtypes[leaf_key] = GetQTypeQType();
    ASSIGN_OR_RETURN(auto arg_is_present,
                     expr::CallOp("core.not_equal",
                                  {nothing_literal, expr::Leaf(leaf_key)}));
    if (presence_expr == nullptr) {
      presence_expr = std::move(arg_is_present);
    } else {
      ASSIGN_OR_RETURN(
          presence_expr,
          expr::CallOp("core.presence_and",
                       {std::move(presence_expr), std::move(arg_is_present)}));
    }
  }
  if (presence_expr == nullptr) {
    presence_expr = Literal(OptionalUnit(kUnit));
  }
  auto deduce_output_qtype =
      [&leaf_qtypes](const ExprNodePtr& e) -> const QType* {
    if (auto annotated_expr = PopulateQTypes(e, leaf_qtypes);
        annotated_expr.ok()) {
      return (*annotated_expr)->qtype();
    } else {
      return nullptr;
    }
  };
  // presence_expr is constructed in such a way that the output type is always
  // OptionalUnit, even the constraint predicate passed to this function is
  // invalid.
  DCHECK_EQ(deduce_output_qtype(presence_expr), GetQType<OptionalUnit>());
  const QType* output_qtype = deduce_output_qtype(predicate_expr);
  if (output_qtype == nullptr) {
    return absl::InvalidArgumentError(
        "error while computing output QType of a QType constraint predicate: " +
        ToDebugString(expr));
  }
  if (output_qtype != GetQType<OptionalUnit>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a constraint predicate to return %s, got %s: %s",
        GetQType<OptionalUnit>()->name(), output_qtype->name(),
        ToDebugString(expr)));
  }
  return std::pair(std::move(predicate_expr), std::move(presence_expr));
}

std::string FormatQTypeNames(absl::string_view message,
                             const ParameterQTypes& parameter_qtypes) {
  absl::flat_hash_map<std::string, std::string> replacements;
  replacements.reserve(parameter_qtypes.size());
  for (const auto& [param_name, param_qtype] : parameter_qtypes) {
    replacements[absl::StrFormat("{%s}", param_name)] =
        std::string(param_qtype->name());
    if (IsTupleQType(param_qtype)) {
      replacements[absl::StrFormat("{*%s}", param_name)] =
          "(" +
          absl::StrJoin(param_qtype->type_fields(), ", ",
                        [](std::string* out, const auto& field_slot) {
                          // NOTE: Old compilers may not support
                          // string->append(string_view).
                          const absl::string_view name =
                              field_slot.GetType()->name();
                          out->append(name.data(), name.size());
                        }) +
          ")";
    }
  }
  return absl::StrReplaceAll(message, replacements);
}

}  // namespace

absl::StatusOr<QTypeConstraintFn> MakeQTypeConstraintFn(
    absl::Span<const QTypeConstraint> constraints) {
  if (constraints.empty()) {  // Simple case.
    return [](const ParameterQTypes&) -> absl::StatusOr<bool> { return true; };
  }

  std::vector<std::string> error_messages;

  // predicate expr and presence expr for each constraint.
  std::vector<ExprNodePtr> exprs;
  exprs.reserve(constraints.size() * 2);

  error_messages.reserve(constraints.size());
  for (const auto& constraint : constraints) {
    ASSIGN_OR_RETURN(
        (auto [predicate_expr, presence_expr]),
        PreprocessQTypeConstraint(constraint.predicate_expr));
    exprs.emplace_back(std::move(predicate_expr));
    exprs.emplace_back(std::move(presence_expr));
    error_messages.emplace_back(constraint.error_message);
  }

  ASSIGN_OR_RETURN(auto expr, BindOp(MakeTupleOperator::Make(), exprs, {}));
  ASSIGN_OR_RETURN(auto executor, MakeParameterQTypeModelExecutor(expr));
  return [executor = std::move(executor),
          error_messages = std::move(error_messages)](
             const ParameterQTypes& parameter_qtypes) -> absl::StatusOr<bool> {
    ASSIGN_OR_RETURN(auto values, executor(parameter_qtypes));
    DCHECK(IsTupleQType(values.GetType()));
    DCHECK(values.GetFieldCount() == error_messages.size() * 2);
    bool all_args_present = true;
    for (size_t i = 0; i < error_messages.size(); ++i) {
      ASSIGN_OR_RETURN(OptionalUnit fulfilled,
                       values.GetField(i * 2 + 0).As<OptionalUnit>());
      ASSIGN_OR_RETURN(OptionalUnit args_present,
                       values.GetField(i * 2 + 1).As<OptionalUnit>());
      all_args_present = all_args_present && args_present;
      if (args_present && !fulfilled) {
        return absl::InvalidArgumentError(
            FormatQTypeNames(error_messages[i], parameter_qtypes));
      }
    }
    return all_args_present;
  };
}

}  // namespace arolla::operator_loader
