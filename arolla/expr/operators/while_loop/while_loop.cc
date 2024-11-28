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
#include "arolla/expr/operators/while_loop/while_loop.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl//algorithm/container.h"
#include "absl//container/flat_hash_map.h"
#include "absl//container/flat_hash_set.h"
#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/match.h"
#include "absl//strings/str_format.h"
#include "absl//strings/str_join.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/while_loop/while_loop_impl.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetAttrQTypes;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;

constexpr absl::string_view kDefaultOperatorName = "anonymous.while_loop";
constexpr absl::string_view kLoopStatePlaceholderName = "loop_state";

// Extracts (sorted) names from named_expressions.
std::vector<std::string> ExpressionNames(
    const NamedExpressions& named_expressions) {
  std::vector<std::string> names_order;
  names_order.reserve(named_expressions.size());
  for (const auto& [k, _] : named_expressions) {
    names_order.push_back(k);
  }
  std::sort(names_order.begin(), names_order.end());
  return names_order;
}

// Splits the given tuple into named elements according to names_order.
absl::StatusOr<NamedExpressions> MakeNamedAccessors(
    const ExprNodePtr& tuple_node, absl::Span<const std::string> names_order) {
  NamedExpressions named_accessors;
  named_accessors.reserve(names_order.size());
  for (size_t i = 0; i < names_order.size(); ++i) {
    ASSIGN_OR_RETURN(
        auto nth_field,
        expr::CallOp("core.get_nth", {tuple_node, expr::Literal<int64_t>(i)}));
    named_accessors.emplace(names_order[i], std::move(nth_field));
  }
  return named_accessors;
}

// Constructs a tuple of the given named expressions in the given order.
absl::StatusOr<ExprNodePtr> WrapAsTuple(
    const NamedExpressions& named_expressions,
    absl::Span<const std::string> names_order) {
  std::vector<ExprNodePtr> deps;
  deps.reserve(names_order.size() + 1);
  deps.emplace_back(Literal(Text(absl::StrJoin(names_order, ","))));
  for (const auto& f : names_order) {
    if (!named_expressions.contains(f)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value for the state variable %s is not specified", f));
    }
    deps.push_back(named_expressions.at(f));
  }
  return BindOp("namedtuple.make", deps, {});
}

// Casts initial_state fields to match the types of body fields.
absl::StatusOr<NamedExpressions> AddImplicitCastsToInitialState(
    const NamedExpressions& initial_state, const NamedExpressions& body) {
  NamedExpressions new_initial_state = initial_state;
  for (auto& [name, expr] : body) {
    ASSIGN_OR_RETURN(auto expr_after_one_iteration,
                     SubstitutePlaceholders(expr, initial_state));
    ASSIGN_OR_RETURN(new_initial_state[name],
                     CallOp("core.cast", {initial_state.at(name),
                                          CallOp("qtype.qtype_of",
                                                 {expr_after_one_iteration}),
                                          /*implicit_only=*/Literal(true)}),
                     _ << "while casting initial state for P." << name);
  }
  return new_initial_state;
}

// Moves subexpressions that do not depend on placeholders (aka immutable in
// the while_loop context) from `condition` and `body` into new items in
// initial_state map. Replaces the moved parts with newly created placeholders.
// All three arguments can be modified.
absl::Status MoveImmutablesIntoInitialState(NamedExpressions& initial_state,
                                            ExprNodePtr& condition,
                                            NamedExpressions& body) {
  constexpr absl::string_view kImmutableNamePrefix = "_while_loop_immutable";
  for (auto& [name, _] : body) {
    if (absl::StartsWith(name, kImmutableNamePrefix)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expression names starting with '%s' are forbidden in while_loop",
          kImmutableNamePrefix));
    }
  }

  absl::flat_hash_map<Fingerprint, std::string> immutable_names;
  auto immutable_naming_function = [&](const ExprNodePtr& node) -> std::string {
    if (auto it = immutable_names.find(node->fingerprint());
        it != immutable_names.end()) {
      return it->second;
    }
    std::string name =
        absl::StrFormat("%s_%d", kImmutableNamePrefix, immutable_names.size());
    immutable_names.emplace(node->fingerprint(), name);
    return name;
  };

  for (auto& [name, expr] : body) {
    ASSIGN_OR_RETURN(
        (auto [converted_expr, immutables]),
        while_loop_impl::ExtractImmutables(expr, immutable_naming_function));
    expr = std::move(converted_expr);
    initial_state.merge(std::move(immutables));
  }
  ASSIGN_OR_RETURN(
      (auto [converted_condition, condition_immutables]),
      while_loop_impl::ExtractImmutables(condition, immutable_naming_function));
  condition = std::move(converted_condition);
  initial_state.merge(std::move(condition_immutables));
  return absl::OkStatus();
}

// Essentially checks that the first argument contains each element of the
// second.
absl::Status CheckAllStateFieldsAreInitialized(
    const std::vector<std::string>& all_field_names,
    const std::vector<std::string>& requested_field_names) {
  absl::flat_hash_set<absl::string_view> all_field_names_set(
      all_field_names.begin(), all_field_names.end());
  for (const auto& name : requested_field_names) {
    if (!all_field_names_set.contains(name)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "no initial value given for the loop state variable `%s`", name));
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<ExprNodePtr> MakeWhileLoop(NamedExpressions initial_state,
                                          ExprNodePtr condition,
                                          NamedExpressions body) {
  RETURN_IF_ERROR(
      MoveImmutablesIntoInitialState(initial_state, condition, body));

  auto state_field_names = ExpressionNames(initial_state);
  auto mutable_state_field_names = ExpressionNames(body);

  // Validate that loop body and condition do not mention variables other than
  // state_field_names.
  RETURN_IF_ERROR(CheckAllStateFieldsAreInitialized(state_field_names,
                                                    mutable_state_field_names));
  RETURN_IF_ERROR(CheckAllStateFieldsAreInitialized(
      state_field_names, expr::GetPlaceholderKeys(condition)));
  for (const auto& [_, expr] : body) {
    RETURN_IF_ERROR(CheckAllStateFieldsAreInitialized(
        state_field_names, expr::GetPlaceholderKeys(expr)));
  }

  ASSIGN_OR_RETURN(initial_state,
                   AddImplicitCastsToInitialState(initial_state, body));

  std::vector<std::string> immutable_state_field_names;
  immutable_state_field_names.reserve(state_field_names.size() -
                                      mutable_state_field_names.size());
  absl::c_set_difference(state_field_names, mutable_state_field_names,
                         std::back_inserter(immutable_state_field_names));

  ASSIGN_OR_RETURN(auto init_mutable_state_tuple,
                   WrapAsTuple(initial_state, mutable_state_field_names));
  ASSIGN_OR_RETURN(auto body_mutable_state_tuple,
                   WrapAsTuple(body, mutable_state_field_names));

  ExprOperatorSignature operators_signature;
  operators_signature.parameters.reserve(1 +
                                         immutable_state_field_names.size());
  operators_signature.parameters.push_back(
      ExprOperatorSignature::Parameter{std::string{kLoopStatePlaceholderName}});
  std::vector<ExprNodePtr> init_deps;
  init_deps.reserve(1 + immutable_state_field_names.size());
  init_deps.emplace_back(init_mutable_state_tuple);
  for (const auto& name : immutable_state_field_names) {
    operators_signature.parameters.push_back(
        ExprOperatorSignature::Parameter{name});
    DCHECK(initial_state.contains(name))
        << "Internal inconsistency: no initializer for node " << name;
    init_deps.emplace_back(initial_state.at(name));
  }

  // Replacing named parameters with getters from the state tuple
  // (P.loop_state).
  auto state_placeholder = Placeholder(kLoopStatePlaceholderName);
  ASSIGN_OR_RETURN(
      auto state_fields,
      MakeNamedAccessors(state_placeholder, mutable_state_field_names));
  ASSIGN_OR_RETURN(auto condition_op,
                   MakeLambdaOperator(
                       "anonymous.loop_condition", operators_signature,
                       SubstitutePlaceholders(condition, state_fields,
                                              /*must_substitute_all=*/false)));
  ASSIGN_OR_RETURN(auto body_op, MakeLambdaOperator(
                                     "anonymous.loop_body", operators_signature,
                                     SubstitutePlaceholders(
                                         body_mutable_state_tuple, state_fields,
                                         /*must_substitute_all=*/false)));

  ASSIGN_OR_RETURN(
      ExprOperatorPtr while_op,
      WhileLoopOperator::Make(operators_signature, condition_op, body_op));
  ASSIGN_OR_RETURN(auto while_node, BindOp(while_op, init_deps, {}));
  return while_node;
}

absl::StatusOr<std::shared_ptr<WhileLoopOperator>> WhileLoopOperator::Make(
    const ExprOperatorSignature& signature, const ExprOperatorPtr& condition,
    const ExprOperatorPtr& body) {
  return Make(kDefaultOperatorName, signature, condition, body);
}

absl::StatusOr<std::shared_ptr<WhileLoopOperator>> WhileLoopOperator::Make(
    absl::string_view name, const ExprOperatorSignature& signature,
    const ExprOperatorPtr& condition, const ExprOperatorPtr& body) {
  if (signature.parameters.empty()) {
    return absl::InvalidArgumentError(
        "WhileLoopOperator must at least have one parameter, got 0");
  }
  // Perform minimal verifications to fail earlier. Deeper inconsistenceis like
  // wrong GetOutputQType can be only detected later.
  ASSIGN_OR_RETURN(auto condition_signature, condition->GetSignature());
  ASSIGN_OR_RETURN(auto body_signature, body->GetSignature());
  auto signature_spec = GetExprOperatorSignatureSpec(signature);
  auto body_signature_spec = GetExprOperatorSignatureSpec(body_signature);
  if (signature_spec != body_signature_spec) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "loop signature does not match its body signature: `%s` vs `%s`",
        signature_spec, body_signature_spec));
  }
  auto condition_signature_spec =
      GetExprOperatorSignatureSpec(condition_signature);
  if (signature_spec != condition_signature_spec) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "loop signature does not match its condition signature: `%s` vs `%s`",
        signature_spec, condition_signature_spec));
  }
  return std::make_shared<WhileLoopOperator>(PrivateConstrutorTag(), name,
                                             signature, condition, body);
}

WhileLoopOperator::WhileLoopOperator(PrivateConstrutorTag,
                                     absl::string_view name,
                                     const ExprOperatorSignature& signature,
                                     const ExprOperatorPtr& condition,
                                     const ExprOperatorPtr& body)
    : ExprOperatorWithFixedSignature(
          name, signature,
          "",  // TODO: doc-string
          FingerprintHasher("arolla::expr_operators::WhileLoopOperator")
              .Combine(name, condition->fingerprint(), body->fingerprint())
              .Finish()),
      condition_(condition),
      body_(body) {}

absl::StatusOr<ExprAttributes> WhileLoopOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  DCHECK_GE(inputs.size(), 1);
  if (!inputs[0].qtype()) {
    return ExprAttributes{};
  }
  // Clean up literal values for mutable state as it is going to change on every
  // iteration.
  std::vector<ExprAttributes> new_inputs;
  new_inputs.reserve(inputs.size());
  new_inputs.emplace_back(inputs[0].qtype());
  new_inputs.insert(new_inputs.end(), inputs.begin() + 1, inputs.end());
  ASSIGN_OR_RETURN(
      auto condition_attr, condition_->InferAttributes(new_inputs),
      _ << "in condition of `" << display_name() << "` while loop");
  if (condition_attr.qtype() &&
      condition_attr.qtype() != GetQType<OptionalUnit>()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "incorrect return type of the condition of `%s` while loop for input "
        "types %s: expected %s, got %s",
        display_name(), FormatTypeVector(GetAttrQTypes(inputs)),
        GetQType<OptionalUnit>()->name(), condition_attr.qtype()->name()));
  }
  ASSIGN_OR_RETURN(auto body_attr, body_->InferAttributes(new_inputs),
                   _ << "in body of `" << display_name() << "` while loop");
  if (body_attr.qtype() && body_attr.qtype() != inputs[0].qtype()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "incorrect return type of the body of `%s` while loop for input types "
        "%s: expected %s, got %s",
        display_name(), FormatTypeVector(GetAttrQTypes(inputs)),
        inputs[0].qtype()->name(), body_attr.qtype()->name()));
  }
  return ExprAttributes(inputs[0].qtype());
}

}  // namespace arolla::expr_operators
