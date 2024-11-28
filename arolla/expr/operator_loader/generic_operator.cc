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
#include "arolla/expr/operator_loader/generic_operator.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/escaping.h"
#include "absl//strings/str_cat.h"
#include "absl//strings/str_format.h"
#include "absl//strings/str_join.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/operator_loader/generic_operator_overload_condition.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/demangle.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorRegistry;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::GetExprAttrs;
using ::arolla::expr::PostOrder;
using ::arolla::expr::RegisteredOperator;
using ::arolla::expr::RegisteredOperatorPtr;
using Param = ExprOperatorSignature::Parameter;

std::string FormatSignatureQTypes(
    const ExprOperatorSignature& signature,
    absl::Span<QType const* const /*nullable*/> input_qtypes) {
  std::string result;
  bool skip_first_comma = true;
  size_t i = 0;
  for (const auto& param : signature.parameters) {
    switch (param.kind) {
      case Param::Kind::kPositionalOrKeyword:
        DCHECK_LT(i, input_qtypes.size());
        if (auto* input_qtype = input_qtypes[i++]) {
          absl::StrAppend(&result, NonFirstComma(skip_first_comma), param.name,
                          ": ", input_qtype->name());
        } else {
          absl::StrAppend(&result, NonFirstComma(skip_first_comma), param.name);
        }
        break;
      case Param::Kind::kVariadicPositional:
        absl::StrAppend(&result, NonFirstComma(skip_first_comma), "*",
                        param.name, ": (");
        for (bool first = true; i < input_qtypes.size(); ++i) {
          absl::StrAppend(&result, NonFirstComma(first),
                          input_qtypes[i] ? input_qtypes[i]->name() : "-");
        }
        absl::StrAppend(&result, ")");
        break;
    }
  }
  return result;
}

}  // namespace

absl::StatusOr<std::shared_ptr<GenericOperator>> GenericOperator::Make(
    absl::string_view name, ExprOperatorSignature signature,
    absl::string_view doc) {
  if (!IsQualifiedIdentifier(name)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a operator name to be a valid namespace name, got '%s'",
        absl::CEscape(name)));
  }
  RETURN_IF_ERROR(ValidateSignature(signature));
  for (const auto& param : signature.parameters) {
    if (param.kind != Param::Kind::kPositionalOrKeyword &&
        param.kind != Param::Kind::kVariadicPositional) {
      return absl::InvalidArgumentError(
          absl::StrCat("unsupported parameter kind '", param.name, "', ",
                       static_cast<int>(param.kind)));
    }
  }
  return std::make_shared<GenericOperator>(PrivateConstructorTag{}, name,
                                           std::move(signature), doc);
}

GenericOperator::GenericOperator(
    PrivateConstructorTag, absl::string_view name,
    ::arolla::expr::ExprOperatorSignature signature, absl::string_view doc)
    : ExprOperatorWithFixedSignature(
          name, signature, doc,
          FingerprintHasher("::arolla::operator_loader::GenericOperator")
              .Combine(name, signature, doc)
              .Finish()),
      revision_id_fn_(
          ExprOperatorRegistry::GetInstance()->AcquireRevisionIdFn(name)) {}

absl::StatusOr<ExprAttributes> GenericOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  ASSIGN_OR_RETURN(auto overload, GetOverload(inputs));
  if (overload == nullptr) {
    return ExprAttributes{};
  }
  return overload->InferAttributes(inputs);
}

absl::StatusOr<::arolla::expr::ExprNodePtr> GenericOperator::ToLowerLevel(
    const ::arolla::expr::ExprNodePtr& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  ASSIGN_OR_RETURN(auto overload, GetOverload(GetExprAttrs(node->node_deps())));
  if (overload == nullptr) {
    return node;
  }
  // Optimization note: We assume that the current node attributes are correct
  // and correspond to this operator, so we transfer them to the new node
  // without recomputing them using the lower-level node factory
  // ExprNode::UnsafeMakeOperatorNode.
  return ExprNode::UnsafeMakeOperatorNode(std::move(overload),
                                          std::vector(node->node_deps()),
                                          ExprAttributes(node->attr()));
}

absl::StatusOr<GenericOperator::SnapshotOfOverloadsPtr>
GenericOperator::BuildSnapshot() const {
  const auto& ns = namespace_for_overloads();
  const auto& registry = *ExprOperatorRegistry::GetInstance();
  const auto revision_id = revision_id_fn_();
  std::vector<RegisteredOperatorPtr> overloads;
  std::vector<ExprNodePtr> condition_exprs;
  for (const auto& operator_name : registry.ListRegisteredOperators()) {
    if (!(ns.size() < operator_name.size() &&
          std::equal(ns.begin(), ns.end(), operator_name.begin()) &&
          operator_name[ns.size()] == '.')) {
      continue;
    }
    auto registered_overload = registry.LookupOperatorOrNull(operator_name);
    if (registered_overload == nullptr) {
      continue;  // Race condition: Possibly, the operator has just been
                 // UnsafeUnregister()-ed.
    }
    auto overload = DecayRegisteredOperator(registered_overload)
                        .value_or(ExprOperatorPtr{});
    if (overload == nullptr) {
      continue;  // Race condition: Possibly, the operator has just been
                 // UnsafeUnregister()-ed.
    }
    auto* typed_overload =
        fast_dynamic_downcast_final<const GenericOperatorOverload*>(
            overload.get());
    if (typed_overload == nullptr) {
      return absl::FailedPreconditionError(
          absl::StrFormat("expected a GenericOperatorOverload, got %s: %s",
                          TypeName(typeid(*overload)), operator_name));
    }
    overloads.push_back(registered_overload);
    condition_exprs.push_back(
        typed_overload->prepared_overload_condition_expr());
  }
  ASSIGN_OR_RETURN(
      auto condition_fn,
      MakeGenericOperatorOverloadConditionFn(condition_exprs),
      _ << "failed to compile overload conditions of generic operator "
        << display_name());
  auto result = std::make_shared<SnapshotOfOverloads>();
  result->overloads = std::move(overloads);
  result->overload_condition_fn = std::move(condition_fn);
  // Use the generation ID we read at the beginning of this call to be able to
  // detect any concurrent changes the next time.
  result->revision_id = revision_id;
  return result;
}

absl::StatusOr<GenericOperator::SnapshotOfOverloadsPtr>
GenericOperator::GetSnapshot() const {
  auto result = snapshot_of_overloads_.load();
  if (result != nullptr && result->revision_id == revision_id_fn_()) {
    return result;
  }
  ASSIGN_OR_RETURN(result, BuildSnapshot());
  snapshot_of_overloads_.store(result);
  return result;
}

absl::StatusOr<ExprOperatorPtr /*nullable*/> GenericOperator::GetOverload(
    absl::Span<const ::arolla::expr::ExprAttributes> inputs) const {
  ASSIGN_OR_RETURN(auto snapshot, GetSnapshot());
  // NOTE: The snapshot of overloads can become obsolete at any moment.
  // The possible scenarios:
  //  * A new operator has been added to the registry but missing in the
  //    snapshot: this situation is the same as if the overload was registered
  //    after this method call.
  //  * An operator has been unregistered ...: removing an operator from the
  //    registry is considered unsafe and (it's expected that it) may result in
  //    unspecified behaviour (but not "undefined behaviour").
  auto input_qtypes = GetAttrQTypes(inputs);
  for (auto& input_qtype : input_qtypes) {
    if (input_qtype == nullptr) {
      input_qtype = GetNothingQType();
    }
  }
  ASSIGN_OR_RETURN(auto overload_conditions, snapshot->overload_condition_fn(
                                                 MakeTupleQType(input_qtypes)));
  const auto& overloads = snapshot->overloads;
  DCHECK_EQ(overload_conditions.size(), overloads.size());
  auto it =
      std::find(overload_conditions.begin(), overload_conditions.end(), true);
  if (it == overload_conditions.end()) {
    if (HasAllAttrQTypes(inputs)) {
      return absl::InvalidArgumentError(absl::StrCat(
          "no matching overload [",
          FormatSignatureQTypes(signature(), GetAttrQTypes(inputs)), "]"));
    }
    return nullptr;
  }
  auto jt = std::find(it + 1, overload_conditions.end(), true);
  if (jt == overload_conditions.end()) {
    return overloads[it - overload_conditions.begin()];
  }
  std::set<absl::string_view> ambiguous_overload_names = {
      overloads[it - overload_conditions.begin()]->display_name(),
      overloads[jt - overload_conditions.begin()]->display_name(),
  };
  for (;;) {
    jt = std::find(jt + 1, overload_conditions.end(), true);
    if (jt == overload_conditions.end()) {
      break;
    }
    ambiguous_overload_names.insert(
        overloads[jt - overload_conditions.begin()]->display_name());
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "ambiguous overloads: ", absl::StrJoin(ambiguous_overload_names, ", "),
      " [", FormatSignatureQTypes(signature(), GetAttrQTypes(inputs)), "]"));
}

absl::string_view GenericOperator::py_qvalue_specialization_key() const {
  return "::arolla::operator_loader::GenericOperator";
}

absl::StatusOr<std::shared_ptr<GenericOperatorOverload>>
GenericOperatorOverload::Make(ExprOperatorPtr base_operator,
                              ExprNodePtr prepared_overload_condition_expr) {
  if (base_operator == nullptr) {
    return absl::InvalidArgumentError("base_operator==nullptr");
  }
  if (prepared_overload_condition_expr == nullptr) {
    return absl::InvalidArgumentError(
        "prepared_overload_condition_expr==nullptr");
  }
  std::set<absl::string_view> leaf_keys;
  std::set<absl::string_view> placeholder_keys;
  PostOrder post_order(prepared_overload_condition_expr);
  for (const auto& node : post_order.nodes()) {
    if (node->is_leaf()) {
      leaf_keys.insert(node->leaf_key());
    } else if (node->is_placeholder()) {
      placeholder_keys.insert(node->placeholder_key());
    }
  }
  leaf_keys.erase(kGenericOperatorPreparedOverloadConditionLeafKey);
  if (!placeholder_keys.empty()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "prepared overload condition contains unexpected placeholders: P.",
        absl::StrJoin(placeholder_keys, ", P.")));
  }
  if (!leaf_keys.empty()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "prepared overload condition contains unexpected leaves: L.",
        absl::StrJoin(leaf_keys, ", L.")));
  }
  return std::make_shared<GenericOperatorOverload>(
      PrivateConstructorTag{}, std::move(base_operator),
      std::move(prepared_overload_condition_expr));
}

GenericOperatorOverload::GenericOperatorOverload(
    PrivateConstructorTag, ExprOperatorPtr base_operator,
    ExprNodePtr prepared_overload_condition_expr)
    : ExprOperator(base_operator->display_name(),
                   FingerprintHasher(
                       "::arolla::operator_loader::GenericOperatorOverload")
                       .Combine(base_operator->fingerprint(),
                                prepared_overload_condition_expr->fingerprint())
                       .Finish()),
      base_operator_(std::move(base_operator)),
      prepared_overload_condition_expr_(
          std::move(prepared_overload_condition_expr)) {}

absl::StatusOr<::arolla::expr::ExprNodePtr>
GenericOperatorOverload::ToLowerLevel(
    const ::arolla::expr::ExprNodePtr& node) const {
  // Optimization note: We assume that the current node attributes are correct
  // and correspond to this operator, so we transfer them to the new node
  // without recomputing them using the lower-level node factory
  // ExprNode::UnsafeMakeOperatorNode.
  auto new_node = ExprNode::UnsafeMakeOperatorNode(
      ExprOperatorPtr(base_operator_), std::vector(node->node_deps()),
      ExprAttributes(node->attr()));
  return base_operator_->ToLowerLevel(new_node);
}

absl::string_view GenericOperatorOverload::py_qvalue_specialization_key()
    const {
  return "::arolla::operator_loader::GenericOperatorOverload";
}

}  // namespace arolla::operator_loader
