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
#include "arolla/expr/operator_loader/dispatch_operator.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/generic_operator_overload_condition.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::GetPlaceholderKeys;
using ::arolla::expr::ValidateDepsCount;

absl::StatusOr<ExprOperatorPtr> DispatchOperator::Make(
    absl::string_view name, expr::ExprOperatorSignature signature,
    std::vector<Overload> overloads,
    expr::ExprNodePtr dispatch_readiness_condition) {
  RETURN_IF_ERROR(ValidateSignature(signature));
  // Collect parameters used by qtype constraints.
  for (const auto& overload : overloads) {
    const auto& placeholder_keys = GetPlaceholderKeys(overload.condition);
    if (!placeholder_keys.empty()) {
      return absl::InvalidArgumentError(
          "placeholders are not supported "
          "in dispatch operator overload conditions");
    }
  }
  for (const auto& param : signature.parameters) {
    if (param.default_value.has_value()) {
      return absl::InvalidArgumentError(
          "signatures with the default values are not supported "
          "in dispatch operator; "
          "got signature: " +
          GetExprOperatorSignatureSpec(signature));
    }
  }
  std::vector<ExprNodePtr> overload_conditions;
  overload_conditions.reserve(overloads.size() + 1);
  for (const auto& overload : overloads) {
    overload_conditions.push_back(overload.condition);
  }
  overload_conditions.push_back(dispatch_readiness_condition);
  // Compile qtype constraints for each overload.
  ASSIGN_OR_RETURN(auto overloads_condition_fn,
                   MakeGenericOperatorOverloadConditionFn(overload_conditions));

  // Generate fingerprint.
  FingerprintHasher hasher("::arolla::operator_loader::DispatchOperator");
  hasher.Combine(name, signature, dispatch_readiness_condition->fingerprint(),
                 overloads.size());
  for (const auto& overload : overloads) {
    hasher.Combine(overload.name, overload.op->fingerprint(),
                   overload.condition->fingerprint());
  }

  return std::make_shared<DispatchOperator>(
      PrivateConstructorTag{}, name, std::move(signature), std::move(overloads),
      std::move(overloads_condition_fn),
      std::move(dispatch_readiness_condition), std::move(hasher).Finish());
}

DispatchOperator::DispatchOperator(
    PrivateConstructorTag, absl::string_view name,
    expr::ExprOperatorSignature signature, std::vector<Overload> overloads,
    GenericOperatorOverloadConditionFn overloads_condition_fn,
    expr::ExprNodePtr dispatch_readiness_condition, Fingerprint fingerprint)
    : ExprOperatorWithFixedSignature(name, signature, "", fingerprint),
      overloads_(std::move(overloads)),
      overloads_condition_fn_(std::move(overloads_condition_fn)),
      dispatch_readiness_condition_(std::move(dispatch_readiness_condition)) {}

absl::StatusOr<expr::ExprAttributes> DispatchOperator::InferAttributes(
    absl::Span<const expr::ExprAttributes> inputs) const {
  ASSIGN_OR_RETURN(const auto* overload, LookupImpl(inputs));
  if (overload == nullptr) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(
      expr::ExprAttributes attr, overload->op->InferAttributes(inputs),
      WithNote(_, absl::StrCat("In ", absl::Utf8SafeCHexEscape(overload->name),
                               " overload of DispatchOperator.")));
  return attr;
}

absl::StatusOr<expr::ExprNodePtr> DispatchOperator::ToLowerLevel(
    const expr::ExprNodePtr& node) const {
  ASSIGN_OR_RETURN(const auto* overload,
                   LookupImpl(GetExprAttrs(node->node_deps())));
  if (overload == nullptr) {
    return node;  // We are not ready for lowering yet.
  }
  // Optimization note: We assume that the current node attributes are correct
  // and correspond to this operator, so we transfer them to the new node
  // without recomputing them using the lower-level node factory
  // ExprNode::UnsafeMakeOperatorNode.
  auto expr = ExprNode::UnsafeMakeOperatorNode(ExprOperatorPtr(overload->op),
                                               std::vector(node->node_deps()),
                                               ExprAttributes(node->attr()));
  ASSIGN_OR_RETURN(
      expr::ExprNodePtr lowered, expr->op()->ToLowerLevel(expr),
      WithNote(_, absl::StrCat("In ", absl::Utf8SafeCHexEscape(overload->name),
                               " overload of DispatchOperator.")));
  return lowered;
}

absl::StatusOr<const DispatchOperator::Overload* absl_nullable>
DispatchOperator::LookupImpl(absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateDepsCount(signature(), inputs.size(),
                                    absl::StatusCode::kInvalidArgument));
  auto input_qtypes = GetAttrQTypes(inputs);
  for (auto& input_qtype : input_qtypes) {
    if (input_qtype == nullptr) {
      input_qtype = GetNothingQType();
    }
  }
  ASSIGN_OR_RETURN(auto is_condition_passed,
                   overloads_condition_fn_(MakeTupleQType(input_qtypes)));
  if (is_condition_passed.size() != overloads_.size() + 1) {
    return absl::InternalError("the state of DispatchOperator is invalid");
  }
  bool ready_to_dispatch = is_condition_passed.back();
  if (!ready_to_dispatch) {
    if (HasAllAttrQTypes(inputs)) {
      return absl::FailedPreconditionError(
          absl::StrFormat("the operator is broken for argument types %s",
                          FormatTypeVector(input_qtypes)));
    }
    return nullptr;  // not enough argument types are known for dispatch.
  }
  std::vector<size_t> matching_ids;
  for (size_t i = 0; i < overloads_.size(); ++i) {
    if (is_condition_passed[i]) {
      matching_ids.push_back(i);
    }
  }
  if (matching_ids.size() > 1) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "constraints of the multiple overloads (%s) passed for argument "
        "types %s",
        absl::StrJoin(matching_ids, ", ",
                      [&](std::string* out, size_t id) {
                        absl::StrAppend(
                            out, absl::Utf8SafeCHexEscape(overloads_[id].name));
                      }),
        FormatTypeVector(input_qtypes)));
  }
  if (matching_ids.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("no suitable overload for argument types %s",
                        FormatTypeVector(input_qtypes)));
  }
  return &overloads_[matching_ids[0]];
}

ReprToken DispatchOperator::GenReprToken() const {
  return {absl::StrFormat(
      "<DispatchOperator: name='%s', signature='%s', cases=['%s']>",
      absl::Utf8SafeCHexEscape(display_name()),
      GetExprOperatorSignatureSpec(signature()),
      absl::StrJoin(
          overloads_, "', '", [](std::string* out, const auto& overload) {
            absl::StrAppend(out, absl::Utf8SafeCHexEscape(overload.name));
          }))};
}

absl::string_view DispatchOperator::py_qvalue_specialization_key() const {
  return "::arolla::operator_loader::DispatchOperator";
}

}  // namespace arolla::operator_loader
