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
#include "arolla/expr/operator_loader/generic_operator.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/eval/thread_safe_model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/helper.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorRegistry;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorSignaturePtr;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::MakeTupleOperator;
using ::arolla::expr::RegisteredOperator;
using ::arolla::expr::RegisteredOperatorPtr;
using ::arolla::expr::ThreadSafeCloneWhenBusyModelExecutor;

absl::StatusOr<std::shared_ptr<GenericOperator>> GenericOperator::Make(
    absl::string_view name, ExprOperatorSignature signature,
    absl::string_view doc) {
  if (!IsQualifiedIdentifier(name)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a operator name to be a valid namespace name, got '%s'",
        absl::CEscape(name)));
  }
  RETURN_IF_ERROR(ValidateSignature(signature));
  return std::make_shared<GenericOperator>(PrivateConstructorTag{}, name,
                                           std::move(signature), doc);
}

GenericOperator::GenericOperator(PrivateConstructorTag, absl::string_view name,
                                 ExprOperatorSignature signature,
                                 absl::string_view doc)
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
  ASSIGN_OR_RETURN(auto input_qtype_sequence, MakeInputQTypeSequence(inputs));
  ASSIGN_OR_RETURN(
      auto overload, GetOverload(input_qtype_sequence),
      WithNote(WithNote(std::move(_),
                        absl::StrCat("Input qtypes: ",
                                     FormatInputQTypes(signature(),
                                                       input_qtype_sequence),
                                     ".")),
               absl::StrCat("In generic operator: '",
                            absl::Utf8SafeCHexEscape(display_name()), "'.")));
  if (overload == nullptr) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(
      ExprAttributes attr, overload->base_operator()->InferAttributes(inputs),
      WithNote(
          _, absl::StrCat("In generic operator: '",
                          absl::Utf8SafeCHexEscape(display_name()), "', case '",
                          absl::Utf8SafeCHexEscape(overload->display_name()),
                          "'.")));
  return attr;
}

absl::StatusOr<ExprNodePtr absl_nonnull> GenericOperator::ToLowerLevel(
    const ExprNodePtr absl_nonnull& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  if (node->qtype() == nullptr) {
    return node;  // We are not ready for lowering yet.
  }
  ASSIGN_OR_RETURN(auto input_qtype_sequence,
                   MakeInputQTypeSequence(node->node_deps()));
  ASSIGN_OR_RETURN(
      auto overload, GetOverload(input_qtype_sequence),
      WithNote(WithNote(std::move(_),
                        absl::StrCat("Input qtypes: ",
                                     FormatInputQTypes(signature(),
                                                       input_qtype_sequence),
                                     ".")),
               absl::StrCat("In generic operator: '",
                            absl::Utf8SafeCHexEscape(display_name()), "'.")));
  if (overload == nullptr) {
    return node;  // We are not ready for lowering yet.
  }

  // Optimization note: We assume that the current node attributes are correct
  // and correspond to this operator, so we transfer them to the new node
  // without recomputing them using the lower-level node factory
  // ExprNode::UnsafeMakeOperatorNode.
  auto expr = ExprNode::UnsafeMakeOperatorNode(
      ExprOperatorPtr(overload->base_operator()),
      std::vector(node->node_deps()), ExprAttributes(node->attr()));
  ASSIGN_OR_RETURN(
      ExprNodePtr lowered, overload->base_operator()->ToLowerLevel(expr),
      WithNote(
          _, absl::StrCat("In generic operator: '",
                          absl::Utf8SafeCHexEscape(display_name()), "', case '",
                          absl::Utf8SafeCHexEscape(overload->display_name()),
                          "'.")));
  return lowered;
}

absl::StatusOr<GenericOperator::SnapshotOfOverloadsPtr>
GenericOperator::BuildSnapshot() const {
  const auto& ns = namespace_for_overloads();
  const auto& registry = *ExprOperatorRegistry::GetInstance();
  const auto revision_id = revision_id_fn_();
  std::vector<std::shared_ptr<const GenericOperatorOverload>> overloads;
  std::vector<ExprNodePtr> prepared_exprs;
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
      continue;  // Ignore non-generic overloads.
    }
    overloads.emplace_back(std::move(overload), typed_overload);
    prepared_exprs.push_back(typed_overload->prepared_readiness_expr());
    prepared_exprs.push_back(typed_overload->prepared_condition_expr());
  }
  ASSIGN_OR_RETURN(auto dispatch_expr, MakeOpNode(MakeTupleOperator::Make(),
                                                  std::move(prepared_exprs)));
  ASSIGN_OR_RETURN(auto model_executor, CompileModelExecutor<TypedValue>(
                                            std::move(dispatch_expr),
                                            GetInputQTypeSequenceLoader()));
  auto snapshot = std::make_shared<SnapshotOfOverloads>();
  snapshot->overloads = std::move(overloads);
  snapshot->dispatch_fn =
      ThreadSafeCloneWhenBusyModelExecutor(std::move(model_executor));
  snapshot->revision_id = revision_id;
  return snapshot;
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

absl::StatusOr<GenericOperatorOverloadPtr absl_nullable>
GenericOperator::GetOverload(const Sequence& input_qtype_sequence) const {
  ASSIGN_OR_RETURN(auto snapshot, GetSnapshot());

  // NOTE: The snapshot of overloads can become obsolete at any moment.
  // The possible scenarios:
  //  * A new operator has been added to the registry but missing in the
  //    snapshot: this situation is the same as if the overload was registered
  //    after this method call.
  //  * An operator has been unregistered ...: removing an operator from the
  //    registry is considered unsafe and (it's expected that it) may result in
  //    unspecified behaviour (but not "undefined behaviour").
  ASSIGN_OR_RETURN(auto dispatch_result,
                   snapshot->dispatch_fn(input_qtype_sequence));
  const auto& overloads = snapshot->overloads;
  const size_t n = overloads.size();
  size_t first_matched_index = n;
  size_t i = 0;
  bool all_ready = true;
  for (; i < n; ++i) {
    if (dispatch_result.GetField(2 * i).UnsafeAs<OptionalUnit>()) {
      if (dispatch_result.GetField(2 * i + 1).UnsafeAs<OptionalUnit>()) {
        first_matched_index = i;
        break;
      }
    } else {
      all_ready = false;
    }
  }
  std::vector<size_t> secondary_matched_indices;
  for (++i; i < n; ++i) {
    if (dispatch_result.GetField(2 * i).UnsafeAs<OptionalUnit>()) {
      if (dispatch_result.GetField(2 * i + 1).UnsafeAs<OptionalUnit>())
          [[unlikely]] {
        secondary_matched_indices.push_back(i);
      }
    } else {
      all_ready = false;
    }
  }
  if (!secondary_matched_indices.empty()) [[unlikely]] {
    std::ostringstream message;
    message << "multiple overload cases matched: '"
            << absl::Utf8SafeCHexEscape(
                   overloads[first_matched_index]->display_name());
    for (size_t j : secondary_matched_indices) {
      message << "', '"
              << absl::Utf8SafeCHexEscape(overloads[j]->display_name());
    }
    message << "'";
    return absl::InvalidArgumentError(std::move(message).str());
  }
  if (!all_ready) {
    return nullptr;
  }
  if (first_matched_index < n) {
    return overloads[first_matched_index];
  }
  return absl::InvalidArgumentError("no suitable overload operator");
}

absl::string_view GenericOperator::py_qvalue_specialization_key() const {
  return "::arolla::operator_loader::GenericOperator";
}

absl::StatusOr<std::shared_ptr<GenericOperatorOverload>>
GenericOperatorOverload::Make(absl::string_view name,
                              ExprOperatorSignature signature,
                              ExprNodePtr absl_nonnull condition_expr,
                              ExprOperatorPtr absl_nonnull base_operator) {
  RETURN_IF_ERROR(ValidateSignature(signature));
  for (const auto& param : signature.parameters) {
    if (param.default_value.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrCat("default values in generic operator overloads are "
                       "not supported, got signature_spec: '",
                       GetExprOperatorSignatureSpec(signature), "'"));
    }
  }
  Fingerprint fingerprint =
      FingerprintHasher("::arolla::operator_loader::GenericOperatorOverload")
          .Combine(name, signature, condition_expr->fingerprint(),
                   base_operator->fingerprint())
          .Finish();
  ASSIGN_OR_RETURN(
      (auto [prepared_condition_expr, prepared_readiness_expr]),
      ResolvePlaceholders(signature, condition_expr, GetQType<OptionalUnit>()),
      WithCause(absl::InvalidArgumentError(
                    absl::StrCat("problem with an overload condition: '",
                                 absl::Utf8SafeCHexEscape(name), "'")),
                std::move(_)));
  return std::make_shared<GenericOperatorOverload>(
      PrivateConstructorTag{}, name, std::move(signature),
      std::move(condition_expr), std::move(base_operator), fingerprint,
      std::move(prepared_readiness_expr), std::move(prepared_condition_expr));
}

GenericOperatorOverload::GenericOperatorOverload(
    PrivateConstructorTag, absl::string_view name,
    ExprOperatorSignature signature, ExprNodePtr absl_nonnull condition_expr,
    ExprOperatorPtr absl_nonnull base_operator, Fingerprint fingerprint,
    ExprNodePtr absl_nonnull prepared_readiness_expr,
    ExprNodePtr absl_nonnull prepared_condition_expr)
    : ExprOperatorWithFixedSignature(
          name, std::move(signature),
          "A generic operator overload; not expected to be used directly!",
          fingerprint),
      condition_expr_(std::move(condition_expr)),
      base_operator_(std::move(base_operator)),
      prepared_readiness_expr_(std::move(prepared_readiness_expr)),
      prepared_condition_expr_(std::move(prepared_condition_expr)) {}

absl::StatusOr<ExprAttributes> GenericOperatorOverload::InferAttributes(
    absl::Span<const ExprAttributes> /*inputs*/) const {
  return absl::UnimplementedError("GenericOperatorOverload::InferAttributes");
}

absl::StatusOr<ExprNodePtr absl_nonnull> GenericOperatorOverload::ToLowerLevel(
    const ExprNodePtr absl_nonnull& /*node*/) const {
  return absl::UnimplementedError("GenericOperatorOverload::ToLowerLevel");
}

absl::string_view GenericOperatorOverload::py_qvalue_specialization_key()
    const {
  return "::arolla::operator_loader::GenericOperatorOverload";
}

}  // namespace arolla::operator_loader
