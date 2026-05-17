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
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operator_loader/helper.h"
#include "arolla/expr/operator_loader/parameter_qtypes.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_loader {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::MakeTupleOperator;

using Overload = DispatchOperator::Overload;

absl::StatusOr<ExprNodePtr absl_nonnull> PrepareDispatchExpr(
    const ExprOperatorSignature& signature,
    absl::Span<const Overload> overloads) {
  std::vector<ExprNodePtr> condition_exprs;
  condition_exprs.reserve(overloads.size());
  for (const auto& overload : overloads) {
    // Note: Verify the condition expression, however we abandon the result, and
    // prepare it again as a part of the whole dispatch expression.
    auto status = ResolvePlaceholders(signature, overload.condition_expr,
                                      GetQType<OptionalUnit>())
                      .status();
    if (!status.ok()) {
      return WithCause(absl::InvalidArgumentError(absl::StrCat(
                           "problem with an overload condition: '",
                           absl::Utf8SafeCHexEscape(overload.name), "'")),
                       std::move(status));
    }
    condition_exprs.push_back(overload.condition_expr);
  }
  const auto& make_tuple_op = MakeTupleOperator::Make();
  ASSIGN_OR_RETURN(auto expr,
                   MakeOpNode(make_tuple_op, std::move(condition_exprs)));
  ASSIGN_OR_RETURN(
      (auto [resolved_expr, readiness_expr]),
      ResolvePlaceholders(signature, expr, /*expected_output_qtype=*/nullptr));
  const auto& resolved_exprs = resolved_expr->node_deps();
  DCHECK_EQ(resolved_expr->op(), make_tuple_op);
  DCHECK_EQ(resolved_exprs.size(), overloads.size());
  std::vector<ExprNodePtr> result;
  result.reserve(1 + resolved_exprs.size());
  result.push_back(std::move(readiness_expr));
  result.insert(result.end(), resolved_exprs.begin(), resolved_exprs.end());
  return MakeOpNode(make_tuple_op, std::move(result));
}

}  // namespace

absl::StatusOr<DispatchOperator::DispatchFn absl_nonnull>
DispatchOperator::MakeDispatchFn(const ExprOperatorSignature& signature,
                                 absl::Span<const Overload> overloads) {
  ASSIGN_OR_RETURN(auto dispatch_expr,
                   PrepareDispatchExpr(signature, overloads));
  ASSIGN_OR_RETURN(auto model_executor, CompileModelExecutor<TypedValue>(
                                            std::move(dispatch_expr),
                                            GetInputQTypeSequenceLoader()));
  return [model_executor =
              std::move(model_executor)](const Sequence& input_qtype_sequence) {
    return model_executor.ExecuteOnHeap(
        /*eval_options=*/{}, input_qtype_sequence);
  };
}

absl::StatusOr<std::shared_ptr<const DispatchOperator>> DispatchOperator::Make(
    absl::string_view name, ExprOperatorSignature signature,
    absl::string_view doc, std::vector<Overload> overloads,
    ExprOperatorPtr absl_nullable default_op) {
  RETURN_IF_ERROR(ValidateSignature(signature));
  ASSIGN_OR_RETURN(auto dispatch_fn, MakeDispatchFn(signature, overloads));
  FingerprintHasher hasher("::arolla::operator_loader::DispatchOperator");
  hasher.Combine(name, signature, doc, overloads.size());
  for (const auto& overload : overloads) {
    hasher.Combine(overload.name, overload.op->fingerprint(),
                   overload.condition_expr->fingerprint());
  }
  if (default_op != nullptr) {
    hasher.Combine(default_op->fingerprint());
  }
  return std::make_shared<DispatchOperator>(
      PrivateConstructorTag{}, name, std::move(signature), doc,
      std::move(overloads), std::move(default_op), std::move(dispatch_fn),
      std::move(hasher).Finish());
}

DispatchOperator::DispatchOperator(
    PrivateConstructorTag, absl::string_view name,
    ExprOperatorSignature signature, absl::string_view doc,
    std::vector<Overload> overloads, ExprOperatorPtr absl_nullable default_op,
    DispatchFn absl_nonnull dispatch_fn, Fingerprint fingerprint)
    : ExprOperatorWithFixedSignature(name, std::move(signature), doc,
                                     fingerprint),
      all_overloads_(std::move(overloads)),
      has_default_overload_(default_op != nullptr),
      dispatch_fn_(std::move(dispatch_fn)) {
  if (default_op != nullptr) {
    all_overloads_.reserve(all_overloads_.size() + 1);
    all_overloads_.push_back(Overload{.name = "default",
                                      .op = std::move(default_op),
                                      .condition_expr = Literal(kPresent)});
  }
}

absl::StatusOr<ExprAttributes> DispatchOperator::InferAttributes(
    absl::Span<const ExprAttributes> inputs) const {
  RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
  ASSIGN_OR_RETURN(auto input_qtype_sequence, MakeInputQTypeSequence(inputs));
  ASSIGN_OR_RETURN(
      auto* overload, GetOverload(input_qtype_sequence),
      WithNote(WithNote(std::move(_),
                        absl::StrCat("Input qtypes: ",
                                     FormatInputQTypes(signature(),
                                                       input_qtype_sequence),
                                     ".")),
               absl::StrCat("In dispatch operator: '",
                            absl::Utf8SafeCHexEscape(display_name()), "'.")));
  if (overload == nullptr) {
    return ExprAttributes{};
  }
  ASSIGN_OR_RETURN(
      ExprAttributes attr, overload->op->InferAttributes(inputs),
      WithNote(
          _, absl::StrCat("In dispatch operator: '",
                          absl::Utf8SafeCHexEscape(display_name()), "', case '",
                          absl::Utf8SafeCHexEscape(overload->name), "'.")));
  return attr;
}

absl::StatusOr<ExprNodePtr absl_nonnull> DispatchOperator::ToLowerLevel(
    const ExprNodePtr absl_nonnull& node) const {
  RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
  if (node->qtype() == nullptr) {
    return node;  // We are not ready for lowering yet.
  }
  ASSIGN_OR_RETURN(auto input_qtype_sequence,
                   MakeInputQTypeSequence(node->node_deps()));
  ASSIGN_OR_RETURN(
      auto* overload, GetOverload(input_qtype_sequence),
      WithNote(WithNote(std::move(_),
                        absl::StrCat("Input qtypes: ",
                                     FormatInputQTypes(signature(),
                                                       input_qtype_sequence),
                                     ".")),
               absl::StrCat("In dispatch operator: '",
                            absl::Utf8SafeCHexEscape(display_name()), "'.")));
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
      ExprNodePtr lowered, overload->op->ToLowerLevel(expr),
      WithNote(
          _, absl::StrCat("In dispatch operator: '",
                          absl::Utf8SafeCHexEscape(display_name()), "', case '",
                          absl::Utf8SafeCHexEscape(overload->name), "'.")));
  return lowered;
}

absl::StatusOr<const DispatchOperator::Overload* absl_nullable>
DispatchOperator::GetOverload(const Sequence& input_qtype_sequence) const {
  ASSIGN_OR_RETURN(auto dispatch_result, dispatch_fn_(input_qtype_sequence));
  const size_t n = static_cast<size_t>(dispatch_result.GetFieldCount()) - 1;
  if (!dispatch_result.GetField(0).UnsafeAs<OptionalUnit>()) {
    return nullptr;  // not ready: some required inputs are unknown
  }
  size_t first_matched_index = n;
  size_t i = 0;
  for (; i < n; ++i) {
    if (dispatch_result.GetField(i + 1).UnsafeAs<OptionalUnit>()) {
      first_matched_index = i;
      break;
    }
  }
  std::vector<size_t> secondary_matched_indices;
  for (++i; i < n; ++i) {
    if (dispatch_result.GetField(i + 1).UnsafeAs<OptionalUnit>()) [[unlikely]] {
      secondary_matched_indices.push_back(i);
    }
  }
  if (!secondary_matched_indices.empty()) [[unlikely]] {
    return absl::FailedPreconditionError(absl::StrFormat(
        "multiple overload cases matched: '%s', '%s'",
        absl::Utf8SafeCHexEscape(all_overloads_[first_matched_index].name),
        absl::StrJoin(
            secondary_matched_indices, "', '", [&](std::string* out, size_t i) {
              absl::StrAppend(out,
                              absl::Utf8SafeCHexEscape(all_overloads_[i].name));
            })));
  }
  if (first_matched_index < all_overloads_.size()) [[likely]] {
    return &all_overloads_[first_matched_index];
  }
  return absl::InvalidArgumentError("no suitable overload or default operator");
}

ReprToken DispatchOperator::GenReprToken() const {
  return {absl::StrFormat(
      "<DispatchOperator: name='%s', signature='%s', cases=['%s']>",
      absl::Utf8SafeCHexEscape(display_name()),
      GetExprOperatorSignatureSpec(signature()),
      absl::StrJoin(
          all_overloads_, "', '", [](std::string* out, const auto& overload) {
            absl::StrAppend(out, absl::Utf8SafeCHexEscape(overload.name));
          }))};
}

absl::Span<const DispatchOperator::Overload> DispatchOperator::overloads()
    const {
  if (has_default_overload_) {
    return absl::MakeSpan(all_overloads_.data(), all_overloads_.size() - 1);
  }
  return all_overloads_;
}

ExprOperatorPtr absl_nullable DispatchOperator::default_op() const {
  if (has_default_overload_) {
    return all_overloads_.back().op;
  }
  return nullptr;
}

absl::string_view DispatchOperator::py_qvalue_specialization_key() const {
  return "::arolla::operator_loader::DispatchOperator";
}

}  // namespace arolla::operator_loader
