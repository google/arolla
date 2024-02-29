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
#include "arolla/serialization_base/encode.h"

#include <cstdint>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decode.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_base {

using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprNodeType;
using ::arolla::expr::VisitorOrder;

absl::StatusOr<ContainerProto> Encode(absl::Span<const TypedValue> values,
                                      absl::Span<const ExprNodePtr> exprs,
                                      ValueEncoder value_encoder) {
  ContainerProto result;
  Encoder encoder(value_encoder, result);
  for (const auto& value : values) {
    ASSIGN_OR_RETURN(auto value_idx, encoder.EncodeValue(value));
    result.add_output_value_indices(value_idx);
  }
  for (const auto& expr : exprs) {
    ASSIGN_OR_RETURN(auto expr_idx, encoder.EncodeExpr(expr));
    result.add_output_expr_indices(expr_idx);
  }
  return result;
}

Encoder::Encoder(ValueEncoder value_encoder, ContainerProto& container_proto)
    : value_encoder_(std::move(value_encoder)),
      container_proto_(container_proto) {
  container_proto_.set_version(kContainerVersion);
}

int64_t Encoder::EncodeCodec(absl::string_view codec) {
  auto it = known_codecs_.find(codec);
  if (it == known_codecs_.end()) {
    it = known_codecs_.emplace(codec, known_codecs_.size()).first;
    DCHECK(container_proto_.codecs().size() == it->second);
    container_proto_.add_codecs()->set_name(codec.data(), codec.size());
  }
  return it->second;
}

absl::StatusOr<int64_t> Encoder::EncodeValue(const TypedValue& value) {
  const auto fingerprint = value.GetFingerprint();
  auto it = known_values_.find(fingerprint);
  if (it == known_values_.end()) {
    ASSIGN_OR_RETURN(auto value_proto, value_encoder_(value.AsRef(), *this));
    *container_proto_.add_decoding_steps()->mutable_value() =
        std::move(value_proto);
    it = known_values_
             .emplace(fingerprint, container_proto_.decoding_steps().size() - 1)
             .first;
  }
  return it->second;
}

absl::StatusOr<int64_t> Encoder::EncodeExpr(const ExprNodePtr& expr) {
  if (expr == nullptr) {
    return absl::InvalidArgumentError("expr is nullptr");
  }
  const auto fingerprint = expr->fingerprint();
  auto it = known_exprs_.find(fingerprint);
  if (it == known_exprs_.end()) {
    for (const auto& expr_node : VisitorOrder(expr)) {
      RETURN_IF_ERROR(EncodeExprNode(*expr_node));
    }
    it = known_exprs_.find(fingerprint);
  }
  return it->second;
}

absl::Status Encoder::EncodeExprNode(const ExprNode& expr_node) {
  if (known_exprs_.contains(expr_node.fingerprint())) {
    return absl::OkStatus();
  }
  switch (expr_node.type()) {
    case ExprNodeType::kLiteral:
      return EncodeLiteralNode(expr_node);
    case ExprNodeType::kLeaf:
      return EncodeLeafNode(expr_node);
    case ExprNodeType::kPlaceholder:
      return EncodePlaceholderNode(expr_node);
    case ExprNodeType::kOperator:
      return EncodeOperatorNode(expr_node);
  }
  return absl::FailedPreconditionError(absl::StrFormat(
      "Unexpected ExprNodeType(%d)", static_cast<int>(expr_node.type())));
}

absl::Status Encoder::EncodeLiteralNode(const ExprNode& expr_node) {
  ASSIGN_OR_RETURN(auto value_index, EncodeValue(*expr_node.qvalue()));
  container_proto_.add_decoding_steps()
      ->mutable_literal_node()
      ->set_literal_value_index(value_index);
  known_exprs_.emplace(expr_node.fingerprint(),
                       container_proto_.decoding_steps().size() - 1);
  return absl::OkStatus();
}

absl::Status Encoder::EncodeLeafNode(const ExprNode& expr_node) {
  container_proto_.add_decoding_steps()->mutable_leaf_node()->set_leaf_key(
      expr_node.leaf_key());
  known_exprs_.emplace(expr_node.fingerprint(),
                       container_proto_.decoding_steps().size() - 1);
  return absl::OkStatus();
}

absl::Status Encoder::EncodePlaceholderNode(const ExprNode& expr_node) {
  container_proto_.add_decoding_steps()
      ->mutable_placeholder_node()
      ->set_placeholder_key(expr_node.placeholder_key());
  known_exprs_.emplace(expr_node.fingerprint(),
                       container_proto_.decoding_steps().size() - 1);
  return absl::OkStatus();
}

absl::Status Encoder::EncodeOperatorNode(const ExprNode& expr_node) {
  DecodingStepProto decoding_step;
  auto* operator_node_proto = decoding_step.mutable_operator_node();
  ASSIGN_OR_RETURN(auto operator_value_index,
                   EncodeValue(TypedValue::FromValue(expr_node.op())));
  operator_node_proto->set_operator_value_index(operator_value_index);
  for (const auto& node_dep : expr_node.node_deps()) {
    auto it = known_exprs_.find(node_dep->fingerprint());
    if (it == known_exprs_.end()) {
      return absl::FailedPreconditionError(
          "Node dependencies must to be pre-serialized.");
    }
    operator_node_proto->add_input_expr_indices(it->second);
  }
  *container_proto_.add_decoding_steps() = std::move(decoding_step);
  known_exprs_.emplace(expr_node.fingerprint(),
                       container_proto_.decoding_steps().size() - 1);
  return absl::OkStatus();
}

}  // namespace arolla::serialization_base
