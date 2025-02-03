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
#include "arolla/serialization_base/encoder.h"

#include <cstdint>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_base {

using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprNodeType;
using ::arolla::expr::VisitorOrder;

Encoder::Encoder(ValueEncoder value_encoder,
                 ContainerBuilder& container_builder)
    : value_encoder_(std::move(value_encoder)),
      container_builder_(container_builder) {}

absl::StatusOr<uint64_t> Encoder::EncodeValue(const TypedValue& value) {
  uint64_t value_index;
  const auto fingerprint = value.GetFingerprint();
  if (auto it = known_values_.find(fingerprint); it != known_values_.end()) {
    value_index = it->second;
  } else {
    nesting_ += 1;
    absl::Cleanup guard = [&] { nesting_ -= 1; };
    DecodingStepProto decoding_step_proto;
    ASSIGN_OR_RETURN(*decoding_step_proto.mutable_value(),
                     value_encoder_(value.AsRef(), *this));
    ASSIGN_OR_RETURN(value_index,
                     container_builder_.Add(std::move(decoding_step_proto)));
    known_values_[fingerprint] = value_index;
  }
  if (nesting_ == 0) {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.set_output_value_index(value_index);
    RETURN_IF_ERROR(
        container_builder_.Add(std::move(decoding_step_proto)).status());
  }
  return value_index;
}

absl::StatusOr<uint64_t> Encoder::EncodeCodec(absl::string_view codec) {
  if (auto it = known_codecs_.find(codec); it != known_codecs_.end()) {
    return it->second;
  }
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_codec()->set_name(codec.data(), codec.size());
  ASSIGN_OR_RETURN(auto codec_index,
                   container_builder_.Add(std::move(decoding_step_proto)));
  known_codecs_[codec] = codec_index;
  return codec_index;
}

absl::StatusOr<uint64_t> Encoder::EncodeExpr(const ExprNodePtr& expr) {
  if (expr == nullptr) {
    return absl::InvalidArgumentError("expr is nullptr");
  }
  uint64_t expr_index;
  const auto fingerprint = expr->fingerprint();
  if (auto it = known_exprs_.find(fingerprint); it != known_exprs_.end()) {
    expr_index = it->second;
  } else {
    nesting_ += 1;
    absl::Cleanup guard = [&] { nesting_ -= 1; };
    for (const auto& expr_node : VisitorOrder(expr)) {
      RETURN_IF_ERROR(EncodeExprNode(*expr_node));
    }
    expr_index = known_exprs_.at(fingerprint);
  }
  if (nesting_ == 0) {
    DecodingStepProto decoding_step_proto;
    decoding_step_proto.set_output_expr_index(expr_index);
    RETURN_IF_ERROR(
        container_builder_.Add(std::move(decoding_step_proto)).status());
  }
  return expr_index;
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
      "unexpected ExprNodeType(%d)", static_cast<int>(expr_node.type())));
}

absl::Status Encoder::EncodeLiteralNode(const ExprNode& expr_node) {
  ASSIGN_OR_RETURN(auto value_index, EncodeValue(*expr_node.qvalue()));
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_literal_node()->set_literal_value_index(
      value_index);
  ASSIGN_OR_RETURN(known_exprs_[expr_node.fingerprint()],
                   container_builder_.Add(std::move(decoding_step_proto)));
  return absl::OkStatus();
}

absl::Status Encoder::EncodeLeafNode(const ExprNode& expr_node) {
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_leaf_node()->set_leaf_key(expr_node.leaf_key());
  ASSIGN_OR_RETURN(known_exprs_[expr_node.fingerprint()],
                   container_builder_.Add(std::move(decoding_step_proto)));
  return absl::OkStatus();
}

absl::Status Encoder::EncodePlaceholderNode(const ExprNode& expr_node) {
  DecodingStepProto decoding_step_proto;
  decoding_step_proto.mutable_placeholder_node()->set_placeholder_key(
      expr_node.placeholder_key());
  ASSIGN_OR_RETURN(known_exprs_[expr_node.fingerprint()],
                   container_builder_.Add(std::move(decoding_step_proto)));
  return absl::OkStatus();
}

absl::Status Encoder::EncodeOperatorNode(const ExprNode& expr_node) {
  DecodingStepProto decoding_step_proto;
  auto* operator_node_proto = decoding_step_proto.mutable_operator_node();
  ASSIGN_OR_RETURN(auto operator_value_index,
                   EncodeValue(TypedValue::FromValue(expr_node.op())));
  operator_node_proto->set_operator_value_index(operator_value_index);
  for (const auto& node_dep : expr_node.node_deps()) {
    auto it = known_exprs_.find(node_dep->fingerprint());
    if (it == known_exprs_.end()) {
      return absl::FailedPreconditionError(
          "node dependencies must to be pre-serialized");
    }
    operator_node_proto->add_input_expr_indices(it->second);
  }
  ASSIGN_OR_RETURN(known_exprs_[expr_node.fingerprint()],
                   container_builder_.Add(std::move(decoding_step_proto)));
  return absl::OkStatus();
}

}  // namespace arolla::serialization_base
