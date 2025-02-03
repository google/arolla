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
#include "arolla/serialization_base/decoder.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_base {

// Overview
// --------
//
// The Decoder processes a sequence of decoding steps, storing intermediate
// results in `decoding_step_results_`. Each decoding step triggers one of the
// following simple actions:
//
//   * assemble an expression (a literal, a leaf, a placeholder, or an operator
//       node);
//   * assemble a value, which is delegated to the value decoders (also known as
//       codecs);
//   * load a codec;
//   * copy a result (either a value or an expression) to the output.
//
// While a decoding step can produce only a single expression node or a value,
// it can reference the results of previous steps as inputs. Therefore, a series
// of steps can assemble arbitrarily complex entities.
//
// Decoding step results are addressable using the indices assigned by
// the container.
//
//
// Implementation details
// ----------------------
//
// Each decoding step is handled by one of the following methods:
//
//   * Decoder::DecodeLiteralNode(): assembles a literal node;
//
//   * Decoder::DecodeLeafNode(): assembles a leaf node;
//
//   * Decoder::DecodePlaceholderNode(): assembles a placeholder node;
//
//   * Decoder::DecodeCodec(): acquires a value decoder from
//       `value_decoder_provider_`;
//
//   * Decoder::DecodeValue(): delegates the value assembly to the corresponding
//       value decoder; the decoding step can either explicitly reference
//       the needed codec (handled by Decoder::DecodeValueWithKnownCodec());
//       otherwise, the codec needs to be detected (handled by
//       Decoder::DecodeValueWithUnknownCodec()).
//
// The storing of the decoding step results is implemented in the methods:
// Decoder::StoreDecodedValue(), StoreDecodedExpr(), and StoreDecodedCodec().
// The retrieval of the intermediate results occurs in LoadDecodedValue() and
// LoadDecodedExpr().
//
// `decoding_step_results_` stores pointers to values, expressions, and codecs.
// The actual entities reside in the respective containers: `known_values_`,
// `known_exprs_`, and `known_codecs_`. This pointer-based approach enables
// optionality and reduces memory footprint, as a decoding step result can be
// empty.
//

using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::Placeholder;

Decoder::Decoder(ValueDecoderProvider value_decoder_provider,
                 const Options& options)
    : value_decoder_provider_(std::move(value_decoder_provider)),
      options_(options) {}

absl::Status Decoder::OnDecodingStep(
    uint64_t decoding_step_index,
    const DecodingStepProto& decoding_step_proto) {
  if (decoding_step_index > decoding_step_results_.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("encountered unexpected decoding_step_index=%d, "
                        "indicating missing step %d",
                        decoding_step_index, decoding_step_results_.size()));
  }
  if (decoding_step_index == decoding_step_results_.size()) {
    decoding_step_results_.emplace_back();
  }
  switch (decoding_step_proto.type_case()) {
    case DecodingStepProto::kLiteralNode: {
      ASSIGN_OR_RETURN(auto expr,
                       DecodeLiteralNode(decoding_step_proto.literal_node()),
                       _ << "decoding_step.type=LITERAL_NODE");
      return StoreDecodedExpr(decoding_step_index, std::move(expr));
    }
    case DecodingStepProto::kLeafNode: {
      ASSIGN_OR_RETURN(auto expr,
                       DecodeLeafNode(decoding_step_proto.leaf_node()),
                       _ << "decoding_step.type=LEAF_NODE");
      return StoreDecodedExpr(decoding_step_index, std::move(expr));
    }
    case DecodingStepProto::kPlaceholderNode: {
      ASSIGN_OR_RETURN(
          auto expr,
          DecodePlaceholderNode(decoding_step_proto.placeholder_node()),
          _ << "decoding_step.type=PLACEHOLDER_NODE");
      return StoreDecodedExpr(decoding_step_index, std::move(expr));
    }
    case DecodingStepProto::kOperatorNode: {
      ASSIGN_OR_RETURN(auto expr,
                       DecodeOperatorNode(decoding_step_proto.operator_node()),
                       _ << "decoding_step.type=OPERATOR_NODE");
      return StoreDecodedExpr(decoding_step_index, std::move(expr));
    }
    case DecodingStepProto::kValue: {
      ASSIGN_OR_RETURN(auto value, DecodeValue(decoding_step_proto.value()),
                       _ << "decoding_step.type=VALUE");
      return StoreDecodedValue(decoding_step_index, std::move(value));
    }
    case DecodingStepProto::kCodec: {
      ASSIGN_OR_RETURN(auto codec, DecodeCodec(decoding_step_proto.codec()),
                       _ << "decoding_step.type=CODEC");
      return StoreDecodedCodec(decoding_step_index, std::move(codec));
    }
    case DecodingStepProto::kOutputValueIndex: {
      ASSIGN_OR_RETURN(
          auto value,
          LoadDecodedValue(decoding_step_proto.output_value_index()),
          _ << "decoding_step.type=OUTPUT_VALUE_INDEX");
      result_.values.push_back(std::move(value));
      return absl::OkStatus();
    }
    case DecodingStepProto::kOutputExprIndex: {
      ASSIGN_OR_RETURN(auto expr,
                       LoadDecodedExpr(decoding_step_proto.output_expr_index()),
                       _ << "decoding_step.type=OUTPUT_EXPR_INDEX");
      result_.exprs.push_back(std::move(expr));
      return absl::OkStatus();
    }
    case DecodingStepProto::TYPE_NOT_SET:
      return absl::InvalidArgumentError("missing decoding_step.type");
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("unexpected decoding_step.type=%d",
                      static_cast<int>(decoding_step_proto.type_case())));
}

Decoder::Result Decoder::Finish() && { return std::move(result_); }

absl::StatusOr<ExprNodePtr> Decoder::DecodeLiteralNode(
    const LiteralNodeProto& literal_node_proto) const {
  if (!literal_node_proto.has_literal_value_index()) {
    return absl::InvalidArgumentError(
        "missing literal_node.literal_value_index");
  }
  ASSIGN_OR_RETURN(auto value,
                   LoadDecodedValue(literal_node_proto.literal_value_index()));
  return Literal(std::move(value));
}

absl::StatusOr<ExprNodePtr> Decoder::DecodeLeafNode(
    const LeafNodeProto& leaf_node_proto) const {
  if (!leaf_node_proto.has_leaf_key()) {
    return absl::InvalidArgumentError("missing leaf_node.leaf_key");
  }
  return Leaf(leaf_node_proto.leaf_key());
}

absl::StatusOr<ExprNodePtr> Decoder::DecodePlaceholderNode(
    const PlaceholderNodeProto& placeholder_node_proto) const {
  if (!placeholder_node_proto.has_placeholder_key()) {
    return absl::InvalidArgumentError(
        "missing placeholder_node.placeholder_key");
  }
  return Placeholder(placeholder_node_proto.placeholder_key());
}

absl::StatusOr<ExprNodePtr> Decoder::DecodeOperatorNode(
    const OperatorNodeProto& operator_node_proto) const {
  if (!operator_node_proto.has_operator_value_index()) {
    return absl::InvalidArgumentError(
        "missing operator_node.operator_value_index");
  }
  ASSIGN_OR_RETURN(
      auto value, LoadDecodedValue(operator_node_proto.operator_value_index()));
  if (value.GetType() != GetQType<ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected an operator in decoding_step_results[%d], got %s",
        operator_node_proto.operator_value_index(), value.GetType()->name()));
  }
  const auto& op = value.UnsafeAs<ExprOperatorPtr>();
  std::vector<ExprNodePtr> exprs(operator_node_proto.input_expr_indices_size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    ASSIGN_OR_RETURN(
        exprs[i], LoadDecodedExpr(operator_node_proto.input_expr_indices(i)));
  }
  if (options_.infer_attributes_for_operator_nodes) {
    return MakeOpNode(op, std::move(exprs));
  } else {
    return ExprNode::UnsafeMakeOperatorNode(ExprOperatorPtr(op),
                                            std::move(exprs), {});
  }
}

absl::StatusOr<TypedValue> Decoder::DecodeValue(
    const ValueProto& value_proto) const {
  std::vector<TypedValue> input_values;
  input_values.reserve(value_proto.input_value_indices_size());
  for (auto value_index : value_proto.input_value_indices()) {
    ASSIGN_OR_RETURN(auto value, LoadDecodedValue(value_index));
    input_values.push_back(std::move(value));
  }
  std::vector<ExprNodePtr> input_exprs(value_proto.input_expr_indices_size());
  for (size_t i = 0; i < input_exprs.size(); ++i) {
    ASSIGN_OR_RETURN(input_exprs[i],
                     LoadDecodedExpr(value_proto.input_expr_indices(i)));
  }
  if (value_proto.has_codec_index()) {
    return DecodeValueWithKnownCodec(value_proto, value_proto.codec_index(),
                                     input_values, input_exprs);
  }
  return DecodeValueWithUnknownCodec(value_proto, input_values, input_exprs);
}

absl::StatusOr<TypedValue> Decoder::DecodeValueWithKnownCodec(
    const ValueProto& value_proto, uint64_t codec_index,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) const {
  if (static_cast<size_t>(codec_index) >= decoding_step_results_.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("codec_index is out of range: %d", codec_index));
  }
  auto* codec = decoding_step_results_[codec_index].codec;
  if (codec == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "found no codec in decoding_step_results[%d]", codec_index));
  }
  ASSIGN_OR_RETURN(auto value_decoder_result,
                   codec->value_decoder(value_proto, input_values, input_exprs),
                   _ << "codecs[" << codec_index << "]=" << codec->codec_name);
  if (auto* value = std::get_if<TypedValue>(&value_decoder_result)) {
    return std::move(*value);
  }
  return absl::NotFoundError(absl::StrFormat(
      "no extension found; codecs[%d]=%s", codec_index, codec->codec_name));
}

absl::StatusOr<TypedValue> Decoder::DecodeValueWithUnknownCodec(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) const {
  // Try the codecs one by one.
  // NOTE: Use extension number from value_proto, when there is a
  // corresponding api.
  for (const auto& codec : know_codecs_) {
    ASSIGN_OR_RETURN(
        auto value_decoder_result,
        codec.value_decoder(value_proto, input_values, input_exprs),
        _ << "detected_codec=" << codec.codec_name);
    if (auto* value = std::get_if<TypedValue>(&value_decoder_result)) {
      return *value;
    }
  }
  return absl::InvalidArgumentError("unable to detect codec");
}

absl::StatusOr<Decoder::Codec> Decoder::DecodeCodec(
    const CodecProto& codec_proto) const {
  ASSIGN_OR_RETURN(auto value_decoder,
                   value_decoder_provider_(codec_proto.name()));
  DCHECK(value_decoder != nullptr);
  return Codec{codec_proto.name(), std::move(value_decoder)};
}

absl::Status Decoder::StoreDecodedValue(uint64_t value_index,
                                        TypedValue&& value) {
  DCHECK_LT(value_index, decoding_step_results_.size());
  if (decoding_step_results_[value_index].value != nullptr) {
    return absl::InvalidArgumentError("value_index collision");
  }
  decoding_step_results_[value_index].value =
      &know_values_.emplace_back(std::move(value));
  return absl::OkStatus();
}

absl::Status Decoder::StoreDecodedExpr(uint64_t expr_index,
                                       ExprNodePtr&& expr) {
  DCHECK_LT(expr_index, decoding_step_results_.size());
  if (decoding_step_results_[expr_index].expr != nullptr) {
    return absl::InvalidArgumentError("expr_index collision");
  }
  decoding_step_results_[expr_index].expr =
      know_exprs_.emplace_back(std::move(expr)).get();
  return absl::OkStatus();
}

absl::Status Decoder::StoreDecodedCodec(uint64_t codec_index, Codec&& codec) {
  DCHECK_LT(codec_index, decoding_step_results_.size());
  if (decoding_step_results_[codec_index].codec != nullptr) {
    return absl::InvalidArgumentError("codec_index collision");
  }
  decoding_step_results_[codec_index].codec =
      &know_codecs_.emplace_back(std::move(codec));
  return absl::OkStatus();
}

absl::StatusOr<TypedValue> Decoder::LoadDecodedValue(
    uint64_t value_index) const {
  if (static_cast<size_t>(value_index) >= decoding_step_results_.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("value_index is out of range: %d", value_index));
  }
  if (decoding_step_results_[value_index].value == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "found no value in decoding_step_results[%d]", value_index));
  }
  return *decoding_step_results_[value_index].value;
}

absl::StatusOr<ExprNodePtr> Decoder::LoadDecodedExpr(
    uint64_t expr_index) const {
  if (static_cast<size_t>(expr_index) >= decoding_step_results_.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expr_index is out of range: %d", expr_index));
  }
  if (decoding_step_results_[expr_index].expr == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "found no expression in decoding_step_results[%d]", expr_index));
  }
  return ExprNodePtr::NewRef(decoding_step_results_[expr_index].expr);
}

}  // namespace arolla::serialization_base
