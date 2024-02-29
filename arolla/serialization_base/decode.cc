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
#include "arolla/serialization_base/decode.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_base {
namespace {

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::Placeholder;

// A helper class that holds together the decoder's state.
class Decoder {
 public:
  absl::StatusOr<DecodeResult> Run(
      const ContainerProto& container_proto,
      const ValueDecoderProvider& value_decoder_provider,
      const DecodingOptions& options) {
    options_ = options;
    RETURN_IF_ERROR(InitValueDecoders(container_proto, value_decoder_provider));
    for (int decoding_step_idx = 0;
         decoding_step_idx < container_proto.decoding_steps_size();
         ++decoding_step_idx) {
      RETURN_IF_ERROR(
          HandleDecodingStep(container_proto.decoding_steps(decoding_step_idx)))
          << "while handling decoding_steps[" << decoding_step_idx << "]";
    }
    DecodeResult result;
    ASSIGN_OR_RETURN(result.values,
                     LoadDecodedValues(container_proto.output_value_indices()),
                     _ << "while loading output values");
    ASSIGN_OR_RETURN(result.exprs,
                     LoadDecodedExprs(container_proto.output_expr_indices()),
                     _ << "while loading output expressions");
    return result;
  }

 private:
  absl::Status HandleDecodingStep(
      const DecodingStepProto& decoding_step_proto) {
    switch (decoding_step_proto.type_case()) {
      case DecodingStepProto::kLiteralNode: {
        ASSIGN_OR_RETURN(auto expr,
                         DecodeLiteralNode(decoding_step_proto.literal_node()),
                         _ << "decoding_step.type=LITERAL_NODE");
        decoding_step_results_.emplace_back(std::move(expr));
        return absl::OkStatus();
      }
      case DecodingStepProto::kLeafNode: {
        ASSIGN_OR_RETURN(auto expr,
                         DecodeLeafNode(decoding_step_proto.leaf_node()),
                         _ << "decoding_step.type=LEAF_NODE");
        decoding_step_results_.emplace_back(std::move(expr));
        return absl::OkStatus();
      }
      case DecodingStepProto::kPlaceholderNode: {
        ASSIGN_OR_RETURN(
            auto expr,
            DecodePlaceholderNode(decoding_step_proto.placeholder_node()),
            _ << "decoding_step.type=PLACEHOLDER_NODE");
        decoding_step_results_.emplace_back(std::move(expr));
        return absl::OkStatus();
      }
      case DecodingStepProto::kOperatorNode: {
        ASSIGN_OR_RETURN(
            auto expr, DecodeOperatorNode(decoding_step_proto.operator_node()),
            _ << "decoding_step.type=OPERATOR_NODE");
        decoding_step_results_.emplace_back(std::move(expr));
        return absl::OkStatus();
      }
      case DecodingStepProto::kValue: {
        ASSIGN_OR_RETURN(auto value, DecodeValue(decoding_step_proto.value()),
                         _ << "decoding_step.type=VALUE");
        decoding_step_results_.emplace_back(std::move(value));
        return absl::OkStatus();
      }
      case DecodingStepProto::TYPE_NOT_SET: {
        return absl::InvalidArgumentError("missing decoding_step.type");
      }
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("unexpected decoding_step.type=%d",
                        static_cast<int>(decoding_step_proto.type_case())));
  }

  absl::StatusOr<ExprNodePtr> DecodeLiteralNode(
      const LiteralNodeProto& literal_node_proto) const {
    if (!literal_node_proto.has_literal_value_index()) {
      return absl::InvalidArgumentError(
          "missing literal_node.literal_value_index");
    }
    ASSIGN_OR_RETURN(
        auto values,
        LoadDecodedValues({literal_node_proto.literal_value_index()}));
    DCHECK(!values.empty());  // values.size() is exactly 1 because we pass
                              // one value index
    return Literal(values.front());
  }

  absl::StatusOr<ExprNodePtr> DecodeLeafNode(
      const LeafNodeProto& leaf_node_proto) const {
    if (!leaf_node_proto.has_leaf_key()) {
      return absl::InvalidArgumentError("missing leaf_node.leaf_key");
    }
    return Leaf(leaf_node_proto.leaf_key());
  }

  absl::StatusOr<ExprNodePtr> DecodePlaceholderNode(
      const PlaceholderNodeProto& placeholder_node_proto) const {
    if (!placeholder_node_proto.has_placeholder_key()) {
      return absl::InvalidArgumentError(
          "missing placeholder_node.placeholder_key");
    }
    return Placeholder(placeholder_node_proto.placeholder_key());
  }

  absl::StatusOr<ExprNodePtr> DecodeOperatorNode(
      const OperatorNodeProto& operator_node_proto) const {
    if (!operator_node_proto.has_operator_value_index()) {
      return absl::InvalidArgumentError(
          "missing operator_node.operator_value_index");
    }
    ASSIGN_OR_RETURN(
        auto values,
        LoadDecodedValues({operator_node_proto.operator_value_index()}));
    DCHECK(!values.empty());  // values.size() is exactly 1 because we pass
                              // one value index
    const auto& value = values[0];
    if (value.GetType() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a value of %s type in decoding_steps[%d], got %s",
          GetQType<ExprOperatorPtr>()->name(),
          operator_node_proto.operator_value_index(), value.GetType()->name()));
    }
    const auto& op = value.UnsafeAs<ExprOperatorPtr>();
    ASSIGN_OR_RETURN(
        auto exprs, LoadDecodedExprs(operator_node_proto.input_expr_indices()));
    if (options_.generate_metadata_for_operator_nodes) {
      return MakeOpNode(op, std::move(exprs));
    } else {
      return ExprNode::UnsafeMakeOperatorNode(
          ExprOperatorPtr(op), std::move(exprs), ExprAttributes{});
    }
  }

  absl::StatusOr<TypedValue> DecodeValueWithKnownCodec(
      const ValueProto& value_proto, int64_t codec_index,
      absl::Span<const TypedValue> input_values,
      absl::Span<const ExprNodePtr> input_exprs) const {
    if (static_cast<size_t>(codec_index) >= value_decoders_.size() ||
        static_cast<size_t>(codec_index) >= codec_names_.size()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("codec index is out of range: %d", codec_index));
    }
    ASSIGN_OR_RETURN(
        auto value_decoder_result,
        value_decoders_[codec_index](value_proto, input_values, input_exprs),
        _ << "codecs[" << codec_index << "]=" << codec_names_[codec_index]);
    if (auto* value = std::get_if<TypedValue>(&value_decoder_result)) {
      return *value;
    }
    return absl::NotFoundError(absl::StrCat("no extension found; codecs[",
                                            codec_index,
                                            "]=", codec_names_[codec_index]));
  }

  absl::StatusOr<TypedValue> DecodeValueWithUnknownCodec(
      const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
      absl::Span<const ExprNodePtr> input_exprs) const {
    // Try the codecs one by one.
    // NOTE: Use extension number from value_proto, when there is a
    // corresponding api.
    for (size_t codec_index = 0; codec_index < value_decoders_.size() &&
                                 codec_index < codec_names_.size();
         ++codec_index) {
      ASSIGN_OR_RETURN(
          auto value_decoder_result,
          value_decoders_[codec_index](value_proto, input_values, input_exprs),
          _ << "codecs[" << codec_index << "]=" << codec_names_[codec_index]);
      if (auto* value = std::get_if<TypedValue>(&value_decoder_result)) {
        return *value;
      }
    }
    return absl::InvalidArgumentError("unable to detect codec");
  }

  absl::StatusOr<TypedValue> DecodeValue(const ValueProto& value_proto) const {
    ASSIGN_OR_RETURN(auto input_values,
                     LoadDecodedValues(value_proto.input_value_indices()));
    ASSIGN_OR_RETURN(auto input_exprs,
                     LoadDecodedExprs(value_proto.input_expr_indices()));
    if (value_proto.has_codec_index()) {
      return DecodeValueWithKnownCodec(value_proto, value_proto.codec_index(),
                                       input_values, input_exprs);
    }
    return DecodeValueWithUnknownCodec(value_proto, input_values, input_exprs);
  }

  absl::StatusOr<std::vector<TypedValue>> LoadDecodedValues(
      absl::Span<const int64_t> value_indices) const {
    std::vector<TypedValue> result;
    result.reserve(value_indices.size());
    for (auto value_idx : value_indices) {
      if (static_cast<size_t>(value_idx) >= decoding_step_results_.size()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("value index is out of range: %d", value_idx));
      }
      const auto* const value =
          std::get_if<TypedValue>(&decoding_step_results_[value_idx]);
      if (value == nullptr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected a value in decoding_steps[%d], got an expression",
            value_idx));
      }
      result.push_back(*value);
    }
    return result;
  }

  absl::StatusOr<std::vector<ExprNodePtr>> LoadDecodedExprs(
      absl::Span<const int64_t> expr_indices) const {
    std::vector<ExprNodePtr> result;
    result.reserve(expr_indices.size());
    for (auto expr_idx : expr_indices) {
      if (static_cast<size_t>(expr_idx) >= decoding_step_results_.size()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("expr index is out of range: %d", expr_idx));
      }
      const auto* const expr =
          std::get_if<ExprNodePtr>(&decoding_step_results_[expr_idx]);
      if (expr == nullptr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected an expression in decoding_steps[%d], got a value",
            expr_idx));
      }
      result.push_back(*expr);
    }
    return result;
  }

  // Initializes value decoder indexing.
  absl::Status InitValueDecoders(
      const ContainerProto& container_proto,
      const ValueDecoderProvider& value_decoder_provider) {
    codec_names_.reserve(container_proto.codecs_size());
    value_decoders_.reserve(container_proto.codecs_size());
    std::vector<absl::string_view> unknown_codecs;
    for (const auto& codec : container_proto.codecs()) {
      auto value_decoder = value_decoder_provider(codec.name());
      if (value_decoder == nullptr) {
        unknown_codecs.push_back(codec.name());
      } else {
        codec_names_.push_back(codec.name());
        value_decoders_.push_back(std::move(value_decoder));
      }
    }
    if (!unknown_codecs.empty()) {
      constexpr absl::string_view suggested_dependency =
          "adding "
          "\"@arolla://arolla/qexpr/serialization_codecs:all_decoders\" "
          "build dependency may help";
      return absl::InvalidArgumentError(absl::StrFormat(
          "unknown codecs: %s; %s.", absl::StrJoin(unknown_codecs, ", "),
          suggested_dependency));
    }
    return absl::OkStatus();
  }

  DecodingOptions options_;

  // Active codecs.
  std::vector<absl::string_view> codec_names_;
  std::vector<ValueDecoder> value_decoders_;

  // Past decoding step results.
  using DecodingStepResult = std::variant<TypedValue, ExprNodePtr>;
  std::vector<DecodingStepResult> decoding_step_results_;
};

}  // namespace

absl::StatusOr<DecodeResult> Decode(
    const ContainerProto& container_proto,
    const ValueDecoderProvider& value_decoder_provider,
    const DecodingOptions& options) {
  if (!container_proto.has_version()) {
    return absl::InvalidArgumentError("missing container.version");
  }
  if (container_proto.version() != kContainerVersion) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected container.version to be %d, got %d",
                        kContainerVersion, container_proto.version()));
  }
  return Decoder().Run(container_proto, value_decoder_provider, options);
}

}  // namespace arolla::serialization_base
