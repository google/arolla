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
#ifndef AROLLA_SERIALIZATION_BASE_DECODER_H_
#define AROLLA_SERIALIZATION_BASE_DECODER_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"

namespace arolla::serialization_base {

// An indicator for a NoExtensionFound case.
struct NoExtensionFound {};

// Return type for ValueDecoder.
//
// NoExtensionFound indicates that the corresponding extension message wasn't
// found. If Decode() has a list of codecs to try, it should try the next one.
using ValueDecoderResult = std::variant<TypedValue, NoExtensionFound>;

// A stateless value decoder.
//
// Returns a value re-constructed from the given message and pre-decoded values
// and expressions.
//
// Sets NoExtensionFound if `value_proto` contains no corresponding
// extension.
using ValueDecoder = std::function<absl::StatusOr<ValueDecoderResult>(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const arolla::expr::ExprNodePtr> input_exprs)>;

// A provider for value decoders. Returns ValueDecoder{nullptr} if no decoder
// available.
using ValueDecoderProvider =
    std::function<absl::StatusOr<ValueDecoder>(absl::string_view codec_name)>;

// The Arolla serialization format decoder.
//
// Note 1: This is rather a low-level class; in the client code, please consider
// using arolla::serialization::Decode().
//
// Note 2: If this class returns an error, the decoder's state is unspecified,
// and the decoding process should be halted.
//
class Decoder final : public ContainerProcessor {
 public:
  // Configuration for the decoder.
  struct Options {
    // If true, infers attributes for operator nodes. This requires all
    // operator definitions to be available.
    //
    // NOTE: Setting this to `false` allows deserializing expressions that
    // contain operators not present in the registry. This is useful for
    // inspecting the expression structure, even if execution is not possible.
    bool infer_attributes_for_operator_nodes = true;

    // Provider for value decoders. This field is required and must support
    // all codecs present in the serialized data.
    ValueDecoderProvider value_decoder_provider = nullptr;
  };

  // The decoding result.
  struct Result {
    std::vector<TypedValue> values;
    std::vector<arolla::expr::ExprNodePtr> exprs;
  };

  // Constructor.
  //
  // NOTE: See the `Options` struct for configuration details.
  explicit Decoder(Options options);

  // Updates the state using information from the next decoding step.
  absl::Status OnDecodingStep(
      uint64_t decoding_step_index,
      const DecodingStepProto& decoding_step_proto) final;

  // Returns the decoding result. This method should only be called once, after
  // all decoding steps have been processed.
  Result Finish() &&;

 private:
  // (See .cc file for implementation details.)

  using ValueOrExpr =
      std::variant<std::monostate, TypedValue, arolla::expr::ExprNodePtr>;

  struct Codec {
    std::string codec_name;
    ValueDecoder value_decoder;
  };

  struct DecodingStepResult {
    ValueOrExpr value_or_expr;
    const Codec* codec = nullptr;
    size_t usage_count = 0;
  };

  absl::Status CheckUsageCount(uint64_t step_index);

  absl::Status ProcessDecoderHints(const DecoderHintsProto& hints);

  absl::StatusOr<arolla::expr::ExprNodePtr> DecodeLiteralNode(
      const LiteralNodeProto& literal_node_proto);

  absl::StatusOr<arolla::expr::ExprNodePtr> DecodeLeafNode(
      const LeafNodeProto& leaf_node_proto) const;

  absl::StatusOr<arolla::expr::ExprNodePtr> DecodePlaceholderNode(
      const PlaceholderNodeProto& placeholder_node_proto) const;

  absl::StatusOr<arolla::expr::ExprNodePtr> DecodeOperatorNode(
      const OperatorNodeProto& operator_node_proto);

  absl::StatusOr<TypedValue> DecodeValue(const ValueProto& value_proto);
  absl::StatusOr<TypedValue> DecodeValueWithKnownCodec(
      const ValueProto& value_proto, uint64_t codec_index,
      absl::Span<const TypedValue> input_values,
      absl::Span<const arolla::expr::ExprNodePtr> input_exprs);
  absl::StatusOr<TypedValue> DecodeValueWithUnknownCodec(
      const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
      absl::Span<const arolla::expr::ExprNodePtr> input_exprs) const;

  absl::StatusOr<Codec> DecodeCodec(const CodecProto& codec_proto) const;

  absl::Status StoreDecodedValueOrExpr(uint64_t index,
                                       ValueOrExpr&& value_or_expr);
  absl::Status StoreDecodedValue(uint64_t value_index, TypedValue&& value);
  absl::Status StoreDecodedExpr(uint64_t expr_index,
                                arolla::expr::ExprNodePtr&& expr);
  absl::Status StoreDecodedCodec(uint64_t codec_index, Codec&& codec);

  absl::StatusOr<ValueOrExpr> LoadDecodedValueOrExpr(uint64_t index);
  absl::StatusOr<TypedValue> LoadDecodedValue(uint64_t value_index);
  absl::StatusOr<arolla::expr::ExprNodePtr> LoadDecodedExpr(
      uint64_t expr_index);

  ValueDecoderProvider value_decoder_provider_;
  Options options_;

  std::vector<size_t> expected_usage_counts_;
  std::vector<DecodingStepResult> decoding_step_results_;
  std::deque<Codec> known_codecs_;

  Result result_;
};

}  // namespace arolla::serialization_base

#endif  // AROLLA_SERIALIZATION_BASE_DECODER_H_
