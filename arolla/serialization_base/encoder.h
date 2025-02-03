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
#ifndef AROLLA_SERIALIZATION_BASE_ENCODER_H_
#define AROLLA_SERIALIZATION_BASE_ENCODER_H_

#include <cstdint>
#include <functional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/util/fingerprint.h"

namespace arolla::serialization_base {

class Encoder;

// Returns a ValueProto corresponding to the `value`.
//
// This type represents a stateless value-encoder. The value-encoder can
// recursively call the given `encoder`; in that case the resulting ValueProto
// corresponds only to the last step of the `value` assembling, and the nested
// calls of the `encoder` already got embedded into the `encoder`s state.
//
// Note 1: The value-encoder gets TypedRef instead of TypedValue to make
// an accidental call `encoder.EncodeValue(value)` unlikely.
//
// Note 2: If an encoder returns an error, the state of the encoder is
// unspecified. In particular, the `encoder`s state may already store a part of
// the `value`s state.
//
using ValueEncoder =
    std::function<absl::StatusOr<ValueProto>(TypedRef value, Encoder& encoder)>;

// Encoder class.
//
// The method EncodeValue() serializes a value and returns the corresponding
// decoding_step index. It deduplicates the values based on fingerprints and
// serializes each unique value only once.
//
// EncodeExpr() works the same way for expressions.
//
// ValueProto message is defined in ./base.proto and has the following
// structure:
//
//   message ValueProto {
//     repeated uint64_t input_value_indices = 1;
//     repeated uint64_t input_expr_indices = 2;
//     optional uint64_t codec_index = 3;
//     extensions 326031909 to 524999999;
//   }
//
// Values and expressions referenced in `input_value_indices` and
// `input_expr_indices` of a ValueProto message will be decoded before this
// message and will be available when this message gets decoded.
//
// `codec_index` identifies ValueDecoder needed of this message. (If this field
// is missing, the decoder will try available codecs one by one).
//
// Note: If this class returns an error, the encoder's state is unspecified,
// and the encoding process should be halted.
//
class Encoder {
 public:
  // Construct an instance that writes data to the given `container_proto`.
  explicit Encoder(ValueEncoder value_encoder,
                   ContainerBuilder& container_builder);
  virtual ~Encoder() = default;

  // Non-copyable/non-movable.
  Encoder(const Encoder&) = delete;
  Encoder& operator=(const Encoder&) = delete;

  // Encodes a codec name and returns its index.
  absl::StatusOr<uint64_t> EncodeCodec(absl::string_view codec);

  // Encodes a value and returns its index.
  //
  // NOTE: The method takes TypedValue because TypedValue owns the fingerprint
  // value. With TypedRef it will have to re-calculate it every time.
  absl::StatusOr<uint64_t> EncodeValue(const TypedValue& value);

  // Encodes an expression and returns its index.
  absl::StatusOr<uint64_t> EncodeExpr(const arolla::expr::ExprNodePtr& expr);

 private:
  // Serializes 'push' of a single EXPR node (all dependencies have to be
  // pre-serialized).
  absl::Status EncodeExprNode(const arolla::expr::ExprNode& expr_node);
  absl::Status EncodeLiteralNode(const arolla::expr::ExprNode& expr_node);
  absl::Status EncodeLeafNode(const arolla::expr::ExprNode& expr_node);
  absl::Status EncodePlaceholderNode(const arolla::expr::ExprNode& expr_node);
  absl::Status EncodeOperatorNode(const arolla::expr::ExprNode& expr_node);

  // Value encoder.
  ValueEncoder value_encoder_;

  // Container builder.
  ContainerBuilder& container_builder_;

  // Indicates whether the current EncodeValue/EncodeExpr calls are top-level.
  uint64_t nesting_ = 0;

  // Mapping from a codec name to its index.
  absl::flat_hash_map<std::string, uint64_t> known_codecs_;

  // Dictionary of stored values. It maps a value fingerprint to
  // the corresponding decoding step.
  absl::flat_hash_map<Fingerprint, uint64_t> known_values_;

  // Dictionary of stored expressions. It maps a value fingerprint to
  // the corresponding decoding step.
  absl::flat_hash_map<Fingerprint, uint64_t> known_exprs_;
};

}  // namespace arolla::serialization_base

#endif  // AROLLA_SERIALIZATION_BASE_ENCODER_H_
