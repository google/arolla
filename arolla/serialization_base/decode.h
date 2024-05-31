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
#ifndef AROLLA_SERIALIZATION_BASE_DECODE_H_
#define AROLLA_SERIALIZATION_BASE_DECODE_H_

#include <functional>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::serialization_base {

// Version of the container format.
//
// NOTE: Alterations in codecs do not change the container format.
constexpr int kContainerVersion = 1;

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

// Extra options for decoding.
struct DecodingOptions {
  // Generate metadata for operator nodes; all operator definitions need to be
  // available.
  //
  // NOTE: This option should be removed after switching to expression
  // attributes.
  bool generate_metadata_for_operator_nodes = true;
};

// Return type for Decode().
struct DecodeResult {
  std::vector<TypedValue> values;
  std::vector<arolla::expr::ExprNodePtr> exprs;
};

// Decodes values and expressions from the container.
absl::StatusOr<DecodeResult> Decode(
    const ContainerProto& container_proto,
    const ValueDecoderProvider& value_decoder_provider,
    const DecodingOptions& options = DecodingOptions());

}  // namespace arolla::serialization_base

#endif  // AROLLA_EXPERIMENTAL_PB_SERIALIZE_BASE_DECODE_H_
