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
#ifndef AROLLA_SERIALIZATION_CODECS_REGISTRY_H_
#define AROLLA_SERIALIZATION_CODECS_REGISTRY_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_base/encoder.h"

namespace arolla::serialization_codecs {

// The dispatching algorithm for the value encoders:
//
// * A simplifying assumption: a codec responsible for serialization of
//   a qtype, is also responsible for serialization of values of that qtype.
//
// * If a value is QType:
//   (q0) lookup based on the qtype *value*
//   (q1) lookup based on the qvalue_specialisation_key
//
// * If a value is not QType:
//   (p0) lookup based on the qvalue_specialisation_key of the value
//   (p1) lookup based on the value *qtype*
//   (p2) lookup based on the qvalue_specialisation_key of the value qtype
//
// Motivation of the algorithm steps:
//
//   q0 -- helps with static qtypes, like the standard scalars/optionals/arrays
//   q1 -- helps with dynamic qtype families, like ::arolla::TupleQType.
//
//   p0 -- enables fine grained dispatching for values of generic qtypes, like
//         ExprOperator
//   p1 -- helps with static qtypes (similar to q0)
//   p2 -- works for dynamic qtype families (similar to q1), when there is no
//         need in a qvalue_specialized_key at the value level
//

// Add a value encoder for the given qtype to the global registry.
absl::Status RegisterValueEncoderByQType(
    QTypePtr qtype, arolla::serialization_base::ValueEncoder value_encoder);

// Add a value encoder for the given qvalue specialisation key to the global
// registry.
absl::Status RegisterValueEncoderByQValueSpecialisationKey(
    absl::string_view key,
    arolla::serialization_base::ValueEncoder value_encoder);

// Adds a value decoder to the global registry.
absl::Status RegisterValueDecoder(
    absl::string_view codec_name,
    arolla::serialization_base::ValueDecoder value_decoder);

// A ValueEncoder that utilizes the codec registry.
class CodecBasedValueEncoder {
 public:
  absl::StatusOr<arolla::serialization_base::ValueProto> operator()(
      TypedRef value, arolla::serialization_base::Encoder& encoder) const;
};

// A ValueDecoderProvider that utilizes the codec registry.
class CodecBasedValueDecoderProvider {
 public:
  absl::StatusOr<arolla::serialization_base::ValueDecoder> operator()(
      absl::string_view codec_name) const;
};

}  // namespace arolla::serialization_codecs

#endif  // AROLLA_SERIALIZATION_CODECS_REGISTRY_H_
