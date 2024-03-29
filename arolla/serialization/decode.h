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
#ifndef AROLLA_SERIALIZATION_DECODE_H_
#define AROLLA_SERIALIZATION_DECODE_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decode.h"

namespace arolla::serialization {

using ::arolla::serialization_base::DecodeResult;
using ::arolla::serialization_base::DecodingOptions;

// Decodes values and expressions from the container using all value decoders
// from the global registry.
absl::StatusOr<DecodeResult> Decode(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options = {});

// Decodes an expression from the container, returns an error if there is not
// just one expression.
absl::StatusOr<arolla::expr::ExprNodePtr> DecodeExpr(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options = {});

// Decodes a value from the container, returns an error if there is not
// just one value.
absl::StatusOr<TypedValue> DecodeValue(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options = {});

// Add a value decoder to the global registry.
absl::Status RegisterValueDecoder(
    absl::string_view codec_name,
    arolla::serialization_base::ValueDecoder value_decoder);

}  // namespace arolla::serialization

#endif  // AROLLA_SERIALIZATION_DECODE_H_
