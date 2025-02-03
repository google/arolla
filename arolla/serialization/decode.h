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

#include "absl/status/statusor.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decoder.h"

namespace arolla::serialization {

using DecodeResult = ::arolla::serialization_base::Decoder::Result;
using DecodingOptions = ::arolla::serialization_base::Decoder::Options;

// Decodes values and expressions from the container using all value decoders
// from the global registry.
absl::StatusOr<DecodeResult> Decode(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options = {});

}  // namespace arolla::serialization

#endif  // AROLLA_SERIALIZATION_DECODE_H_
