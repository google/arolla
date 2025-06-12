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
#include "arolla/serialization/decode.h"

#include <utility>

#include "absl/status/statusor.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container_proto.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization {

using ::arolla::serialization_base::ContainerProto;
using ::arolla::serialization_base::Decoder;
using ::arolla::serialization_base::ProcessContainerProto;
using ::arolla::serialization_codecs::CodecBasedValueDecoderProvider;

absl::StatusOr<DecodeResult> Decode(const ContainerProto& container_proto,
                                    const DecodingOptions& options) {
  Decoder decoder(CodecBasedValueDecoderProvider(), options);
  RETURN_IF_ERROR(ProcessContainerProto(container_proto, decoder));
  return std::move(decoder).Finish();
}

}  // namespace arolla::serialization
