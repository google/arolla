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
#include "arolla/serialization_base/container_proto.h"

#include <cstdint>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/serialization_base/container.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization_base {

absl::StatusOr<uint64_t> ContainerProtoBuilder::Add(
    DecodingStepProto&& decoding_step_proto) {
  switch (decoding_step_proto.type_case()) {
    case DecodingStepProto::kCodec:
      *result_.add_codecs() = std::move(*decoding_step_proto.mutable_codec());
      return result_.codecs_size() - 1;

    case DecodingStepProto::kOutputValueIndex:
      result_.add_output_value_indices(
          decoding_step_proto.output_value_index());
      return result_.output_value_indices_size() - 1;

    case DecodingStepProto::kOutputExprIndex:
      result_.add_output_expr_indices(decoding_step_proto.output_expr_index());
      return result_.output_expr_indices_size() - 1;

    default:
      *result_.add_decoding_steps() = std::move(decoding_step_proto);
      return result_.decoding_steps_size() - 1;
  }
}

ContainerProto ContainerProtoBuilder::Finish() && {
  result_.set_version(kContainerProtoVersion);
  return std::move(result_);
}

absl::Status ProcessContainerProto(const ContainerProto& container_proto,
                                   ContainerProcessor& container_processor) {
  constexpr int kSupportedContainerProtoVersion = 1;
  if (!container_proto.has_version()) {
    return absl::InvalidArgumentError("missing container.version");
  }
  if (container_proto.version() != kSupportedContainerProtoVersion) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected container.version to be %d, got %d",
        kSupportedContainerProtoVersion, container_proto.version()));
  }
  DecodingStepProto decoding_step;
  // Emit decoding steps corresponding to the codec messages.
  for (int codec_index = 0; codec_index < container_proto.codecs_size();
       ++codec_index) {
    *decoding_step.mutable_codec() = container_proto.codecs(codec_index);
    RETURN_IF_ERROR(
        container_processor.OnDecodingStep(codec_index, decoding_step))
        << "while handling codecs[" << codec_index << "]";
  }
  // Emit the stored decoding steps.
  for (int decoding_step_index = 0;
       decoding_step_index < container_proto.decoding_steps_size();
       ++decoding_step_index) {
    RETURN_IF_ERROR(container_processor.OnDecodingStep(
        decoding_step_index,
        container_proto.decoding_steps(decoding_step_index)))
        << "while handling decoding_steps[" << decoding_step_index << "]";
  }
  // Emit decoding steps corresponding to the output values.
  for (int i = 0; i < container_proto.output_value_indices_size(); ++i) {
    decoding_step.set_output_value_index(
        container_proto.output_value_indices(i));
    RETURN_IF_ERROR(container_processor.OnDecodingStep(0, decoding_step))
        << "while handling output_value_indices[" << i << "]";
  }
  // Emit decoding steps corresponding to the output expressions.
  for (int i = 0; i < container_proto.output_expr_indices_size(); ++i) {
    decoding_step.set_output_expr_index(container_proto.output_expr_indices(i));
    RETURN_IF_ERROR(container_processor.OnDecodingStep(0, decoding_step))
        << "while handling output_expr_indices[" << i << "]";
  }
  return absl::OkStatus();
}

}  // namespace arolla::serialization_base
