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
#include "arolla/serialization/riegeli.h"

#include <cstdint>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "riegeli/bytes/string_reader.h"
#include "riegeli/bytes/string_writer.h"
#include "riegeli/records/record_reader.h"
#include "riegeli/records/record_writer.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization {
namespace {

using ::arolla::serialization_base::ContainerBuilder;
using ::arolla::serialization_base::ContainerProcessor;
using ::arolla::serialization_base::Decoder;
using ::arolla::serialization_base::DecodingStepProto;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_codecs::CodecBasedValueDecoderProvider;
using ::arolla::serialization_codecs::CodecBasedValueEncoder;

// NOTE: We use a "stop" record -– a decoding step with no fields set -– to mark
// the end of a container's records. This enables some degree of composability
// for the record sequences, allowing to embed a sequence of container records
// within a broader sequence of records.
//
class RiegeliContainerBuilder final : public ContainerBuilder {
 public:
  explicit RiegeliContainerBuilder(riegeli::RecordWriterBase& record_writer)
      : record_writer_(record_writer) {}

  absl::StatusOr<uint64_t> Add(DecodingStepProto&& decoding_step_proto) final {
    if (!record_writer_.WriteRecord(decoding_step_proto)) {
      return absl::InvalidArgumentError("failed to write a decoding step");
    }
    return record_count_++;
  }

  // Add a "stop" record, so the decoder knew where to stop.
  absl::Status Finish() && {
    if (!record_writer_.WriteRecord(DecodingStepProto{})) {
      return absl::InvalidArgumentError("failed to write a decoding step");
    }
    return absl::OkStatus();
  }

 private:
  riegeli::RecordWriterBase& record_writer_;
  uint64_t record_count_ = 0;
};

absl::Status ProcessRiegeliContainer(riegeli::RecordReaderBase& record_reader,
                                     ContainerProcessor& container_processor) {
  uint64_t decoding_step_count = 0;
  DecodingStepProto decoding_step_proto;
  for (;; ++decoding_step_count) {
    if (!record_reader.ReadRecord(decoding_step_proto)) {
      if (record_reader.ok()) {
        return absl::InvalidArgumentError(
            "unable to read the next decoding step; riegeli container is not "
            "properly terminated");
      }
      return record_reader.status();
    }
    if (decoding_step_proto.type_case() == DecodingStepProto::TYPE_NOT_SET) {
      return absl::OkStatus();  // a "stop" record
    }
    RETURN_IF_ERROR(container_processor.OnDecodingStep(decoding_step_count,
                                                       decoding_step_proto))
        << "while handling decoding_steps[" << decoding_step_count << "]";
  }
}

}  // namespace

absl::StatusOr<std::string> EncodeAsRiegeliData(
    absl::Span<const TypedValue> values,
    absl::Span<const arolla::expr::ExprNodePtr> exprs,
    absl::string_view riegeli_options) {
  riegeli::RecordWriterBase::Options record_writer_options;
  RETURN_IF_ERROR(record_writer_options.FromString(riegeli_options));
  std::string result;
  riegeli::RecordWriter record_writer((riegeli::StringWriter(&result)),
                                      std::move(record_writer_options));
  RiegeliContainerBuilder container_builder(record_writer);
  {
    Encoder encoder(CodecBasedValueEncoder(), container_builder);
    for (const auto& value : values) {
      RETURN_IF_ERROR(encoder.EncodeValue(value).status());
    }
    for (const auto& expr : exprs) {
      RETURN_IF_ERROR(encoder.EncodeExpr(expr).status());
    }
  }
  RETURN_IF_ERROR(std::move(container_builder).Finish());
  if (!record_writer.Close()) {
    return record_writer.status();
  }
  return result;
}

absl::StatusOr<DecodeResult> DecodeFromRiegeliData(
    absl::string_view riegeli_data, const DecodingOptions& decoding_options) {
  riegeli::RecordReader record_reader((riegeli::StringReader(riegeli_data)));
  Decoder decoder(CodecBasedValueDecoderProvider(), decoding_options);
  RETURN_IF_ERROR(ProcessRiegeliContainer(record_reader, decoder));
  if (!record_reader.Close()) {
    return record_reader.status();
  }
  return std::move(decoder).Finish();
}

}  // namespace arolla::serialization
