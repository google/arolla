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
#include "arolla/serialization/riegeli.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
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

namespace arolla::serialization {
namespace {

using ::arolla::serialization_base::ContainerBuilder;
using ::arolla::serialization_base::ContainerProcessor;
using ::arolla::serialization_base::Decoder;
using ::arolla::serialization_base::DecoderHintsProto;
using ::arolla::serialization_base::DecodingStepProto;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_codecs::CodecBasedValueDecoderProvider;
using ::arolla::serialization_codecs::CodecBasedValueEncoder;

// NOTE: We use a "stop" record -- a decoding step with no `oneof type` field
// set -- to mark the end of a container's records. The stop record may also
// carry an optional `decoder_hints` field (outside the oneof) that enables
// memory-efficient decoding.
//
// If `riegeli_use_stop_record_hints` is set, the current implementation
// assumes the stop record is the last record in the record sequence.
// The decoder seeks to the end of the sequence and reads the stop record first
// (via `Seek(Size())` and `SeekBack()`) to extract hints before the forward
// decoding pass.
//
// Historically, the stop record was used to provide composability --
// allowing multiple containers to be embedded sequentially within a single
// record sequence. However, this was never used in practice, and
// `Seek(Size())` is incompatible with it (only the last container's stop
// record is discoverable from the end). If composability is ever needed,
// the design needs a revision for the hints usage.

class RiegeliContainerBuilder final : public ContainerBuilder {
 public:
  explicit RiegeliContainerBuilder(riegeli::RecordWriterBase& record_writer)
      : record_writer_(record_writer) {}

  absl::StatusOr<uint64_t> Add(DecodingStepProto&& decoding_step_proto) final {
    if (record_count_ == 0) {
      // Indicate that the decoder should look for hints in the stop record.
      decoding_step_proto.mutable_decoder_hints()
          ->set_riegeli_use_stop_record_hints(true);
    }
    record_writer_.WriteRecord(decoding_step_proto);
    RETURN_IF_ERROR(record_writer_.status()) << "while writing a decoding step";
    return record_count_++;
  }

  // Add a "stop" record, so the decoder knew where to stop.
  absl::Status Finish(DecoderHintsProto&& hints_proto) && final {
    DecodingStepProto stop_record_proto;
    *stop_record_proto.mutable_decoder_hints() = std::move(hints_proto);
    record_writer_.WriteRecord(stop_record_proto);
    RETURN_IF_ERROR(record_writer_.status())
        << "while writing the finishing decoding step";
    return absl::OkStatus();
  }

 private:
  riegeli::RecordWriterBase& record_writer_;
  uint64_t record_count_ = 0;
};

absl::StatusOr<DecodingStepProto> PeekStopRecord(
    riegeli::RecordReaderBase& record_reader) {
  const auto initial_pos = record_reader.pos();
  const auto size = record_reader.Size();
  if (!size.has_value()) {
    return DecodingStepProto::default_instance();
  }
  if (!record_reader.Seek(*size) || !record_reader.SeekBack()) {
    RETURN_IF_ERROR(record_reader.status());
    return absl::InvalidArgumentError(
        "unable to seek to the end of the riegeli container");
  }
  DecodingStepProto decoding_step_proto;
  if (!record_reader.ReadRecord(decoding_step_proto) ||
      decoding_step_proto.type_case() != DecodingStepProto::TYPE_NOT_SET) {
    RETURN_IF_ERROR(record_reader.status());
    return absl::InvalidArgumentError("unable to find the stop record");
  }
  if (!record_reader.Seek(initial_pos)) {
    RETURN_IF_ERROR(record_reader.status());
    return absl::InvalidArgumentError(
        "unable to rewind to the beginning of the riegeli container");
  }
  return decoding_step_proto;
}

absl::Status ProcessRiegeliContainer(riegeli::RecordReaderBase& record_reader,
                                     ContainerProcessor& container_processor) {
  bool used_stop_record_hints = false;
  uint64_t decoding_step_count = 0;
  DecodingStepProto decoding_step_proto;
  for (;; ++decoding_step_count) {
    if (!record_reader.ReadRecord(decoding_step_proto)) {
      RETURN_IF_ERROR(record_reader.status());
      return absl::InvalidArgumentError(
          "unable to read the next decoding step; riegeli container is not "
          "properly terminated");
    }
    if (!used_stop_record_hints &&
        decoding_step_proto.decoder_hints().riegeli_use_stop_record_hints() &&
        record_reader.SupportsRandomAccess()) {
      used_stop_record_hints = true;
      ASSIGN_OR_RETURN(auto stop_record_proto, PeekStopRecord(record_reader));
      RETURN_IF_ERROR(container_processor.OnDecodingStep(
          /*decoding_step_index=*/0, stop_record_proto))
          << "while handling decoder hints from the stop record";
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
  {
    Encoder encoder(CodecBasedValueEncoder(),
                    std::make_unique<RiegeliContainerBuilder>(record_writer));
    for (const auto& value : values) {
      RETURN_IF_ERROR(encoder.EncodeValue(value).status());
    }
    for (const auto& expr : exprs) {
      RETURN_IF_ERROR(encoder.EncodeExpr(expr).status());
    }
    RETURN_IF_ERROR(std::move(encoder).Finish());
  }
  if (!record_writer.Close()) {
    return record_writer.status();
  }
  return result;
}

absl::StatusOr<DecodeResult> DecodeFromRiegeliData(
    absl::string_view riegeli_data, DecodingOptions decoding_options) {
  if (decoding_options.value_decoder_provider == nullptr) {
    decoding_options.value_decoder_provider = CodecBasedValueDecoderProvider();
  }
  riegeli::RecordReader record_reader((riegeli::StringReader(riegeli_data)));
  Decoder decoder(std::move(decoding_options));
  RETURN_IF_ERROR(ProcessRiegeliContainer(record_reader, decoder));
  if (!record_reader.Close()) {
    return record_reader.status();
  }
  return std::move(decoder).Finish();
}

}  // namespace arolla::serialization
