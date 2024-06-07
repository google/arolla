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
#include "arolla/serialization/decode.h"

#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container_proto.h"
#include "arolla/serialization_base/decode.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization {
namespace {

using ::arolla::serialization_base::ContainerProto;
using ::arolla::serialization_base::ValueDecoder;

// The global registry of value decoders.
class ValueDecoderRegistry {
 public:
  static ValueDecoderRegistry& instance() {
    static Indestructible<ValueDecoderRegistry> result;
    return *result;
  }

  absl::Status RegisterValueDecoder(absl::string_view codec_name,
                                    ValueDecoder value_decoder) {
    if (value_decoder == nullptr) {
      return absl::InvalidArgumentError("value_decoder is empty");
    }
    absl::MutexLock lock(&mutex_);
    registry_[codec_name] = std::move(value_decoder);
    return absl::OkStatus();
  }

  absl::StatusOr<ValueDecoder> LookupValueDecoder(
      absl::string_view codec_name) {
    {
      absl::MutexLock lock(&mutex_);
      if (auto it = registry_.find(codec_name); it != registry_.end()) {
        return it->second;
      }
    }
    constexpr absl::string_view suggested_dependency =
        R"(adding "@arolla://arolla/serialization_codecs:all_decoders")"
        R"( build dependency may help)";
    return absl::InvalidArgumentError(absl::StrFormat(
        R"(unknown codec: "%s"; %s)", absl::Utf8SafeCHexEscape(codec_name),
        suggested_dependency));
  }

 private:
  absl::Mutex mutex_;
  absl::flat_hash_map<std::string, ValueDecoder> registry_
      ABSL_GUARDED_BY(mutex_);
};

}  // namespace

absl::Status RegisterValueDecoder(absl::string_view codec_name,
                                  ValueDecoder value_decoder) {
  return ValueDecoderRegistry::instance().RegisterValueDecoder(
      codec_name, std::move(value_decoder));
}

absl::StatusOr<DecodeResult> Decode(const ContainerProto& container_proto,
                                    const DecodingOptions& options) {
  arolla::serialization_base::Decoder decoder(
      [](absl::string_view codec_name) {
        return ValueDecoderRegistry::instance().LookupValueDecoder(codec_name);
      },
      options);
  RETURN_IF_ERROR(arolla::serialization_base::ProcessContainerProto(
      container_proto, decoder));
  return std::move(decoder).Finish();
}

}  // namespace arolla::serialization
