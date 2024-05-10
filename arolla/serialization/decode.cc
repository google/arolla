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
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decode.h"
#include "arolla/util/indestructible.h"

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

  ValueDecoder LookupValueDecoder(absl::string_view codec_name) {
    absl::MutexLock lock(&mutex_);
    auto it = registry_.find(codec_name);
    if (it == registry_.end()) {
      return nullptr;
    }
    return it->second;
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
  return arolla::serialization_base::Decode(
      container_proto,
      [](absl::string_view codec_name) {
        return ValueDecoderRegistry::instance().LookupValueDecoder(codec_name);
      },
      options);
}

}  // namespace arolla::serialization
