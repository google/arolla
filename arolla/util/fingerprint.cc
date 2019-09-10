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
#include "arolla/util/fingerprint.h"

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

#include "absl/hash/hash.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cityhash/city.h"
#include "arolla/util/types.h"

namespace arolla {
namespace {

// A runtime specific seed.
uint32_t RuntimeSeed() {
  static uint32_t result = absl::Hash<int>{}(501816262);
  return result;
}

}  // namespace

std::string Fingerprint::AsString() const {
  return absl::StrFormat("%032x", value);
}

signed_size_t Fingerprint::PythonHash() const {
  return absl::Hash<Fingerprint>()(*this);
}

std::ostream& operator<<(std::ostream& ostream,
                         const Fingerprint& fingerprint) {
  return ostream << absl::StreamFormat("%032x", fingerprint.value);
}

Fingerprint RandomFingerprint() {
  absl::BitGen bitgen;
  return Fingerprint{absl::MakeUint128(absl::Uniform<uint64_t>(bitgen),
                                       absl::Uniform<uint64_t>(bitgen))};
}

FingerprintHasher::FingerprintHasher(absl::string_view salt)
    : state_{3102879407, 2758948377}  // initial_seed
{
  Combine(RuntimeSeed(), salt);
}

Fingerprint FingerprintHasher::Finish() && {
  return Fingerprint{absl::MakeUint128(state_.second, state_.first)};
}

void FingerprintHasher::CombineRawBytes(const void* data, size_t size) {
  state_ = cityhash::CityHash128WithSeed(
      static_cast<const char*>(data), size, state_);
}

}  // namespace arolla
