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
#include "arolla/util/unit.h"

#include "absl/strings/string_view.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

ReprToken ReprTraits<Unit>::operator()(const Unit&) const {
  return ReprToken{"unit"};
}

void FingerprintHasherTraits<Unit>::operator()(FingerprintHasher* hasher,
                                               const Unit& value) const {
  hasher->Combine(absl::string_view("unit"));
}

}  // namespace arolla
