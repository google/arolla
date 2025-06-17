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
#ifndef AROLLA_UTIL_UNIT_H_
#define AROLLA_UTIL_UNIT_H_

#include <variant>

#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// A special type that allows only one value:
//
//   https://en.wikipedia.org/wiki/Unit_type
//
using Unit = std::monostate;

inline constexpr Unit kUnit = Unit();

AROLLA_DECLARE_REPR(Unit);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Unit);

}  // namespace arolla

#endif  // AROLLA_UTIL_UNIT_H_
