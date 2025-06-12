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
#ifndef AROLLA_UTIL_TYPES_H_
#define AROLLA_UTIL_TYPES_H_

#include <cstddef>
#include <type_traits>

namespace arolla {

// Signed integer type of the same byte size as the size_t.
//
// NOTE: Please use it instead of ssize_t if you need a signed size type.
// ssize_t guarantees only the value range [-1, SSIZE_MAX].
// (https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/sys_types.h.html)
using signed_size_t = typename std::make_signed<size_t>::type;

}  // namespace arolla

#endif  // AROLLA_UTIL_TYPES_H_
