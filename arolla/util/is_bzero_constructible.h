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
#ifndef AROLLA_UTIL_IS_BZERO_CONSTRUCTIBLE_H_
#define AROLLA_UTIL_IS_BZERO_CONSTRUCTIBLE_H_

#include <type_traits>

namespace arolla {

// A trait class that identifies whether a valid initialization of type T could
// be done through nullifying its memory structure.
// All trivially default-constructible types are automatically
// bzero-constructible.
template <typename T>
struct is_bzero_constructible : std::is_trivially_default_constructible<T> {};

}  // namespace arolla

#endif  // AROLLA_UTIL_IS_BZERO_CONSTRUCTIBLE_H_
