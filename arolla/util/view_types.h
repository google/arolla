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
#ifndef AROLLA_UTIL_VIEW_TYPES_H_
#define AROLLA_UTIL_VIEW_TYPES_H_

#include <string>

#include "absl/strings/string_view.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {

// `view_type_t<T>` is a lightweight class that refers to the data of `T`.
// For trivial types `view_type_t<T>` is equal to `T`.
// Several types can have a single view type.
// If `T` has a view type, it should be implicitly convertible to it.

template <typename T>
struct view_type {
  using type = T;
};

template <>
struct view_type<std::string> {
  using type = absl::string_view;
};

template <>
struct view_type<Text> {
  using type = absl::string_view;
};

template <typename T>
using view_type_t = typename view_type<T>::type;

}  // namespace arolla

#endif  // AROLLA_UTIL_VIEW_TYPES_H_
