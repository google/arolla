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
#ifndef PY_AROLLA_TYPES_S11N_CODEC_NAME_H_
#define PY_AROLLA_TYPES_S11N_CODEC_NAME_H_

#include "absl/strings/string_view.h"

namespace arolla::python {

constexpr absl::string_view kPyObjectV1Codec =
    "arolla.python.PyObjectV1Proto.extension";

}  // namespace arolla::python

#endif  // PY_AROLLA_TYPES_S11N_CODEC_NAME_H_
