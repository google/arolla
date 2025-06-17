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
#ifndef AROLLA_JAGGED_SHAPE_ARRAY_SERIALIZATION_CODECS_CODEC_NAME_H_
#define AROLLA_JAGGED_SHAPE_ARRAY_SERIALIZATION_CODECS_CODEC_NAME_H_

#include "absl/strings/string_view.h"

namespace arolla::serialization_codecs {

inline constexpr absl::string_view kJaggedArrayShapeV1Codec =
    "arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension";

}  // namespace arolla::serialization_codecs

#endif  // AROLLA_JAGGED_SHAPE_ARRAY_SERIALIZATION_CODECS_CODEC_NAME_H_
