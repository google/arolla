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
#include "arolla/io/proto_types/types.h"

#include <cstddef>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace arolla::proto {

absl::Status UnexpectedArraySizeError(absl::string_view field_name,
                                      size_t proto_size, size_t array_size) {
  return absl::FailedPreconditionError(
      absl::StrCat("unexpected array size for ", field_name, ": proto size ",
                   proto_size, " vs array size ", array_size));
}

}  // namespace arolla::proto
