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
#include "arolla/memory/simple_buffer.h"

#include <cstddef>
#include <limits>

#include "absl/log/check.h"

namespace arolla {

void CheckSimpleBufferMaxSizeValid(size_t max_size, size_t sizeof_t) {
  CHECK_LE(max_size, std::numeric_limits<size_t>::max() / sizeof_t)
      << "integer overflow in SimpleBuffer::Builder: max_size is too "
         "large for sizeof(T)="
      << sizeof_t;
}

}  // namespace arolla
