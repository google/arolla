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
#include "arolla/util/preallocated_buffers.h"
#include <cstring>

#include "arolla/util/memory.h"

namespace arolla {

namespace {
const void* CreateBuffer() {
  auto alloc = AlignedAlloc(Alignment{kZeroInitializedBufferAlignment},
                            kZeroInitializedBufferSize);
  std::memset(alloc.get(), 0, kZeroInitializedBufferSize);
  return alloc.release();
}
}  // namdespace

const void* GetZeroInitializedBuffer() {
  static const void* const kBuffer = CreateBuffer();
  return kBuffer;
}

}  // namespace arolla
