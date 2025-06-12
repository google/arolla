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
#ifndef AROLLA_UTIL_MEMORY_H_
#define AROLLA_UTIL_MEMORY_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>

#include "absl/log/check.h"

namespace arolla {

struct FreeDeleter {
  void operator()(const void* ptr) const { std::free(const_cast<void*>(ptr)); }
};

using MallocPtr = std::unique_ptr<void, FreeDeleter>;

// Type-safe wrapper for `alignment`, to distinguish it from `size`.
struct Alignment {
  size_t value;
};

// Aligned allocation. Result is never equal to nullptr.
inline MallocPtr AlignedAlloc(Alignment alignment, size_t size) {
  // Alignment must be a power of 2.
  DCHECK(alignment.value > 0 && !(alignment.value & (alignment.value - 1)));
  if (size == 0) {
    size = 1;  // posix_memalign(size=0) can result with nullptr.
  }
  if (alignment.value <= sizeof(void*)) {
    return MallocPtr(std::malloc(size));
  }
  void* result = nullptr;
  if (posix_memalign(&result, alignment.value, size)) {
    result = nullptr;
  }
  DCHECK(result) << "posix_memalign failed.";
  return MallocPtr(result);
}

inline bool IsAlignedPtr(size_t alignment, const void* ptr) {
  DCHECK(alignment > 0 && !(alignment & (alignment - 1)));
  return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1)) == 0;
}

inline bool IsAlignedPtr(Alignment alignment, const void* ptr) {
  return IsAlignedPtr(alignment.value, ptr);
}

}  // namespace arolla

#endif  // AROLLA_UTIL_MEMORY_H_
