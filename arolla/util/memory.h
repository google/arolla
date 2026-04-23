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
#include <memory>
#include <new>

#include "absl/base/nullability.h"
#include "absl/log/check.h"

namespace arolla {

struct AlignedDeleter {
  std::align_val_t alignment;
  void operator()(const void* ptr) const noexcept {
    ::operator delete(const_cast<void*>(ptr), alignment);
  }
};

// Unique pointer for aligned memory blocks.
using AlignedPtr = std::unique_ptr<void, AlignedDeleter>;

// Aligned allocation. Returns nullptr if allocation fails.
inline AlignedPtr absl_nullable AlignedAlloc(std::align_val_t alignment,
                                             size_t size) {
  DCHECK(!(static_cast<size_t>(alignment) &
           (static_cast<size_t>(alignment) - 1)) &&
         !(size & (static_cast<size_t>(alignment) - 1)));
  return AlignedPtr(::operator new(size, alignment, std::nothrow),
                    AlignedDeleter{alignment});
}

inline bool IsAlignedPtr(size_t alignment, const void* absl_nullable ptr) {
  DCHECK(alignment > 0 && !(alignment & (alignment - 1)));
  return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1)) == 0;
}

}  // namespace arolla

#endif  // AROLLA_UTIL_MEMORY_H_
