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
#ifndef AROLLA_UTIL_MEMORY_ALLOCATION_H_
#define AROLLA_UTIL_MEMORY_ALLOCATION_H_

#include <utility>

#include "absl/log/check.h"
#include "arolla/memory/frame.h"
#include "arolla/util/memory.h"

namespace arolla {

// MemoryAllocation allocates and owns an aligned piece of memory, required to
// store contents of the provided FrameLayout. It takes care of initializing all
// of FrameLayout slots during its construction, and destroying the slots during
// its destruction.
class MemoryAllocation {
 public:
  // Constructs an empty (IsValid() == false) memory allocation.
  MemoryAllocation() = default;

  // Allocates and initializes memory based on the provided layout.
  explicit MemoryAllocation(const FrameLayout* layout)
      : layout_(layout),
        alloc_(AlignedAlloc(layout->AllocAlignment(), layout->AllocSize())) {
    layout_->InitializeAlignedAlloc(alloc_.get());
  }

  MemoryAllocation(const MemoryAllocation&) = delete;
  MemoryAllocation& operator=(const MemoryAllocation&) = delete;

  MemoryAllocation(MemoryAllocation&&) = default;

  MemoryAllocation& operator=(MemoryAllocation&& other) {
    if (alloc_ != nullptr) {
      layout_->DestroyAlloc(alloc_.get());
    }
    layout_ = other.layout_;
    alloc_ = std::move(other.alloc_);
    return *this;
  }

  // Destroys and deallocates memory.
  ~MemoryAllocation() {
    if (alloc_ != nullptr) {
      layout_->DestroyAlloc(alloc_.get());
    }
  }

  bool IsValid() const { return alloc_ != nullptr; }

  // Get FramePtr to allocation. Contains same data as this object, but doesn't
  // own the data.
  FramePtr frame() {
    DCHECK(IsValid());
    return FramePtr(alloc_.get(), layout_);
  }
  ConstFramePtr frame() const {
    DCHECK(IsValid());
    return ConstFramePtr(alloc_.get(), layout_);
  }

 private:
  const FrameLayout* layout_ = nullptr;
  MallocPtr alloc_ = nullptr;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_MEMORY_ALLOCATION_H_
