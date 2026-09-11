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
#ifndef AROLLA_MEMORY_VOID_BUFFER_H_
#define AROLLA_MEMORY_VOID_BUFFER_H_

// IWYU pragma: private

#include <cstddef>
#include <cstdint>
#include <variant>

#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/iterator.h"
#include "arolla/util/unit.h"

namespace arolla {

// Buffer specialization for std::monostate value type. This can be used
// in generic containers as a low-overhead dummy buffer where no real values
// are needed. Specifically, this is used in the implementation of "mask"
// array types, where only the presence information is used.
class VoidBuffer {
 public:
  using value_type = std::monostate;
  using size_type = size_t;
  using difference_type = ptrdiff_t;
  using const_iterator = ConstArrayIterator<VoidBuffer>;
  using offset_type = size_t;

  explicit VoidBuffer(size_t size = 0) : size_(size) {}
  bool is_owner() const { return true; }
  bool empty() const { return (size_ == 0); }
  size_t size() const { return size_; }
  value_type operator[](size_t offset) const { return {}; }

  const_iterator begin() const { return const_iterator{this, 0}; }
  const_iterator end() const { return const_iterator{this, size()}; }
  value_type front() const { return {}; }
  value_type back() const { return {}; }

  bool operator==(const VoidBuffer& other) const {
    return size_ == other.size_;
  }
  bool operator!=(const VoidBuffer& other) const { return !(*this == other); }

  VoidBuffer ShallowCopy() const { return VoidBuffer(size_); }
  VoidBuffer DeepCopy(RawBufferFactory* = nullptr) const {
    return VoidBuffer(size_);
  }
  VoidBuffer Slice(size_t offset, size_t count) const {
    return VoidBuffer(count);
  }

  // Returns buffer of the given size.
  static VoidBuffer CreateUninitialized(size_t size,
                                        RawBufferFactory* = nullptr) {
    return VoidBuffer(size);
  }

  template <typename Iter>
  static VoidBuffer Create(Iter begin, Iter end, RawBufferFactory* = nullptr) {
    return VoidBuffer(std::distance(begin, end));
  }

  struct Inserter {
    size_t size = 0;
    void Add(value_type) { size++; }
    void SkipN(size_t count) { size += count; }
  };

  class Builder {
   public:
    Builder() = default;
    Builder(Builder&&) = default;
    Builder& operator=(Builder&&) = default;

    explicit Builder(size_t max_size, RawBufferFactory* = nullptr)
        : max_size_(max_size) {}
    void Set(size_t, value_type) {}
    void Copy(size_t, size_t) {}
    template <typename T>
    void SetN(size_t first_offset, size_t count, T) {}
    void SetNConst(size_t first_offset, size_t count, value_type) {}
    VoidBuffer Build(size_t size) && { return VoidBuffer(size); }
    VoidBuffer Build() && { return VoidBuffer(max_size_); }

    Inserter GetInserter(size_t offset = 0) { return Inserter(); }
    VoidBuffer Build(Inserter ins) && { return VoidBuffer(ins.size); }

   private:
    size_t max_size_;
  };

  // Allows to create a buffer by reordering elements of another buffer.
  // Needed for consistency with StringsBuffer.
  class ReshuffleBuilder {
   public:
    explicit ReshuffleBuilder(size_t new_size, VoidBuffer,
                              const OptionalValue<Unit>&,
                              RawBufferFactory* buf_factory = nullptr)
        : size_(new_size) {}
    void CopyValue(size_t, size_t) {}
    void CopyValueToRange(size_t new_index_from, size_t new_index_to,
                          size_t old_index) {}
    VoidBuffer Build() && { return VoidBuffer(size_); }
    VoidBuffer Build(size_t size) && { return VoidBuffer(size); }

   private:
    size_t size_;
  };

  // Return the allocated memory used by structures required by this object.
  // Note that different Buffers can share internal structures. In these cases
  // the sum of the Buffers::memory_usage() can be higher that the actual system
  // memory use.
  size_t memory_usage() const { return 0; }

  template <typename H>
  friend H AbslHashValue(H h, const VoidBuffer& buffer) {
    return H::combine(std::move(h), buffer.size_);
  }

 private:
  size_t size_;
};

}  // namespace arolla

#endif  // AROLLA_MEMORY_VOID_BUFFER_H_
