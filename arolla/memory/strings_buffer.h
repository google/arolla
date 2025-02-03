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
#ifndef AROLLA_MEMORY_STRINGS_BUFFER_H_
#define AROLLA_MEMORY_STRINGS_BUFFER_H_

// IWYU pragma: private, include "arolla/memory/buffer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <tuple>
#include <utility>

#include "absl/log/check.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/memory/simple_buffer.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/iterator.h"
#include "arolla/util/preallocated_buffers.h"

namespace arolla {

// Stores string data into a contiguous SimpleBuffer<char>, with a second buffer
// containing the start and end offsets per row. This representation allows rows
// to reference independent ranges of characters, allowing efficient filtering
// and simple dictionary encoding. For example, a constant string array can be
// generated with each row reusing to the same range of characters.
class StringsBuffer {
 public:
  using value_type = absl::string_view;
  using size_type = int64_t;
  using difference_type = int64_t;
  using const_iterator = ConstArrayIterator<StringsBuffer>;
  using offset_type = int64_t;

  struct Offsets {
    // If you modify this struct, update StringsBuffer::AbslHashValue.
    offset_type start;  // inclusive
    offset_type end;    // exclusive
  };

  StringsBuffer() = default;

  // Constructor. `offsets` is a collection of start/end offsets which must
  // be in the range [base_offset, base_offset + characters.size()], each
  // pair defining the range of characters in that row. `characters` holds
  // the raw string data referenced by the offsets and biased by `base_offset`.
  StringsBuffer(SimpleBuffer<Offsets> offsets, SimpleBuffer<char> characters,
                offset_type base_offset = 0);

  class Builder;
  class Inserter {
   public:
    void Add(absl::string_view v) { builder_->Set(offset_++, v); }
    void Add(const absl::Cord& v) { builder_->Set(offset_++, std::string(v)); }
    void SkipN(int64_t count) {
      offset_ += count;
      DCHECK_LE(offset_, builder_->offsets_.size());
    }

   private:
    friend class Builder;
    explicit Inserter(Builder* builder, int64_t offset)
        : builder_(builder), offset_(offset) {}
    Builder* builder_;
    int64_t offset_;
  };

  class Builder {
   public:
    Builder() = default;
    Builder(Builder&&) = default;
    Builder& operator=(Builder&&) = default;

    explicit Builder(int64_t max_size,
                     RawBufferFactory* factory = GetHeapBufferFactory());
    explicit Builder(int64_t max_size, int64_t initial_char_buffer_size,
                     RawBufferFactory* factory = GetHeapBufferFactory());

    Inserter GetInserter(int64_t offset = 0) { return Inserter(this, offset); }

    void Set(int64_t offset, absl::string_view v) {
      DCHECK_GE(offset, 0);
      DCHECK_LT(offset, offsets_.size());
      if (v.size() + num_chars_ > characters_.size()) {
        ResizeCharacters(EstimateRequiredCharactersSize(v.size()));
      }
      DCHECK_LE(v.size() + num_chars_, characters_.size());
      std::copy(v.begin(), v.end(), characters_.data() + num_chars_);
      offsets_[offset].start = num_chars_;
      num_chars_ += v.size();
      offsets_[offset].end = num_chars_;
    }

    void Set(int64_t offset, const absl::Cord& v) {
      Set(offset, std::string(v));
    }

    void Copy(int64_t offset_from, int64_t offset_to) {
      offsets_[offset_to] = offsets_[offset_from];
    }

    template <typename NextValueFn>
    void SetN(int64_t first_offset, int64_t count, NextValueFn fn) {
      for (int64_t i = first_offset; i < first_offset + count; ++i) {
        Set(i, fn());
      }
    }
    void SetNConst(int64_t first_offset, int64_t count, absl::string_view v) {
      DCHECK_GE(count, 0);
      DCHECK_GE(first_offset, 0);
      DCHECK_LE(first_offset + count, offsets_.size());
      if (count <= 0) return;
      Set(first_offset, v);
      auto d = offsets_[first_offset];
      std::fill(offsets_.data() + first_offset + 1,
                offsets_.data() + first_offset + count, d);
    }

    StringsBuffer Build(Inserter ins) && {
      if (!ins.builder_) return StringsBuffer();
      DCHECK_EQ(ins.builder_, this);
      return std::move(*this).Build(ins.offset_);
    }
    StringsBuffer Build(int64_t size) &&;
    StringsBuffer Build() && { return std::move(*this).Build(offsets_.size()); }

   private:
    friend class Inserter;
    size_t EstimateRequiredCharactersSize(size_t size_to_add);
    void ResizeCharacters(size_t new_size);
    void InitDataPointers(std::tuple<RawBufferPtr, void*>&& buf,
                          int64_t offsets_count, int64_t characters_size);

    RawBufferFactory* factory_;
    RawBufferPtr buf_;
    absl::Span<Offsets> offsets_;
    absl::Span<char> characters_;
    offset_type num_chars_ = 0;
  };

  // Allows to create a buffer by reordering elements of another buffer.
  // Reuses the old characters buffer if `default_value` is empty or missed.
  class ReshuffleBuilder {
   public:
    explicit ReshuffleBuilder(
        int64_t max_size, const StringsBuffer& buffer,
        const OptionalValue<absl::string_view>& default_value,
        RawBufferFactory* buf_factory = GetHeapBufferFactory());

    void CopyValue(int64_t new_index, int64_t old_index) {
      offsets_bldr_.Set(new_index, old_offsets_[old_index]);
    }

    void CopyValueToRange(int64_t new_index_from, int64_t new_index_to,
                          int64_t old_index) {
      auto* new_offsets = offsets_bldr_.GetMutableSpan().begin();
      std::fill(new_offsets + new_index_from, new_offsets + new_index_to,
                old_offsets_[old_index]);
    }

    StringsBuffer Build(int64_t size) && {
      return StringsBuffer(std::move(offsets_bldr_).Build(size),
                           std::move(characters_), base_offset_);
    }

    // Build with size=max_size.
    // A bit faster since compiler has more freedom for optimization.
    StringsBuffer Build() && {
      return StringsBuffer(std::move(offsets_bldr_).Build(),
                           std::move(characters_), base_offset_);
    }

   private:
    SimpleBuffer<Offsets>::Builder offsets_bldr_;
    SimpleBuffer<Offsets> old_offsets_;
    SimpleBuffer<char> characters_;
    offset_type base_offset_;
  };

  // Returns buffer of the given size with uninitialized values (empty strings).
  static StringsBuffer CreateUninitialized(
      size_t size, RawBufferFactory* factory = GetHeapBufferFactory()) {
    if (size <= kZeroInitializedBufferSize / sizeof(Offsets)) {
      return StringsBuffer(
          SimpleBuffer<Offsets>(nullptr, absl::Span<const Offsets>(
                                             static_cast<const Offsets*>(
                                                 GetZeroInitializedBuffer()),
                                             size)),
          SimpleBuffer<char>{nullptr, absl::Span<const char>()});
    }
    SimpleBuffer<Offsets>::Builder builder(size, factory);
    std::memset(builder.GetMutableSpan().data(), 0, size * sizeof(Offsets));
    return StringsBuffer(std::move(builder).Build(),
                         SimpleBuffer<char>{nullptr, absl::Span<const char>()});
  }

  // Creates a buffer from a range of input iterators.
  template <class InputIt>
  static StringsBuffer Create(
      InputIt begin, InputIt end,
      RawBufferFactory* factory = GetHeapBufferFactory()) {
    auto size = std::distance(begin, end);
    if (size > 0) {
      Builder builder(size, factory);
      for (int64_t offset = 0; offset < size; begin++, offset++) {
        builder.Set(offset, *begin);
      }
      return std::move(builder).Build(size);
    } else {
      return StringsBuffer();
    }
  }

  // Returns true if block contains zero values.
  bool empty() const { return offsets_.empty(); }

  // Returns true if this block controls the lifetime of all of its data.
  bool is_owner() const {
    return offsets_.is_owner() && characters_.is_owner();
  }

  // Returns the number of strings in this block.
  size_type size() const { return offsets_.size(); }

  // Return the allocated memory used by structures required by this object.
  // Note that different Buffers can share internal structures. In these cases
  // the sum of the Buffers::memory_usage() can be higher that the actual system
  // memory use.
  size_t memory_usage() const {
    return offsets().memory_usage() + characters().memory_usage();
  }

  // Returns the buffer value at the given offset. `i` must be in the
  // range [0, size()-1].
  absl::string_view operator[](size_type i) const {
    DCHECK_LE(0, i);
    DCHECK_LT(i, size());
    auto start = offsets_[i].start;
    auto end = offsets_[i].end;
    return absl::string_view(characters_.begin() + start - base_offset_,
                             end - start);
  }

  bool operator==(const StringsBuffer& other) const;
  bool operator!=(const StringsBuffer& other) const {
    return !(*this == other);
  }

  StringsBuffer ShallowCopy() const;
  StringsBuffer DeepCopy(RawBufferFactory* = GetHeapBufferFactory()) const;

  const_iterator begin() const { return const_iterator{this, 0}; }
  const_iterator end() const { return const_iterator{this, size()}; }
  absl::string_view front() const { return (*this)[0]; }
  absl::string_view back() const { return (*this)[size() - 1]; }

  StringsBuffer Slice(size_type offset) const& {
    return Slice(offset, size() - offset);
  }
  StringsBuffer Slice(size_type offset, size_type count) const&;

  StringsBuffer Slice(size_type offset) && {
    return std::move(*this).Slice(offset, size() - offset);
  }

  StringsBuffer Slice(size_type offset, size_type count) &&;

  const SimpleBuffer<Offsets>& offsets() const { return offsets_; }
  const SimpleBuffer<char>& characters() const { return characters_; }
  offset_type base_offset() const { return base_offset_; }

  template <typename H>
  friend H AbslHashValue(H h, const StringsBuffer& buffer) {
    // Here we hash the complete underlying character buffer even if this
    // StringsBuffer may skip over large sections of it.
    // This is fine for now, but may be worth reconsidering in the future.
    h = H::combine(std::move(h), buffer.size());
    if (!buffer.empty()) {
      auto* start = reinterpret_cast<const char*>(&*buffer.offsets().begin());
      const size_t size = buffer.size() * sizeof(Offsets);
      h = H::combine_contiguous(std::move(h), start, size);
      h = H::combine(std::move(h), buffer.characters());
    }
    return h;
  }

 private:
  // Pairs of (start, end) offsets. The range of offsets for element i is
  // [offsets_[i].start, offsets_[i].end - 1]. All offsets must be in
  // the range [base_offset_, base_offset_ + characters_.size()].
  SimpleBuffer<Offsets> offsets_;

  // Contiguous character data representing the strings in this block, starting
  // from base_offset_.
  SimpleBuffer<char> characters_;

  // The starting offset of the characters buffer.
  offset_type base_offset_ = 0;
};

template <>
struct ArenaTraits<StringsBuffer> {
  static StringsBuffer MakeOwned(StringsBuffer&& v,
                                 RawBufferFactory* buf_factory) {
    return v.DeepCopy(buf_factory);
  }
};

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(StringsBuffer);

}  // namespace arolla

#endif  // AROLLA_MEMORY_STRINGS_BUFFER_H_
