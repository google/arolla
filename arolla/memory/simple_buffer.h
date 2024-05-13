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
#ifndef AROLLA_MEMORY_SIMPLE_BUFFER_H_
#define AROLLA_MEMORY_SIMPLE_BUFFER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/preallocated_buffers.h"

namespace arolla {

template <typename T>
class SimpleBuffer final {
 private:
  static_assert(!std::is_same_v<T, std::monostate>,
                "For std::monostate use VoidBuffer");

  // Use RawBufferFactory for allocations only if values are trivially
  // destructible.
  static constexpr bool kUseRawBuffer = std::is_trivially_destructible_v<T>;

 public:
  using value_type = T;
  using const_iterator = typename absl::Span<T>::const_iterator;

  class Builder;

  // This Inserter is just a pointer to a current item. In other cases (see
  // StringsBuffer::Inserter) implementation can be different, but interface is
  // the same.
  class Inserter {
   public:
    // Set value in the current position and move pointer to then next item.
    void Add(T v) {
      DCheckEnoughSpace(1);
      *(cur_++) = v;
    }

    // Move pointer.
    void SkipN(int64_t count) {
      DCheckEnoughSpace(count);
      cur_ += count;
    }

   private:
    friend class Builder;
    T* cur_;
#ifdef NDEBUG
    Inserter(T* begin, T* end) : cur_(begin) {}
    void DCheckEnoughSpace(int64_t count) const {}
#else
    Inserter(T* begin, T* end) : cur_(begin), end_(end) {}
    void DCheckEnoughSpace(int64_t count) const {
      DCHECK_LE(cur_ + count, end_);
    }
    const T* end_;
#endif
  };

  class Builder {
   public:
    Builder() = default;
    Builder(Builder&&) = default;
    Builder& operator=(Builder&&) = default;

    // max_size - maximal number of elements in the buffer.
    explicit Builder(int64_t max_size,
                     RawBufferFactory* factory = GetHeapBufferFactory())
        : factory_(factory) {
      if constexpr (kUseRawBuffer) {
        auto [buf, data] = factory->CreateRawBuffer(max_size * sizeof(T));
        // We don't preinitialize primitive arrays for performance reasons.
        // Present values are initialized via Set/Copy/SetN/etc functions.
        // Missing values remain uninitialized. We may apply arithmetic
        // operations on them (it is faster then an extra branch to filter
        // them out), and ignore the result of the operations.
        // But for some types (e.g. bool) not all values are valid. In such
        // cases we have to initialize the memory to avoid undefined behavior.
        if constexpr (std::is_enum_v<T> || std::is_same_v<T, bool>) {
          std::memset(data, 0, max_size * sizeof(T));
        }
        buf_ = std::move(buf);
        data_ = absl::Span<T>(reinterpret_cast<T*>(data), max_size);
      } else {
        // We don't use RawBufferFactory for types with non-trivial destructors
        // because UnsafeArenaBufferFactory doesn't track lifetimes
        // and may even be removed before this shared_ptr.
        buf_ = std::make_shared<std::vector<T>>(max_size);
        data_ = absl::Span<T>(buf_->data(), buf_->size());
      }
    }

    // Available only for SimpleBuffer<T>::Builder.
    // Not supported by StringsBuffer::Builder.
    absl::Span<T> GetMutableSpan() { return data_; }

    Inserter GetInserter(int64_t offset = 0) {
      return Inserter(data_.begin() + offset, data_.end());
    }

    void Set(int64_t offset, T value) { data_[offset] = value; }
    void Copy(int64_t offset_from, int64_t offset_to) {
      data_[offset_to] = data_[offset_from];
    }

    template <typename NextValueFn>
    void SetN(int64_t first_offset, int64_t count, NextValueFn fn) {
      DCHECK_GE(count, 0);
      DCHECK_GE(first_offset, 0);
      DCHECK_LE(first_offset + count, data_.size());
      T* start = data_.data() + first_offset;
      std::generate(start, start + count, fn);
    }
    void SetNConst(int64_t first_offset, int64_t count, T v) {
      DCHECK_GE(count, 0);
      DCHECK_GE(first_offset, 0);
      DCHECK_LE(first_offset + count, data_.size());
      T* start = data_.data() + first_offset;
      std::fill(start, start + count, v);
    }

    // Inserter is used to determine size. If several inserters were created,
    // pass here the one with the largest added ID.
    SimpleBuffer Build(Inserter inserter) && {
      if (!inserter.cur_) return SimpleBuffer();
      return std::move(*this).Build(inserter.cur_ - data_.begin());
    }

    SimpleBuffer Build(int64_t size) && {
      DCHECK_LE(size, data_.size());
      if (ABSL_PREDICT_FALSE(size == 0)) return SimpleBuffer();
      // Resizing is expensive, so we skip it if difference is <1KB.
      if (size + 1024 / sizeof(T) < data_.size()) {
        if constexpr (kUseRawBuffer) {
          auto [buf, data] = factory_->ReallocRawBuffer(
              std::move(buf_), data_.begin(), data_.size() * sizeof(T),
              size * sizeof(T));
          return SimpleBuffer(std::move(buf),
                              absl::Span<T>(reinterpret_cast<T*>(data), size));
        } else {
          buf_->resize(size);
          return SimpleBuffer::Create(std::move(*buf_));
        }
      }
      return SimpleBuffer(std::move(buf_), data_.first(size));
    }

    // Build with size=max_size.
    // A bit faster since compiler has more freedom for optimization.
    SimpleBuffer Build() && { return SimpleBuffer(std::move(buf_), data_); }

   private:
    RawBufferFactory* factory_;
    std::conditional_t<kUseRawBuffer, RawBufferPtr,
                       std::shared_ptr<std::vector<T>>>
        buf_;
    absl::Span<T> data_;
  };

  // Allows to create a buffer by reordering elements of another buffer.
  // For SimpleBuffer it doesn't have a performance benefit. Needed for
  // consistency with StringsBuffer.
  class ReshuffleBuilder {
   public:
    explicit ReshuffleBuilder(
        int64_t max_size, SimpleBuffer buffer,
        const OptionalValue<T>& default_value,
        RawBufferFactory* buf_factory = GetHeapBufferFactory())
        : builder_(max_size, buf_factory), buffer_(std::move(buffer)) {
      if (default_value.present) {
        auto span = builder_.GetMutableSpan();
        std::fill(span.begin(), span.end(), default_value.value);
      }
    }

    void CopyValue(int64_t new_index, int64_t old_index) {
      builder_.Set(new_index, buffer_[old_index]);
    }

    // Fill the range [new_index_from, new_index_to) in the new buffer with
    // value on position `old_index` in the old buffer.
    void CopyValueToRange(int64_t new_index_from, int64_t new_index_to,
                          int64_t old_index) {
      auto data = builder_.GetMutableSpan().begin();
      std::fill(data + new_index_from, data + new_index_to, buffer_[old_index]);
    }

    SimpleBuffer Build(int64_t size) && {
      return std::move(builder_).Build(size);
    }

    // Build with size=max_size.
    // A bit faster since compiler has more freedom for optimization.
    SimpleBuffer Build() && { return std::move(builder_).Build(); }

   private:
    typename SimpleBuffer<T>::Builder builder_;
    SimpleBuffer buffer_;
  };

  // Returns buffer of the given size with uninitialized values.
  // Default constructor is called for non trivially destructible types.
  static SimpleBuffer CreateUninitialized(
      size_t size, RawBufferFactory* factory = GetHeapBufferFactory()) {
    if constexpr (kUseRawBuffer) {
      if (size <= kZeroInitializedBufferSize / sizeof(T)) {
        return SimpleBuffer(
            nullptr,
            absl::Span<const T>(
                static_cast<const T*>(GetZeroInitializedBuffer()), size));
      }
    }
    Builder builder(size, factory);
    return std::move(builder).Build();
  }

  // Creates a buffer from a range of input iterators.
  template <class InputIt>
  static SimpleBuffer Create(
      InputIt begin, InputIt end,
      RawBufferFactory* factory = GetHeapBufferFactory()) {
    auto size = std::distance(begin, end);
    if (size > 0) {
      Builder builder(size, factory);
      std::copy(begin, end, builder.GetMutableSpan().data());
      return std::move(builder).Build();
    } else {
      return SimpleBuffer();
    }
  }

  // Creates a buffer from initializer_list by copying all data.
  static SimpleBuffer Create(
      std::initializer_list<T> values,
      RawBufferFactory* factory = GetHeapBufferFactory()) {
    return Create(values.begin(), values.end(), factory);
  }

  // Creates a buffer from vector without copying data. Doesn't work for bool
  // and string types.
  static SimpleBuffer Create(std::vector<T>&& v) {
    absl::Span<const T> values(v);
    RawBufferPtr holder = std::make_shared<std::vector<T>>(std::move(v));
    return SimpleBuffer(std::move(holder), values);
  }

  // Creates an empty buffer.
  SimpleBuffer() = default;

  // Creates a buffer over the given span, whose memory is managed by
  // raw_buffer. raw_buffer may be nullptr, in which case the buffer is
  // unowned.
  SimpleBuffer(RawBufferPtr raw_buffer, absl::Span<const T> span)
      : raw_buffer_(std::move(raw_buffer)), span_(span) {}

  // Returns a Buffer which has a subset of the current buffer. A non-empty
  // slice has the same ownership of the underlying data as the original buffer.
  SimpleBuffer Slice(size_t offset, size_t count) const& {
    if (count) {
      return SimpleBuffer(raw_buffer_, span_.subspan(offset, count));
    } else {
      // Buffer pointer not needed if size is zero.
      return SimpleBuffer();
    }
  }

  // Move and slice. Can be used for greater efficiency when the original
  // buffer is no longer needed.
  SimpleBuffer Slice(size_t offset, size_t count) && {
    if (count) {
      return SimpleBuffer(std::move(raw_buffer_), span_.subspan(offset, count));
    } else {
      // Allow raw buffer to be released if count is zero.
      return SimpleBuffer();
    }
  }

  // Slice from `offset` to end of buffer.
  SimpleBuffer Slice(size_t offset) const& {
    return Slice(offset, size() - offset);
  }

  SimpleBuffer Slice(size_t offset) && {
    return (*this).Slice(offset, size() - offset);
  }

  // Returns an unowned copy of this buffer.
  SimpleBuffer ShallowCopy() const { return SimpleBuffer{nullptr, span_}; }

  // Returns Buffer which owns a copy of the data referenced by the current
  // buffer. This works regardless of whether the current buffer is owned or
  // unowned.
  SimpleBuffer DeepCopy(
      RawBufferFactory* factory = GetHeapBufferFactory()) const {
    if (is_owner()) {
      return *this;
    } else {
      return Create(span().begin(), span().end(), factory);
    }
  }

  // Returns true if this Buffer object owns the underlying data. Note that
  // an empty buffer is considered 'owned' to avoid unnecessary (and futile)
  // attempts to DeepCopy empty buffers.
  bool is_owner() const { return empty() || (raw_buffer_ != nullptr); }

  // Returns true if the buffer length is 0.
  bool empty() const { return span_.empty(); }

  // Returns the number of values in the buffer.
  int64_t size() const { return span_.size(); }

  // Return the allocated memory used by structures required by this object.
  // Note that different Buffers can share internal structures. In these cases
  // the sum of the Buffers::memory_usage() can be higher that the actual system
  // memory use.
  size_t memory_usage() const { return size() * sizeof(T); }

  // Returns the value at the given position.
  T operator[](int64_t i) const { return span_.data()[i]; }

  // Const iterator methods.
  const_iterator begin() const { return span_.begin(); }
  const_iterator end() const { return span_.end(); }
  T front() const { return span_.front(); }
  T back() const { return span_.back(); }

  // Returns true if the buffer values are equal.
  bool operator==(const SimpleBuffer<T>& other) const {
    if ((span_.begin() == other.span_.begin()) &&
        (span_.end() == other.span_.end())) {
      // Buffers contain same span.
      return true;
    } else {
      // Test for equality of elements.
      return span_ == other.span_;
    }
  }

  bool operator!=(const SimpleBuffer<T>& other) const {
    return !(*this == other);
  }

  // Returns data as a constant span.
  absl::Span<const T> span() const { return span_; }

  template <typename H>
  friend H AbslHashValue(H h, const SimpleBuffer& buffer) {
    return H::combine(std::move(h), buffer.span_);
  }

 private:
  // Pointer to the RawBuffer which contains the data.
  RawBufferPtr raw_buffer_;

  // Span of data represented by this buffer. The span is guaranteed to lie
  // completely within the raw_buffer's allocated space.
  absl::Span<const T> span_;
};

// Support comparison between Buffer<T> and any collection which is convertible
// to Span<const T> using element-wise comparison. This is mostly used in tests.
template <typename T, typename U>
std::enable_if_t<std::is_convertible_v<U, absl::Span<const T>>, bool>
operator==(const SimpleBuffer<T>& lhs, const U& rhs) {
  absl::Span<const T> rhs_span(rhs);
  if (rhs_span.size() != lhs.size()) {
    return false;
  }
  for (int64_t i = 0; i < rhs_span.size(); ++i) {
    if (rhs_span[i] != lhs[i]) {
      return false;
    }
  }
  return true;
}

// Support equality comparison where Buffer<T> is right-hand argument.
template <typename T, typename U>
std::enable_if_t<std::is_convertible_v<U, absl::Span<const T>>, bool>
operator==(const U& lhs, const SimpleBuffer<T>& rhs) {
  return rhs == lhs;
}

template <typename T>
struct ArenaTraits<SimpleBuffer<T>> {
  static SimpleBuffer<T> MakeOwned(SimpleBuffer<T>&& v,
                                   RawBufferFactory* buf_factory) {
    return v.DeepCopy(buf_factory);
  }
};

template <typename T>
struct FingerprintHasherTraits<SimpleBuffer<T>> {
  // Make sure that string types and unit don't use this specialisations.
  static_assert(std::is_trivially_destructible<T>::value &&
                !std::is_same_v<T, std::monostate>);

  void operator()(FingerprintHasher* hasher, const SimpleBuffer<T>& value) {
    hasher->CombineSpan(value.span());
  }
};

}  // namespace arolla

#endif  // AROLLA_MEMORY_SIMPLE_BUFFER_H_
