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
#ifndef AROLLA_UTIL_RAW_SPAN_H_
#define AROLLA_UTIL_RAW_SPAN_H_

#include <cstddef>
#include <type_traits>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/log/check.h"
#include "absl/types/span.h"

namespace arolla {

// We really don't need range checks in performance-critical loops, so in a few
// places we have to migrate from absl::Span to our own implementation.
//
// RawSpan is a pointer+size abstraction similar to absl::Span.
// Intended for performance-critical places where we explicitly want to avoid
// range checks enforced by absl::Span.
template <class T>
class ABSL_ATTRIBUTE_VIEW RawSpan {
 public:
  RawSpan() noexcept : data_(nullptr), size_(0) {}
  RawSpan(T* data, size_t size) noexcept : data_(data), size_(size) {}

  RawSpan<T>& operator=(const RawSpan<T>& other) = default;

  template <class V = T>
  RawSpan<T>& operator=(
      const RawSpan<std::enable_if_t<std::is_const_v<V>, std::decay_t<V>>>&
          other) {
    data_ = other.data();
    size_ = other.size();
    return *this;
  }

  RawSpan(absl::Span<T> span) noexcept  // NOLINT(google-explicit-constructor)
      : data_(span.data()), size_(span.size()) {}

  operator absl::Span<T>()  // NOLINT(google-explicit-constructor)
      const noexcept {
    return absl::Span<T>(data_, size_);
  }

  template <class V,
            std::enable_if_t<std::is_const_v<T> &&
                             std::is_same_v<std::decay_t<T>, std::decay_t<V>>>>
  RawSpan(  // NOLINT(google-explicit-constructor)
      const std::vector<V>& v ABSL_ATTRIBUTE_LIFETIME_BOUND) noexcept
      : data_(v.data()), size_(v.size()) {}

  T* begin() const noexcept { return data_; }
  T* data() const noexcept { return data_; }
  T* end() const noexcept { return data_ + size_; }
  size_t size() const noexcept { return size_; }
  bool empty() const noexcept { return size_ == 0; }

  T& operator[](size_t index) const noexcept {
    DCHECK_LT(index, size_);
    return data_[index];
  }

  T& front() const noexcept {
    DCHECK(!empty());
    return *data_;
  }
  T& back() const noexcept {
    DCHECK(!empty());
    return data_[size_ - 1];
  }

  RawSpan<T> subspan(size_t offset) const noexcept {
    DCHECK_LE(offset, size_);
    return RawSpan<T>(data_ + offset, size_ - offset);
  }
  RawSpan<T> subspan(size_t offset, size_t new_size) const {
    DCHECK_LE(offset + new_size, size_);
    return RawSpan<T>(data_ + offset, new_size);
  }

 private:
  T* data_;
  size_t size_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_RAW_SPAN_H_
