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
#ifndef AROLLA_SEQUENCE_SEQUENCE_H_
#define AROLLA_SEQUENCE_SEQUENCE_H_

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/api.h"
#include "arolla/util/demangle.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

// An immutable sequence of qtyped values.
class AROLLA_API Sequence {
 public:
  // Constructs an empty sequence of NOTHING.
  Sequence() = default;

  // Constructor.
  //
  // NOTE: Please use MutableSequence for initialization.
  Sequence(QTypePtr value_qtype, size_t size,
           std::shared_ptr<const void>&& data)
      : value_qtype_(value_qtype), size_(size), data_(std::move(data)) {
    DCHECK_NE(value_qtype, nullptr);
  }

  // Copyable and movable.
  Sequence(const Sequence&) = default;
  Sequence(Sequence&&) = default;
  Sequence& operator=(const Sequence&) = default;
  Sequence& operator=(Sequence&&) = default;

  // Returns the value qtype.
  QTypePtr value_qtype() const;

  // Returns the number of elements stored in the sequence.
  size_t size() const;

  // Returns the raw data pointer.
  const void* RawData() const;

  // Returns a raw pointer to an element stored in the sequence.
  const void* RawAt(size_t i, size_t element_alloc_size) const;

  // Returns a span to the elements stored in the sequence.
  template <typename T>
  absl::Span<const T> UnsafeSpan() const;

  // Returns a reference to the element stored in the sequence.
  TypedRef GetRef(size_t i) const;

  // Returns a slice of the sequence.
  Sequence subsequence(size_t offset, size_t count) const;

 private:
  QTypePtr value_qtype_ = GetNothingQType();
  size_t size_ = 0;  // number of elements
  std::shared_ptr<const void> data_;
};

inline QTypePtr Sequence::value_qtype() const { return value_qtype_; }

inline size_t Sequence::size() const { return size_; }

inline const void* Sequence::RawData() const { return data_.get(); }

inline const void* Sequence::RawAt(size_t i, size_t element_alloc_size) const {
  DCHECK_LT(i, size_) << "index is out of range: " << i << " >= size=" << size_;
  DCHECK_EQ(element_alloc_size, value_qtype_->type_layout().AllocSize())
      << "element size mismatched: expected "
      << value_qtype_->type_layout().AllocSize() << ", got "
      << element_alloc_size;
  return reinterpret_cast<const char*>(RawData()) + i * element_alloc_size;
}

template <typename T>
absl::Span<const T> Sequence::UnsafeSpan() const {
  DCHECK(typeid(T) == value_qtype_->type_info())
      << "element type mismatched: expected "
      << TypeName(value_qtype_->type_info()) << ", got " << TypeName<T>();
  return absl::Span<const T>(reinterpret_cast<const T*>(data_.get()), size_);
}

inline TypedRef Sequence::GetRef(size_t i) const {
  DCHECK_LT(i, size_) << "index is out of range: " << i << " >= size=" << size_;
  const char* const data = reinterpret_cast<const char*>(data_.get());
  return TypedRef::UnsafeFromRawPointer(
      value_qtype_, data + i * value_qtype_->type_layout().AllocSize());
}

inline Sequence Sequence::subsequence(size_t offset, size_t count) const {
  DCHECK_LE(offset, size_) << "offset is out of range: " << offset
                           << " > size=" << size_;
  count = std::min(count, size_ - offset);
  if (count == 0) {
    return Sequence(value_qtype_, 0, nullptr);
  }
  const char* const data = reinterpret_cast<const char*>(data_.get());
  return Sequence(
      value_qtype_, count,
      std::shared_ptr<const void>(
          data_, data + offset * value_qtype_->type_layout().AllocSize()));
}

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Sequence);
AROLLA_DECLARE_REPR(Sequence);

}  // namespace arolla

#endif  // AROLLA_SEQUENCE_SEQUENCE_H_
