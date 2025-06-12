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
#ifndef AROLLA_SEQUENCE_MUTABLE_SEQUENCE_H_
#define AROLLA_SEQUENCE_MUTABLE_SEQUENCE_H_

#include <cstddef>
#include <memory>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/demangle.h"

namespace arolla {

// A mutable sequence of qtyped values.
//
// This class can be considered as a factory for immutable sequences.
class MutableSequence {
 public:
  // Constrcuts a sequence of the given size.
  static absl::StatusOr<MutableSequence> Make(QTypePtr value_qtype,
                                              size_t size);

  // Constructs an empty sequence of NOTHING.
  MutableSequence() = default;

  // Non-copyable.
  MutableSequence(const MutableSequence&) = delete;
  MutableSequence& operator=(const MutableSequence&) = delete;

  // Movable.
  MutableSequence(MutableSequence&&) = default;
  MutableSequence& operator=(MutableSequence&&) = default;

  // Returns the value qtype.
  QTypePtr value_qtype() const;

  // Returns the number of elements stored in the sequence.
  size_t size() const;

  // Returns the raw data pointer.
  void* RawData();

  // Returns a raw pointer to an element stored in the sequence.
  void* RawAt(size_t i, size_t element_alloc_size);

  // Returns a span to the elements stored in the sequence.
  template <typename T>
  absl::Span<T> UnsafeSpan();

  // Returns a reference to the element stored in the sequence.
  TypedRef GetRef(size_t i);

  // Stores a new element value.
  void UnsafeSetRef(size_t i, TypedRef value);

  // Returns an immutable sequence instance.
  [[nodiscard]] Sequence Finish() &&;

 private:
  QTypePtr value_qtype_ = GetNothingQType();
  size_t size_ = 0;  // number of elements
  std::shared_ptr<void> data_;
};

inline QTypePtr MutableSequence::value_qtype() const { return value_qtype_; }

inline size_t MutableSequence::size() const { return size_; }

inline void* MutableSequence::RawData() { return data_.get(); }

inline void* MutableSequence::RawAt(size_t i, size_t element_alloc_size) {
  DCHECK_LT(i, size_) << "index is out of range: " << i << " >= size=" << size_;
  DCHECK_EQ(element_alloc_size, value_qtype_->type_layout().AllocSize())
      << "element size mismatched: expected "
      << value_qtype_->type_layout().AllocSize() << ", got "
      << element_alloc_size;
  return reinterpret_cast<char*>(RawData()) + i * element_alloc_size;
}

template <typename T>
absl::Span<T> MutableSequence::UnsafeSpan() {
  DCHECK(typeid(T) == value_qtype_->type_info())
      << "element type mismatched: expected "
      << TypeName(value_qtype_->type_info()) << ", got " << TypeName<T>();
  return absl::Span<T>(reinterpret_cast<T*>(data_.get()), size_);
}

inline TypedRef MutableSequence::GetRef(size_t i) {
  DCHECK_LT(i, size_) << "index is out of range: " << i << " >= size=" << size_;
  const char* const data = reinterpret_cast<const char*>(data_.get());
  return TypedRef::UnsafeFromRawPointer(
      value_qtype_, data + i * value_qtype_->type_layout().AllocSize());
}

inline void MutableSequence::UnsafeSetRef(size_t i, TypedRef value) {
  DCHECK_LT(i, size_) << "index is out of range: " << i << " >= size=" << size_;
  DCHECK_EQ(value.GetType(), value_qtype_)
      << "element qtype mismatched: expected " << value_qtype_->name()
      << ", got " << value.GetType()->name();
  char* const data = reinterpret_cast<char*>(data_.get());
  value_qtype_->UnsafeCopy(value.GetRawPointer(),
                           data + i * value_qtype_->type_layout().AllocSize());
}

inline Sequence MutableSequence::Finish() && {
  return Sequence(value_qtype_, size_, std::move(data_));
}

}  // namespace arolla

#endif  // AROLLA_SEQUENCE_MUTABLE_SEQUENCE_H_
