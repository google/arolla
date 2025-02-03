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
#ifndef AROLLA_QTYPE_ARRAY_LIKE_ARRAY_LIKE_QTYPE_H_
#define AROLLA_QTYPE_ARRAY_LIKE_ARRAY_LIKE_QTYPE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/api.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"

namespace arolla {

// This class iterates over any number of arrays and copies values to
// output_buffers with scalar_slot offset.
class BatchToFramesCopier {
 public:
  virtual ~BatchToFramesCopier() {}

  // Adds a new array and a correspoing scalar slot (should have the same
  // value type). All arrays should have the same row count.
  virtual absl::Status AddMapping(TypedRef array_ptr,
                                  TypedSlot scalar_slot) = 0;

  // Should be called after the last AddMapping and before the first
  // CopyNextBatch.
  void Start() { started_ = true; }
  bool IsStarted() const { return started_; }

  // Can be missed if there is no mappings.
  std::optional<int64_t> row_count() const { return row_count_; }

  // Reads output_buffers.size() values from each array and stores to
  // output_buffers.
  virtual void CopyNextBatch(absl::Span<FramePtr> output_buffers) = 0;

 protected:
  absl::Status SetRowCount(int64_t row_count) {
    if (!row_count_.has_value()) {
      row_count_ = row_count;
    } else if (*row_count_ != row_count) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "array size doesn't match: %d vs %d", *row_count_, row_count));
    }
    return absl::OkStatus();
  }

 private:
  bool started_ = false;
  std::optional<int64_t> row_count_ = std::nullopt;
};

// An interface to create arrays from batches of frames.
class BatchFromFramesCopier {
 public:
  virtual ~BatchFromFramesCopier() {}

  // Adds a new array and a correspoing scalar slot (should have the same
  // value type). All arrays should have the same number of rows.
  virtual absl::Status AddMapping(TypedSlot scalar_slot,
                                  TypedSlot array_slot) = 0;

  // Should be called after the last AddMapping and before the first
  // CopyNextBatch. row_count is the size of the array to create. Required to
  // allocate memory.
  void Start(int64_t row_count) {
    started_ = true;
    SetArraySize(row_count);
  }
  bool IsStarted() const { return started_; }

  // Reads values from input_buffers and stores it to the corresponding
  // arrays. Returns FailedPreconditionError if called before Start.
  virtual absl::Status CopyNextBatch(
      absl::Span<const ConstFramePtr> input_buffers) = 0;

  // Creates output arrays and stores it to the given frame.
  // Can be called only once after the last CopyNextBatch.
  virtual absl::Status Finalize(FramePtr arrays_frame) = 0;

 private:
  virtual void SetArraySize(int64_t row_count) {}
  bool started_ = false;
};

// Base class for all edge qtypes.
class AROLLA_API EdgeQType : public SimpleQType {
 public:
  // Returns type that represents shape of the array of the type corresponding
  // to the detail side of the Edge.
  virtual QTypePtr child_shape_qtype() const = 0;

  // Returns type that represents shape of the array of the type corresponding
  // to the group side of the Edge.
  virtual QTypePtr parent_shape_qtype() const = 0;

 protected:
  using SimpleQType::SimpleQType;
};

bool IsEdgeQType(const QType* /*nullable*/ qtype);

absl::StatusOr<const EdgeQType*> ToEdgeQType(QTypePtr type);

// Dummy type representing an edge that connects two scalars.
struct AROLLA_API ScalarToScalarEdge {};

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(ScalarToScalarEdge);
AROLLA_DECLARE_QTYPE(ScalarToScalarEdge);

// Base class for all array ShapeQTypes.
class AROLLA_API ArrayLikeShapeQType : public ShapeQType {
 protected:
  using ShapeQType::ShapeQType;
};

// Base class for all array QTypes.
class AROLLA_API ArrayLikeQType : public SimpleQType {
 public:
  // Returns type that represents shape of the array.
  virtual const ArrayLikeShapeQType* shape_qtype() const = 0;

  // Returns type that represents Edge for the specific array.
  virtual const EdgeQType* edge_qtype() const = 0;

  // Returns type that represents GroupScalarEdge for the specific array.
  virtual const EdgeQType* group_scalar_edge_qtype() const = 0;

  virtual absl::StatusOr<size_t> ArraySize(TypedRef value) const = 0;

  // Returns an ArrayLikeQType for the same kind of array, but with different
  // value type.
  absl::StatusOr<QTypePtr> WithValueQType(QTypePtr value_qtype) const {
    return shape_qtype()->WithValueQType(value_qtype);
  }

  // Returns type that represents presence in the array.
  QTypePtr presence_qtype() const { return shape_qtype()->presence_qtype(); }

  // Creates BatchToFramesCopier instance for this type of array.
  virtual std::unique_ptr<BatchToFramesCopier> CreateBatchToFramesCopier()
      const = 0;

  // Creates BatchFromFramesCopier instance for this type of array.
  virtual std::unique_ptr<BatchFromFramesCopier> CreateBatchFromFramesCopier(
      RawBufferFactory* buffer_factory) const = 0;

 protected:
  template <typename T>
  ArrayLikeQType(meta::type<T> type, std::string type_name,
                 QTypePtr value_qtype)
      : SimpleQType(type, std::move(type_name), value_qtype) {}
};

bool IsArrayLikeQType(const QType* /*nullable*/ qtype);

bool IsArrayLikeShapeQType(const QType* /*nullable*/ qtype);

absl::StatusOr<const ArrayLikeQType*> ToArrayLikeQType(QTypePtr type);

absl::StatusOr<size_t> GetArraySize(TypedRef array);

absl::StatusOr<std::unique_ptr<BatchToFramesCopier>> CreateBatchToFramesCopier(
    QTypePtr type);

absl::StatusOr<std::unique_ptr<BatchFromFramesCopier>>
CreateBatchFromFramesCopier(
    QTypePtr type, RawBufferFactory* buffer_factory = GetHeapBufferFactory());

// Class template for defining a mapping between value types and corresponding
// array types. Intended for use within implementation of array types rather
// than directly by their clients.
template <typename ArrayLikeType, const char* array_type_name>
class ValueToArrayLikeTypeMapping {
 public:
  absl::StatusOr<const ArrayLikeType*> Get(QTypePtr value) const {
    absl::ReaderMutexLock l(&mu_);
    auto it = values_to_arrays_.find(value);
    if (it == values_to_arrays_.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("%s type with elements of type %s is not registered.",
                          array_type_name, value->name()));
    }
    return it->second;
  }

  void Set(QTypePtr value, const ArrayLikeType* array) {
    absl::WriterMutexLock l(&mu_);
    auto [iter, inserted] = values_to_arrays_.emplace(value, array);
    // Double insertion of the same type means that QType is created twice.
    DCHECK(inserted);
  }

  static ValueToArrayLikeTypeMapping* GetInstance() {
    static absl::NoDestructor<ValueToArrayLikeTypeMapping> mapping;
    return mapping.get();
  }

 private:
  mutable absl::Mutex mu_;
  absl::flat_hash_map<QTypePtr, const ArrayLikeType*> values_to_arrays_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace arolla

#endif  // AROLLA_QTYPE_ARRAY_LIKE_ARRAY_LIKE_QTYPE_H_
