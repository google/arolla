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
#ifndef AROLLA_ARRAY_QTYPE_COPIER_H_
#define AROLLA_ARRAY_QTYPE_COPIER_H_

// IWYU pragma: private

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <optional>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// Implementation of BatchToFramesCopier for Array.
// Supports mappings
//     Array<T> -> OptionalValue<T>
template <class T>
class ArrayToFramesCopier : public BatchToFramesCopier {
 public:
  absl::Status AddMapping(TypedRef array_ptr, TypedSlot scalar_slot) final {
    if (IsStarted()) {
      return absl::FailedPreconditionError(
          "cannot add new mappings when started");
    }
    ASSIGN_OR_RETURN(const Array<T>& array, array_ptr.As<Array<T>>());
    RETURN_IF_ERROR(SetRowCount(array.size()));
    ASSIGN_OR_RETURN(auto slot, scalar_slot.ToSlot<OptionalValue<T>>());
    mappings_.push_back(Mapping{array, slot});
    return absl::OkStatus();
  }

  void CopyNextBatch(absl::Span<FramePtr> output_buffers) final {
    if (!IsStarted()) Start();  // Forbid adding new mappings.
    for (const Mapping& mapping : mappings_) {
      const DenseArray<T>& data = mapping.array.dense_data();
      auto scalar_slot = mapping.scalar_slot;
      if (mapping.array.IsFullForm()) {
        auto iter = data.values.begin() + current_row_id_;
        for (FramePtr frame : output_buffers) {
          frame.Set(scalar_slot, {true, T(*(iter++))});
        }
      } else if (mapping.array.IsDenseForm()) {
        bitmap::IterateByGroups(
            data.bitmap.begin(), current_row_id_ + data.bitmap_bit_offset,
            output_buffers.size(), [&](int64_t offset) {
              FramePtr* frames_group = output_buffers.begin() + offset;
              auto values_group =
                  data.values.begin() + current_row_id_ + offset;
              return [=](int64_t i, bool present) {
                frames_group[i].Set(scalar_slot, {present, T(values_group[i])});
              };
            });
      } else {
        const OptionalValue<T>& missing_id_v = mapping.array.missing_id_value();
        for (FramePtr& frame : output_buffers) {
          frame.Set(scalar_slot, missing_id_v);
        }
        const IdFilter& id_filter = mapping.array.id_filter();
        const int64_t* ids_iter =
            std::lower_bound(id_filter.ids().begin(), id_filter.ids().end(),
                             current_row_id_ + id_filter.ids_offset());
        int64_t offset_from = std::distance(id_filter.ids().begin(), ids_iter);
        auto iter_to = std::lower_bound(
            id_filter.ids().begin(), id_filter.ids().end(),
            current_row_id_ + output_buffers.size() + id_filter.ids_offset());
        int64_t count = std::distance(ids_iter, iter_to);
        FramePtr* frames =
            output_buffers.begin() - id_filter.ids_offset() - current_row_id_;
        if (data.bitmap.empty()) {
          auto values_iter = data.values.begin() + offset_from;
          for (int64_t i = 0; i < count; ++i) {
            frames[*(ids_iter++)].Set(scalar_slot, {true, T(*(values_iter++))});
          }
        } else {
          bitmap::IterateByGroups(
              data.bitmap.begin(), offset_from + data.bitmap_bit_offset, count,
              [&](int64_t offset) {
                const int64_t* ids_group = ids_iter + offset;
                auto values_group = data.values.begin() + offset_from + offset;
                return [=](int64_t i, bool present) {
                  frames[ids_group[i]].Set(scalar_slot,
                                           {present, T(values_group[i])});
                };
              });
        }
      }
    }
    current_row_id_ += output_buffers.size();
  }

 private:
  struct Mapping {
    const Array<T>& array;
    FrameLayout::Slot<OptionalValue<T>> scalar_slot;
  };
  std::vector<Mapping> mappings_;
  int64_t current_row_id_ = 0;
};

// Implementation of BatchFromFramesCopier for Array.
// Supports mappings
//     T -> Array<T>
//     OptionalValue<T> -> Array<T>
template <class T>
class ArrayFromFramesCopier : public BatchFromFramesCopier {
 public:
  explicit ArrayFromFramesCopier(
      RawBufferFactory* buffer_factory = GetHeapBufferFactory())
      : buffer_factory_(buffer_factory) {}

  absl::Status AddMapping(TypedSlot scalar_slot, TypedSlot array_slot) final {
    if (IsStarted()) {
      return absl::FailedPreconditionError(
          "cannot add new mappings when started");
    }
    ASSIGN_OR_RETURN(auto dst_slot, array_slot.ToSlot<Array<T>>());
    if (IsOptionalQType(scalar_slot.GetType())) {
      ASSIGN_OR_RETURN(auto src_slot, scalar_slot.ToSlot<OptionalValue<T>>());
      mappings_.push_back(Mapping{src_slot, dst_slot});
    } else {
      ASSIGN_OR_RETURN(auto src_slot, scalar_slot.ToSlot<T>());
      mappings_.push_back(Mapping{src_slot, dst_slot});
    }
    return absl::OkStatus();
  }

  absl::Status CopyNextBatch(
      absl::Span<const ConstFramePtr> input_buffers) final {
    if (!IsStarted()) {
      return absl::FailedPreconditionError(
          "Start(row_count) should be called before CopyNextBatch");
    }

    for (Mapping& mapping : mappings_) {
      DCHECK(mapping.values_builder.has_value());
      if (std::holds_alternative<FrameLayout::Slot<T>>(mapping.scalar_slot)) {
        // copy from non-optional scalars into array.
        auto scalar_slot =
            *std::get_if<FrameLayout::Slot<T>>(&mapping.scalar_slot);
        const ConstFramePtr* iter = input_buffers.data();
        mapping.values_builder->SetN(
            current_row_id_, input_buffers.size(),
            [&] { return (iter++)->Get(scalar_slot); });
      } else {
        // copy from optional scalars into array.
        DCHECK(std::holds_alternative<FrameLayout::Slot<OptionalValue<T>>>(
            mapping.scalar_slot));
        DCHECK(mapping.bitmap_builder.has_value());
        auto scalar_slot = *std::get_if<FrameLayout::Slot<OptionalValue<T>>>(
            &mapping.scalar_slot);
        auto values_inserter =
            mapping.values_builder->GetInserter(current_row_id_);
        auto fn = [&](ConstFramePtr frame) {
          const OptionalValue<T>& v = frame.Get(scalar_slot);
          values_inserter.Add(v.value);
          return v.present;
        };
        // Callback `fn` inserts values as a side-effect while setting presence
        // bits.
        mapping.bitmap_builder->AddForEach(input_buffers, fn);
      }
    }
    current_row_id_ += input_buffers.size();
    return absl::OkStatus();
  }

  absl::Status Finalize(FramePtr arrays_frame) final {
    if (finished_) {
      return absl::FailedPreconditionError("finalize can be called only once");
    }
    finished_ = true;
    for (Mapping& mapping : mappings_) {
      DCHECK(mapping.values_builder.has_value());
      DenseArray<T> res;
      res.values = (*std::move(mapping.values_builder)).Build();
      if (mapping.bitmap_builder.has_value()) {
        res.bitmap = (*std::move(mapping.bitmap_builder)).Build();
      }
      arrays_frame.Set(mapping.array_slot, Array<T>(std::move(res)));
    }
    return absl::OkStatus();
  }

 private:
  // Mapping from a `scalar_slot` from which values are being read to a
  // corresponding `array_slot` and it's associated `builder` to which values
  // are written. `scalar_slot` is a variant in order to support either
  // non-optional or optional scalar types.
  struct Mapping {
    std::variant<FrameLayout::Slot<T>, FrameLayout::Slot<OptionalValue<T>>>
        scalar_slot;
    FrameLayout::Slot<Array<T>> array_slot;
    std::optional<typename Buffer<T>::Builder> values_builder;
    std::optional<bitmap::Builder> bitmap_builder;
  };

  void SetArraySize(int64_t size) final {
    // Initialize builders
    for (auto& mapping : mappings_) {
      mapping.values_builder.emplace(size, buffer_factory_);
      if (mapping.scalar_slot.index() == 1) {
        mapping.bitmap_builder.emplace(size, buffer_factory_);
      }
    }
  }

  std::vector<Mapping> mappings_;
  int64_t current_row_id_ = 0;
  bool finished_ = false;
  RawBufferFactory* buffer_factory_;
};

}  // namespace arolla

#endif  // AROLLA_ARRAY_QTYPE_COPIER_H_
