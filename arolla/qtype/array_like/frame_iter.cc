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
#include "arolla/qtype/array_like/frame_iter.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace {

absl::StatusOr<std::vector<std::unique_ptr<BatchToFramesCopier>>>
CreateInputCopiers(absl::Span<const TypedRef> input_arrays,
                   absl::Span<const TypedSlot> input_scalar_slots) {
  if (input_arrays.size() != input_scalar_slots.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("size of input_arrays and input_scalar_slots should be "
                        "the same: %d vs %d",
                        input_arrays.size(), input_scalar_slots.size()));
  }

  absl::flat_hash_map<QTypePtr, std::unique_ptr<BatchToFramesCopier>>
      input_copiers;
  for (size_t i = 0; i < input_arrays.size(); ++i) {
    QTypePtr array_type = input_arrays[i].GetType();
    if (!input_copiers.contains(array_type)) {
      ASSIGN_OR_RETURN(input_copiers[array_type],
                       CreateBatchToFramesCopier(array_type));
    }
    RETURN_IF_ERROR(input_copiers[array_type]->AddMapping(
        input_arrays[i], input_scalar_slots[i]));
  }

  std::vector<std::unique_ptr<BatchToFramesCopier>> input_copiers_vector;
  for (auto& [_, v] : input_copiers)
    input_copiers_vector.push_back(std::move(v));

  return input_copiers_vector;
}

absl::StatusOr<std::vector<std::unique_ptr<BatchFromFramesCopier>>>
CreateOutputCopiers(absl::Span<const TypedSlot> output_array_slots,
                    absl::Span<const TypedSlot> output_scalar_slots,
                    RawBufferFactory* buffer_factory) {
  if (output_array_slots.size() != output_scalar_slots.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "size of output_array_slots and output_scalar_slots should be "
        "the same: %d vs %d",
        output_array_slots.size(), output_scalar_slots.size()));
  }

  absl::flat_hash_map<QTypePtr, std::unique_ptr<BatchFromFramesCopier>>
      output_copiers;
  for (size_t i = 0; i < output_array_slots.size(); ++i) {
    QTypePtr array_type = output_array_slots[i].GetType();
    if (!output_copiers.contains(array_type)) {
      ASSIGN_OR_RETURN(output_copiers[array_type],
                       CreateBatchFromFramesCopier(array_type, buffer_factory));
    }
    RETURN_IF_ERROR(output_copiers[array_type]->AddMapping(
        output_scalar_slots[i], output_array_slots[i]));
  }

  std::vector<std::unique_ptr<BatchFromFramesCopier>> output_copiers_vector;
  for (auto& [_, v] : output_copiers)
    output_copiers_vector.push_back(std::move(v));

  return output_copiers_vector;
}

}  // namespace

absl::StatusOr<FrameIterator> FrameIterator::Create(
    absl::Span<const TypedRef> input_arrays,
    absl::Span<const TypedSlot> input_scalar_slots,
    absl::Span<const TypedSlot> output_array_slots,
    absl::Span<const TypedSlot> output_scalar_slots,
    const FrameLayout* scalar_layout, FrameIterator::Options options) {
  ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<BatchToFramesCopier>> input_copiers,
      CreateInputCopiers(input_arrays, input_scalar_slots));
  RawBufferFactory* buf_factory = options.buffer_factory;
  if (!buf_factory) buf_factory = GetHeapBufferFactory();
  ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<BatchFromFramesCopier>> output_copiers,
      CreateOutputCopiers(output_array_slots, output_scalar_slots,
                          buf_factory));

  std::optional<int64_t> row_count = std::nullopt;
  for (const auto& copier : input_copiers) {
    if (!copier->row_count() ||
        (row_count && *row_count != *copier->row_count())) {
      return absl::InvalidArgumentError(
          absl::StrFormat("input arrays have different sizes: %d vs %d",
                          *row_count, *copier->row_count()));
    }
    row_count = copier->row_count();
  }
  if (!row_count.has_value()) {
    if (!options.row_count.has_value()) {
      return absl::InvalidArgumentError(
          "options.row_count can not be missed if there is no input arrays");
    }
    row_count = options.row_count;
  } else if (options.row_count.has_value() &&
             *options.row_count != *row_count) {
    return absl::InvalidArgumentError(
        absl::StrFormat("sizes of input arrays don't correspond "
                        "to options.row_count: %d vs %d",
                        *row_count, *options.row_count));
  }
  return FrameIterator(std::move(input_copiers), std::move(output_copiers),
                       *row_count, options.frame_buffer_count, scalar_layout);
}

FrameIterator::FrameIterator(
    std::vector<std::unique_ptr<BatchToFramesCopier>>&& input_copiers,
    std::vector<std::unique_ptr<BatchFromFramesCopier>>&& output_copiers,
    size_t row_count, size_t frame_buffer_count,
    const FrameLayout* scalar_layout)
    : row_count_(row_count),
      input_copiers_(std::move(input_copiers)),
      output_copiers_(std::move(output_copiers)),
      scalar_layout_(scalar_layout) {
  frame_buffer_count = std::min(row_count, frame_buffer_count);
  // Should be aligned by 8 bytes to access double and int64_t efficiently.
  // TODO: Maybe calculate the optimal alignment in FrameLayout.
  dense_scalar_layout_size_ = (scalar_layout_->AllocSize() + 7) & ~7;
  buffer_.resize(dense_scalar_layout_size_ * frame_buffer_count);
  for (size_t i = 0; i < frame_buffer_count; ++i) {
    void* alloc_ptr = GetAllocByIndex(i);
    scalar_layout->InitializeAlignedAlloc(alloc_ptr);
    frames_.emplace_back(alloc_ptr, scalar_layout);
    const_frames_.emplace_back(alloc_ptr, scalar_layout);
  }
  for (auto& copier : input_copiers_) copier->Start();
  for (auto& copier : output_copiers_) copier->Start(row_count);
}

FrameIterator::~FrameIterator() {
  for (size_t i = 0; i < frames_.size(); ++i) {
    scalar_layout_->DestroyAlloc(GetAllocByIndex(i));
  }
}

absl::Status FrameIterator::StoreOutput(FramePtr output_frame) {
  for (std::unique_ptr<BatchFromFramesCopier>& copier : output_copiers_) {
    RETURN_IF_ERROR(copier->Finalize(output_frame));
  }
  return absl::OkStatus();
}

void FrameIterator::PreloadFrames(size_t frames_count) {
  for (auto& copier : input_copiers_) {
    copier->CopyNextBatch({frames_.data(), frames_count});
  }
}

void FrameIterator::SaveOutputsOfProcessedFrames(size_t frames_count) {
  for (auto& copier : output_copiers_) {
    absl::Status status =
        copier->CopyNextBatch({const_frames_.data(), frames_count});
    // Copier returns error status only if CopyNextBatch is called before
    // copier->Start(row_count). Here status is always OK, so we can ignore it.
    DCHECK_OK(status);
  }
}

}  // namespace arolla
