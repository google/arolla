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
#ifndef AROLLA_QTYPE_ARRAY_LIKE_FRAME_ITER_H_
#define AROLLA_QTYPE_ARRAY_LIKE_FRAME_ITER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "absl//log/check.h"
#include "absl//status/statusor.h"
#include "absl//synchronization/barrier.h"
#include "absl//types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/threading.h"

namespace arolla {

// This class iterates over set of frames and for each frame gets input scalar
// values from the input arrays and stores output values to the output arrays.
// All array implementations with QType inherited from ArrayLikeQType are
// supported.
//
// Usage example (see frame_iter_test.cc for more details):
//
// auto frame_iterator = FrameIterator::Create(
//      input_arrays, input_scalar_slots,
//      output_array_slots, output_scalar_slots,
//      scalar_layout).value();
//
// frame_iterator.ForEachFrame([&](FramePtr scalar_frame) {
//   scalar_evaluator.Eval(scalar_frame);
// });
// frame_iterator.StoreOutput(output_frame);

class FrameIterator {
 public:
  FrameIterator(const FrameIterator&) = delete;
  FrameIterator(FrameIterator&&) = default;
  ~FrameIterator();

  struct Options {
    static constexpr Options Default() { return {}; }
    // Size of input and output arrays. By default will be taken from
    // the input arrays.
    std::optional<int64_t> row_count;
    // The number of frames to store in a buffer.
    int64_t frame_buffer_count = 64;
    // Buffer factory for output arrays allocation.
    RawBufferFactory* buffer_factory = nullptr;
  };

  // Creates FrameIterator from lists of arrays and scalar slots.
  // On each iteration values from input_arrays will be copied to
  // input_scalar_slots and values from output_scalar_slots (calculated on
  // previous iteration) will be stored to output arrays.
  // input_arrays.size() should be equal to input_scalar_slots.size().
  // output_array_slots.size() should be equal to output_scalar_slots.size().
  // scalar_layout - frame layout for the input and output scalar slots.
  // options - optional settings.
  static absl::StatusOr<FrameIterator> Create(
      absl::Span<const TypedRef> input_arrays,
      absl::Span<const TypedSlot> input_scalar_slots,
      absl::Span<const TypedSlot> output_array_slots,
      absl::Span<const TypedSlot> output_scalar_slots,
      const FrameLayout* scalar_layout, Options options = Options::Default());

  // Apply fn to all allocated frame buffers. Used to initialize values which
  // are independent of the input arrays. Can be called only prior the
  // ForEachFrame() call.
  template <class Fn>
  void CustomFrameInitialization(Fn&& fn) {
    for (auto& frame : frames_) fn(frame);
  }

  int64_t row_count() const { return row_count_; }

  // For each row creates a frame using one element from each input array.
  // Applies `fn` to each of the frames.
  template <typename Fn>
  void ForEachFrame(Fn&& fn) {
    for (int64_t offset = 0; offset < row_count_; offset += frames_.size()) {
      int64_t count = std::min<int64_t>(frames_.size(), row_count_ - offset);
      PreloadFrames(count);
      for (int64_t i = 0; i < count; ++i) {
        fn(frames_[i]);
      }
      SaveOutputsOfProcessedFrames(count);
    }
  }

  // The same as ForEachFrame(fn), but can use several threads.
  template <typename Fn>
  void ForEachFrame(Fn&& fn, ThreadingInterface& threading, int thread_count) {
    DCHECK_GE(thread_count, 1);
    const int frames_per_worker =
        (frames_.size() + thread_count - 1) / thread_count;

    auto barrier1 = std::make_unique<absl::Barrier>(thread_count);
    auto barrier2 = std::make_unique<absl::Barrier>(thread_count);
    auto BarrierSync = [thread_count](std::unique_ptr<absl::Barrier>& b) {
      if (b->Block()) {
        b = std::make_unique<absl::Barrier>(thread_count);
      }
    };

    auto worker_fn = [&](int worker_id) {
      for (int64_t offset = 0; offset < row_count_; offset += frames_.size()) {
        int64_t count = std::min<int64_t>(frames_.size(), row_count_ - offset);
        if (worker_id == 0) {
          PreloadFrames(count);
        }
        BarrierSync(barrier1);
        for (int64_t i = worker_id * frames_per_worker;
             i < std::min<int64_t>(count, (worker_id + 1) * frames_per_worker);
             ++i) {
          fn(frames_[i]);
        }
        BarrierSync(barrier2);
        if (worker_id == 0) {
          SaveOutputsOfProcessedFrames(count);
        }
      }
    };

    threading.WithThreading([&] {
      std::vector<std::function<void()>> join_fns;
      join_fns.reserve(thread_count - 1);
      for (int i = 1; i < thread_count; ++i) {
        join_fns.push_back(
            threading.StartThread([&worker_fn, i] { worker_fn(i); }));
      }
      worker_fn(0);
      for (auto& join : join_fns) join();
    });
  }

  // Stores output arrays to given frame. Can be called only once after the last
  // iteration. Can be skipped if there is no output arrays.
  absl::Status StoreOutput(FramePtr output_frame);

 private:
  void* GetAllocByIndex(size_t index) {
    return buffer_.data() + index * dense_scalar_layout_size_;
  }

  void PreloadFrames(size_t frames_count);
  void SaveOutputsOfProcessedFrames(size_t frames_count);

  FrameIterator(
      std::vector<std::unique_ptr<BatchToFramesCopier>>&& input_copiers,
      std::vector<std::unique_ptr<BatchFromFramesCopier>>&& output_copiers,
      size_t row_count, size_t frame_buffer_count,
      const FrameLayout* scalar_layout);

  int64_t row_count_;

  std::vector<std::unique_ptr<BatchToFramesCopier>> input_copiers_;
  std::vector<std::unique_ptr<BatchFromFramesCopier>> output_copiers_;
  std::vector<FramePtr> frames_;
  // const_frames_ contains the same links as frames_. Needed because frames_
  // can not be passed to a function that expects Span<const ConstFramePtr>
  // without explicit conversion.
  std::vector<ConstFramePtr> const_frames_;
  std::vector<char> buffer_;
  const FrameLayout* scalar_layout_;
  size_t dense_scalar_layout_size_;
};

}  // namespace arolla

#endif  // AROLLA_QTYPE_ARRAY_LIKE_FRAME_ITER_H_
