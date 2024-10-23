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
#ifndef AROLLA_UTIL_REFCOUNT_H_
#define AROLLA_UTIL_REFCOUNT_H_

#include <atomic>
#include <cstdint>

namespace arolla {

// Compact class for reference counting.
//
// (Based on absl/strings/internal/cord_internal.h)
class Refcount {
 public:
  constexpr Refcount() noexcept : count_{1} {}

  // Non-copyable, non-movable.
  Refcount(const Refcount& other) = delete;
  Refcount& operator=(const Refcount& other) = delete;

  // Increments the reference count.
  //
  // Imposes no memory ordering.
  void increment() noexcept { count_.fetch_add(1, std::memory_order_relaxed); }

  // Decrements the reference count.
  // Returns false if there are no references outstanding; true otherwise.
  //
  // Inserts barriers to ensure that state written before this method returns
  // false will be visible to a thread that just observed this method returning
  // false.
  [[nodiscard]] bool decrement() noexcept {
    return count_.fetch_sub(1, std::memory_order_acq_rel) != 1;
  }

  // Decrements the reference count.
  //
  // If you expect the reference count to be 1, this method can be more
  // efficient than decrement():
  //
  //   BM_Refcount_Decrement                   5.34ns ± 1%
  //   BM_Refcount_SkewedDecrement (last)      0.60ns ± 1%
  //   BM_Refcount_SkewedDecrement (non-last)  9.19ns ± 0%
  //
  // Inserts barriers to ensure that state written before this method returns
  // false will be visible to a thread that just observed this method returning
  // false.
  [[nodiscard]] bool skewed_decrement() noexcept {
    auto refcount = count_.load(std::memory_order_acquire);
    return refcount != 1 && count_.fetch_sub(1, std::memory_order_acq_rel) != 1;
  }

  // A custom  constructor used for testing purposes.
  struct TestOnly {};
  constexpr Refcount(TestOnly, int initial_count) noexcept
      : count_{initial_count} {}

 private:
  std::atomic<int32_t> count_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_REFCOUNT_H_
