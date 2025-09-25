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
#ifndef AROLLA_UTIL_THREAD_SAFE_SHARED_PTR_H_
#define AROLLA_UTIL_THREAD_SAFE_SHARED_PTR_H_

#include <cstddef>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"

namespace arolla {

// This class provides a thread-safe access to a std::shared_ptr<T> object.
//
// TODO: Consider replacing with std::atomic<std::shared_ptr<T>>
// once available.
template <typename T>
class ThreadSafeSharedPtr {
 public:
  ThreadSafeSharedPtr() = default;

  explicit ThreadSafeSharedPtr(std::shared_ptr<T> ptr) : ptr_(std::move(ptr)) {}

  ThreadSafeSharedPtr(const ThreadSafeSharedPtr&) = delete;
  ThreadSafeSharedPtr& operator=(const ThreadSafeSharedPtr&) = delete;

  bool operator==(std::nullptr_t) const ABSL_LOCKS_EXCLUDED(mx_) {
    absl::MutexLock guard(mx_);
    return ptr_ == nullptr;
  }

  bool operator!=(std::nullptr_t) const ABSL_LOCKS_EXCLUDED(mx_) {
    return !(*this == nullptr);
  }

  std::shared_ptr<T> load() const ABSL_LOCKS_EXCLUDED(mx_) {
    absl::MutexLock guard(mx_);
    return ptr_;
  }

  void store(std::shared_ptr<T> ptr) ABSL_LOCKS_EXCLUDED(mx_) {
    absl::MutexLock guard(mx_);
    ptr.swap(ptr_);
  }

 private:
  std::shared_ptr<T> ptr_ ABSL_GUARDED_BY(mx_);
  mutable absl::Mutex mx_;
};

}  // namespace arolla

#endif  // AROLLA_UTIL_THREAD_SAFE_SHARED_PTR_H_
