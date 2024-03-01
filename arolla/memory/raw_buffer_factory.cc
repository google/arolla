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
#include "arolla/memory/raw_buffer_factory.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <tuple>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"  // IWYU pragma: keep
#include "absl/log/check.h"

namespace arolla {

namespace {

// Used in HeapBufferFactory::ReallocRawBuffer to replace existing deleter.
// Should have exactly the same prototype as 'free'.
void noop_free(void*) noexcept {}

// We intentionally reading uninitialized memory in many operations,
// which is considered an error under UBSAN.
// UBSAN is typically enabled together with other sanitizers.
#if defined(ABSL_HAVE_ADDRESS_SANITIZER) || \
    defined(ABSL_HAVE_MEMORY_SANITIZER) || defined(ABSL_HAVE_THREAD_SANITIZER)
#define AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER 1
#endif

#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
// Initializes the memory with either `0` or `1` bytes.
// Only `0` and `1` are valid values for booleans.
// Always initializing with `0` can hide errors with accidental access to
// uninitialized memory.
// Always initializing with `1` can also cause issues, when code rely
// on equality of non initialized data.
void InitializeMemoryForSanitizer(void* data, size_t size) {
  // absl::Hash provides good bits distribution within the process for different
  // pointers and instability for different runs.
  // This will reduce probability that code rely on buffer being initialized
  // in a particular way.
  std::memset(data, absl::Hash<const void*>()(data) % 2, size);
}
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER

}  // namespace

std::tuple<RawBufferPtr, void*> HeapBufferFactory::CreateRawBuffer(
    size_t nbytes) {
  if (ABSL_PREDICT_FALSE(nbytes == 0)) return {nullptr, nullptr};
  void* data = malloc(nbytes);
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  InitializeMemoryForSanitizer(data, nbytes);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  return {std::shared_ptr<void>(data, free), data};
}

std::tuple<RawBufferPtr, void*> HeapBufferFactory::ReallocRawBuffer(
    RawBufferPtr&& old_buffer, void* old_data, size_t old_size,
    size_t new_size) {
  if (new_size == 0) return {nullptr, nullptr};
  if (old_size == 0) return CreateRawBuffer(new_size);
  DCHECK_EQ(old_buffer.use_count(), 1);

  void* new_data = realloc(old_data, new_size);
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  if (new_size > old_size) {
    InitializeMemoryForSanitizer(static_cast<char*>(new_data) + old_size,
                                 new_size - old_size);
  }
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER

  // Remove old deleter to prevent freeing old_data during reset.
  *std::get_deleter<decltype(&free)>(old_buffer) = &noop_free;
  old_buffer.reset(new_data, free);

  return {std::move(old_buffer), new_data};
}

std::tuple<RawBufferPtr, void*> ProtobufArenaBufferFactory::CreateRawBuffer(
    size_t nbytes) {
  char* data = arena_.CreateArray<char>(&arena_, nbytes);
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  InitializeMemoryForSanitizer(data, nbytes);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  return {nullptr, data};
}

std::tuple<RawBufferPtr, void*> ProtobufArenaBufferFactory::ReallocRawBuffer(
    RawBufferPtr&& old_buffer, void* data, size_t old_size, size_t new_size) {
  if (old_size >= new_size) return {nullptr, data};
  char* new_data = arena_.CreateArray<char>(&arena_, new_size);
  memcpy(new_data, data, std::min(old_size, new_size));
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  InitializeMemoryForSanitizer(new_data + old_size, new_size - old_size);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  return {nullptr, new_data};
}

std::tuple<RawBufferPtr, void*> UnsafeArenaBufferFactory::CreateRawBuffer(
    size_t nbytes) {
  auto last_alloc =  // 8 bytes alignment
      reinterpret_cast<char*>(reinterpret_cast<size_t>(current_ + 7) & ~7ull);
  if (ABSL_PREDICT_FALSE(last_alloc + nbytes > end_)) {
    return {nullptr, SlowAlloc(nbytes)};
  }
  current_ = last_alloc + nbytes;
  return {nullptr, last_alloc};
}

std::tuple<RawBufferPtr, void*> UnsafeArenaBufferFactory::ReallocRawBuffer(
    RawBufferPtr&& old_buffer, void* data, size_t old_size, size_t new_size) {
  char* last_alloc = current_ - old_size;
  if ((data != last_alloc) || last_alloc + new_size > end_) {
    if (old_size >= new_size) return {nullptr, data};
    if (data == last_alloc) current_ = last_alloc;
    void* new_data = SlowAlloc(new_size);
    memcpy(new_data, data, std::min(old_size, new_size));
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
    InitializeMemoryForSanitizer(data, old_size);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
    return {nullptr, new_data};
  }
  current_ = last_alloc + new_size;
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  if (new_size < old_size) {
    InitializeMemoryForSanitizer(current_, old_size - new_size);
  }
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  return {nullptr, last_alloc};
}

void UnsafeArenaBufferFactory::Reset() {
  if (page_id_ >= 0) {
    page_id_ = 0;
    current_ = reinterpret_cast<char*>(std::get<1>(pages_[0]));
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
    InitializeMemoryForSanitizer(current_, page_size_);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
    end_ = current_ + page_size_;
  }
  big_allocs_.clear();
}

ABSL_ATTRIBUTE_NOINLINE void* UnsafeArenaBufferFactory::SlowAlloc(
    size_t nbytes) {
  if (ABSL_PREDICT_FALSE(nbytes > page_size_ ||
                         end_ - current_ >= page_size_ / 2)) {
    auto [holder, memory] = base_factory_.CreateRawBuffer(nbytes);
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
    InitializeMemoryForSanitizer(memory, nbytes);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
    big_allocs_.emplace_back(std::move(holder), memory);
    return memory;
  }
  NextPage();
  auto last_alloc = current_;
  current_ += nbytes;
  return last_alloc;
}

void UnsafeArenaBufferFactory::NextPage() {
  ++page_id_;
  if (ABSL_PREDICT_FALSE(page_id_ == pages_.size())) {
    auto [holder, page] = base_factory_.CreateRawBuffer(page_size_);
    current_ = reinterpret_cast<char*>(page);
    pages_.emplace_back(std::move(holder), page);
  } else {
    current_ = reinterpret_cast<char*>(std::get<1>(pages_[page_id_]));
  }
#ifdef AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  InitializeMemoryForSanitizer(current_, page_size_);
#endif  // AROLLA_INITIALIZE_MEMORY_FOR_SANITIZER
  end_ = current_ + page_size_;
}

}  // namespace arolla
