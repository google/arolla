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
#ifndef AROLLA_MEMORY_RAW_BUFFER_FACTORY_H_
#define AROLLA_MEMORY_RAW_BUFFER_FACTORY_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>

#include "absl/base/no_destructor.h"
#include "absl/container/inlined_vector.h"
#include "google/protobuf/arena.h"

namespace arolla {

// Pointer to a buffer object, like a memory block allocated by malloc, or
// arrow.Buffer. shared_ptr is used to manage its lifetime and should have an
// appropriate deleter for this object. Since it can be a pointer not to the
// allocation directly, but to some object which owns the allocation, it
// shouldn't be used for getting memory address of the buffer.
using RawBufferPtr = std::shared_ptr<const void>;

// Interface for creating new RawBuffers. RawBufferFactory is not guaranteed
// to be thread safe.
// However the allocated buffers itself are thread safe.
class RawBufferFactory {
 public:
  virtual ~RawBufferFactory() = default;

  // Method for creating new raw buffers provided by derived class. The
  // returned shared_ptr controls the lifetime of the buffer, while
  // returned raw pointer can be used to initialize the buffer.
  virtual std::tuple<RawBufferPtr, void*> CreateRawBuffer(size_t nbytes) = 0;

  // Resizes raw buffer. This method may only be used on buffers which were
  // created by the same buffer factory, and which are known to be uniquely
  // owned. Any other use is unsafe.
  // Resizing can be done by either:
  //   a) expanding or contracting the existing area pointed to by ptr, if
  // possible. The contents of the area remain unchanged up to the lesser of the
  // new and old sizes. If the area is expanded, the contents of the new part of
  // the array are undefined.
  //   b) allocating a new memory block of size new_size bytes, copying memory
  // area with size equal the lesser of the new and the old sizes, and freeing
  // the old block.
  virtual std::tuple<RawBufferPtr, void*> ReallocRawBuffer(
      RawBufferPtr&& old_buffer, void* data, size_t old_size,
      size_t new_size) = 0;
};

// Buffer factory that allocates buffer on heap.
class HeapBufferFactory : public RawBufferFactory {
 public:
  std::tuple<RawBufferPtr, void*> CreateRawBuffer(size_t nbytes) final;
  std::tuple<RawBufferPtr, void*> ReallocRawBuffer(RawBufferPtr&& old_buffer,
                                                   void* old_data,
                                                   size_t old_size,
                                                   size_t new_size) final;
};

// Returns non owning singleton buffer factory allocating on heap.
// Heap buffer factory is thread safe.
// NOTE: the function is defined in the header file on purpose. It is called on
// every model evaluation, so inlining it gives 2-4ns speedup, which may be
// important for smaller codegen models.
inline RawBufferFactory* GetHeapBufferFactory() {
  static absl::NoDestructor<HeapBufferFactory> factory;
  return factory.get();
}

// Provides BufferFactory interface for google::protobuf::Arena. All buffers will be
// allocated inside the given google::protobuf::Arena. The arena should outlive
// the BufferFactory.
class ProtobufArenaBufferFactory final : public RawBufferFactory {
 public:
  explicit ProtobufArenaBufferFactory(google::protobuf::Arena& arena) : arena_(arena) {}

  std::tuple<RawBufferPtr, void*> CreateRawBuffer(size_t nbytes) override;

  std::tuple<RawBufferPtr, void*> ReallocRawBuffer(RawBufferPtr&& old_buffer,
                                                   void* data, size_t old_size,
                                                   size_t new_size) override;

 private:
  google::protobuf::Arena& arena_;
};

// Allows to preallocate space for multiple temporary buffers.
// Automatically resizes if necessary. Returns unowned buffers that become
// invalid when the factory is destroyed or when Reset() is called.
class UnsafeArenaBufferFactory : public RawBufferFactory {
 public:
  UnsafeArenaBufferFactory(const UnsafeArenaBufferFactory&) = delete;
  UnsafeArenaBufferFactory& operator=(const UnsafeArenaBufferFactory&) = delete;
  // Dangerous. Some classes may store pointer to the arena.
  UnsafeArenaBufferFactory(UnsafeArenaBufferFactory&&) = delete;
  UnsafeArenaBufferFactory& operator=(UnsafeArenaBufferFactory&&) = delete;

  // Recommendation: average allocation should be 0-5% of the page_size.
  // All pages will be allocated via given `base_factory`. `base_factory` should
  // outlive the arena. `GetHeapBufferFactory()` returns a global indestructible
  // variable, so the default argument is always safe to use.
  // Note: after resetting the arena may still keep memory allocated in
  // the `base_factory`.
  explicit UnsafeArenaBufferFactory(
      int64_t page_size,
      RawBufferFactory& base_factory = *GetHeapBufferFactory())
      : page_size_(page_size), base_factory_(base_factory) {}

  std::tuple<RawBufferPtr, void*> CreateRawBuffer(size_t nbytes) override;

  // NOTE: only the most recently allocated buffer can be resized efficiently
  // and release memory if new_size < old_size.
  std::tuple<RawBufferPtr, void*> ReallocRawBuffer(RawBufferPtr&& old_buffer,
                                                   void* data, size_t old_size,
                                                   size_t new_size) override;

  // Reset internal state. All previously allocated buffers become invalid and
  // memory will be reused for next allocations. To release the memory Arena
  // should destroyed or recreated (e.g. arena=UnsafeArenaBufferFactory(size)).
  void Reset();

 private:
  using Alloc = std::tuple<RawBufferPtr, void*>;
  void NextPage();
  void* SlowAlloc(size_t nbytes);

  int64_t page_id_ = -1;

  // We use 0x8 instead of nullptr to workaround UBSAN error. If
  // current_ == end_, the actual value is not important, can be any constant.
  char* current_ = reinterpret_cast<char*>(0x8);
  char* end_ = reinterpret_cast<char*>(0x8);

  int64_t page_size_;
  RawBufferFactory& base_factory_;
  absl::InlinedVector<Alloc, 16> pages_;
  absl::InlinedVector<Alloc, 16> big_allocs_;
};

// Types that can be unowned should overload ArenaTraits. Should be used
// in ModelExecutor to make the result owned even if was created using
// UnsafeArenaBufferFactory. It is a default implementation that does nothing.
template <typename T>
struct ArenaTraits {
  static T MakeOwned(T&& v, RawBufferFactory*) { return std::move(v); }
};

}  // namespace arolla

#endif  // AROLLA_MEMORY_RAW_BUFFER_FACTORY_H_
