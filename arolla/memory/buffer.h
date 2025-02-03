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
#ifndef AROLLA_MEMORY_BUFFER_H_
#define AROLLA_MEMORY_BUFFER_H_

#include <initializer_list>
#include <string>
#include <variant>
#include <vector>

#include "absl/strings/string_view.h"
#include "arolla/memory/simple_buffer.h"
#include "arolla/memory/strings_buffer.h"  // IWYU pragma: export
#include "arolla/memory/void_buffer.h"     // IWYU pragma: export
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace arolla {

// Handle to a contiguous, immutable, typed block of memory.
//
// Buffers may be "owned" or "unowned". "owned" buffers hold a reference
// counted pointer to an underlying object which controls the buffer's
// lifetime. "unowned" buffers are transient, and do not control the lifetime
// of their underlying data. Unowned buffers have two use cases:
// 1) Wrapping an externally allocated buffer where there is no possibility to
//        control ownership at all.
// 2) Creating an unowned buffer from owned is slightly faster than copying an
//    owned buffer (no need to update usages counter). So unowned buffers are
//        sometimes useful in performance critical places.
//
// Buffers are copyable and movable.
//
// Taking ownership of an "unowned" buffer requires copying the buffer's data
// into a new "owned" buffer. This can be accomplished using the DeepCopy
// method.
//
// Buffers may be sliced, which returns a new Buffer containing a subset of
// the original buffer's data. By default, ownership of the underlying data
// is preserved when slicing.
//
// By default Buffer<T> is an alias to SimpleBuffer<T>. SimpleBuffer only
// support trivial element types. Other buffer types can be added by overloading
// BufferTraits. See strings_buffer.h as an example.
//
// There are several ways to create a new buffer:
// 1) From any container by copying all data.
//   Buffer<T> buffer = Buffer<T>::Create(v.begin(), v.end());
//   Buffer<int> buffer = Buffer<int>::Create({1, 2});  // initializer_list
//
// 2) Get mutable data and fill manually. In theory it is the fastest way since
//       user can explicitly use vector instructions and multithreading. Other
//       ways in general are not guaranteed to be threadsafe.
//       NOTE: Buffer<std::string>::Builder doesn't support GetMutableSpan.
//   Buffer<T>::Builder builder(max_size);
//   absl::Span<T> mutable_data = builder.GetMutableSpan();
//   ...  // fill mutable_data
//   Buffer<T> buffer = std::move(builder).Build(size);
//
// 3) Add elements one by one. Separate class for Inserter is used for
//       performance reasons. In some cases it allows compiler to vectorize code
//       automatically.
//   Buffer<T>::Builder builder(max_size);
//   Buffer<T>::Inserter inserter = builder.GetInserter();
//   while (...) inserter.Add(v);
//   Buffer<T> buffer = std::move(builder).Build(inserter);
//
// 4) Pass a value generator. Performance is the same as in previous way.
//   Buffer<T>::Builder builder(max_size);
//   builder.SetN(first, count, [&i]() { return generate_value(i++); });
//   Buffer<T> buffer = std::move(builder).Build(size);
//
// 5) Use random access building. In simple cases performance is worse than for
//       3 and 4 because it can not be autovectorized.
//   Buffer<T>::Builder builder(max_size);
//   while (...) builder.Set(id, v);
//   Buffer<T> buffer = std::move(builder).Build(size);

template <typename T>
struct BufferTraits {
  using buffer_type = SimpleBuffer<T>;
};

template <>
struct BufferTraits<std::monostate> {
  using buffer_type = VoidBuffer;
};

template <>
struct BufferTraits<std::string> {
  using buffer_type = StringsBuffer;
};

template <>
struct BufferTraits<absl::string_view> {
  using buffer_type = StringsBuffer;
};

template <>
struct BufferTraits<Text> {
  using buffer_type = StringsBuffer;
};

template <typename T>
using Buffer = typename BufferTraits<T>::buffer_type;

template <typename T>
Buffer<T> CreateBuffer(std::initializer_list<T> values) {
  return Buffer<T>::Create(values.begin(), values.end());
}
template <typename T>
Buffer<T> CreateBuffer(const std::vector<T>& values) {
  return Buffer<T>::Create(values.begin(), values.end());
}

}  // namespace arolla

#endif  // AROLLA_MEMORY_BUFFER_H_
