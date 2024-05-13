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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "google/protobuf/arena.h"

namespace arolla {
namespace {

using ::testing::AnyOf;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::Le;

// Under asan reading uninitialized memory may be considered as an error.
// We do intentionally skip initialization and read uninitialized memory
// for performance reasons.
// Asan detect problems with enums and bools the best.
// We do initialize memory in asan mode, here we verify that.
void VerifyCanReadUninitialized(const void* ptr, size_t size) {
  const char* char_ptr = static_cast<const char*>(ptr);
  for (size_t i = 0; i != size; ++i) {
    // Note that it works with `char` but wouldn't work with `bool` because
    // for bool only bytes 0 or 1 are valid.
    char c = *(char_ptr + i);
    benchmark::DoNotOptimize(c);
  }
}

TEST(HeapBufferFactory, CreateEmptyBuffer) {
  auto [buf, data] = GetHeapBufferFactory()->CreateRawBuffer(0);
  EXPECT_EQ(buf, nullptr);
  EXPECT_EQ(data, nullptr);
}

TEST(HeapBufferFactory, CreateRawBuffer) {
  const size_t size = 13;
  auto [buf, data] = GetHeapBufferFactory()->CreateRawBuffer(size);
  EXPECT_NE(buf, nullptr);
  VerifyCanReadUninitialized(data, size);
  EXPECT_EQ(reinterpret_cast<size_t>(data) & 7, 0);  // Check alignment.
  memset(data, 0, size);
}

TEST(HeapBufferFactory, ReallocRawBuffer) {
  size_t size = 13;
  RawBufferPtr buf;
  char* data;
  {
    auto res = GetHeapBufferFactory()->CreateRawBuffer(size);
    buf = std::get<0>(res);
    data = reinterpret_cast<char*>(std::get<1>(res));
    VerifyCanReadUninitialized(data, size);
  }
  auto resize_fn = [&](size_t new_size) {
    auto res = GetHeapBufferFactory()->ReallocRawBuffer(std::move(buf), data,
                                                        size, new_size);
    buf = std::get<0>(res);
    data = reinterpret_cast<char*>(std::get<1>(res));
    size = new_size;
  };

  data[0] = 5;
  resize_fn(4);
  EXPECT_EQ(data[0], 5);
  VerifyCanReadUninitialized(data + 1, size - 1);
  resize_fn(145);
  EXPECT_EQ(data[0], 5);
  VerifyCanReadUninitialized(data + 1, 144);
}

TEST(ProtobufArenaBufferFactory, CreateAndResize) {
  google::protobuf::Arena arena;
  ProtobufArenaBufferFactory buf_factory(arena);
  auto [buf1, data1] = buf_factory.CreateRawBuffer(2);
  VerifyCanReadUninitialized(data1, 2);
  char* d = reinterpret_cast<char*>(data1);
  d[0] = 'A';
  d[1] = 'B';
  auto [buf2, data2] =
      buf_factory.ReallocRawBuffer(std::move(buf1), data1, 2, 1);
  EXPECT_EQ(data1, data2);
  auto [buf3, data3] =
      buf_factory.ReallocRawBuffer(std::move(buf2), data2, 1, 3);
  EXPECT_NE(data2, data3);
  d = reinterpret_cast<char*>(data3);
  EXPECT_EQ(d[0], 'A');
  VerifyCanReadUninitialized(d + 1, 2);
}

TEST(UnsafeArenaBufferFactory, CreateEmptyBuffer) {
  UnsafeArenaBufferFactory arena(25);

  auto [buf1, data1] = arena.CreateRawBuffer(0);
  auto [buf2, data2] = arena.CreateRawBuffer(0);
  auto [buf3, data3] = arena.CreateRawBuffer(1);
  VerifyCanReadUninitialized(data3, 1);
  auto [buf4, data4] = arena.CreateRawBuffer(0);
  auto [buf5, data5] = arena.CreateRawBuffer(0);

  EXPECT_EQ(data1, data2);
  EXPECT_NE(data3, nullptr);
  EXPECT_NE(data2, data4);
  EXPECT_NE(data3, data4);
  EXPECT_EQ(data4, data5);
}

TEST(UnsafeArenaBufferFactory, CreateRawBuffer) {
  std::vector<int64_t> sizes = {17, 1, 15, 1, 10};
  std::vector<RawBufferPtr> bufs;
  std::vector<char*> ptrs;
  bufs.reserve(sizes.size());
  ptrs.reserve(sizes.size());

  UnsafeArenaBufferFactory arena1(25);

  google::protobuf::Arena proto_arena;
  ProtobufArenaBufferFactory proto_buf_factory(proto_arena);
  UnsafeArenaBufferFactory arena2(25, proto_buf_factory);

  for (UnsafeArenaBufferFactory* arena_ptr : {&arena1, &arena2}) {
    UnsafeArenaBufferFactory& arena = *arena_ptr;

    for (size_t i = 0; i < sizes.size(); ++i) {
      auto [buf, data] = arena.CreateRawBuffer(sizes[i]);
      VerifyCanReadUninitialized(data, sizes[i]);
      EXPECT_EQ(reinterpret_cast<size_t>(data) & 7, 0);
      memset(data, i, sizes[i]);
      bufs.push_back(buf);
      ptrs.push_back(reinterpret_cast<char*>(data));
    }

    EXPECT_EQ(ptrs[0] + 24, ptrs[1]);  // both on page 0
    EXPECT_EQ(ptrs[2] + 16, ptrs[3]);  // both on page 1

    for (size_t i = 0; i < sizes.size(); ++i) {
      for (int64_t j = 0; j < sizes[i]; ++j) {
        EXPECT_EQ(ptrs[i][j], i);
      }
    }
  }
}

TEST(UnsafeArenaBufferFactory, ReallocRawBuffer) {
  UnsafeArenaBufferFactory arena1(25);

  google::protobuf::Arena proto_arena;
  ProtobufArenaBufferFactory proto_buf_factory(proto_arena);
  UnsafeArenaBufferFactory arena2(25, proto_buf_factory);

  for (UnsafeArenaBufferFactory* arena_ptr : {&arena1, &arena2}) {
    UnsafeArenaBufferFactory& arena = *arena_ptr;

    auto [buf1, data1] = arena.CreateRawBuffer(10);
    VerifyCanReadUninitialized(data1, 10);
    EXPECT_EQ(buf1, nullptr);
    reinterpret_cast<char*>(data1)[0] = 7;
    auto [buf2, data2] = arena.ReallocRawBuffer(std::move(buf1), data1, 10, 25);
    // initialize to verify that reused memory will be reinitialized under asan
    reinterpret_cast<char*>(data1)[24] = -1;
    EXPECT_EQ(reinterpret_cast<char*>(data2)[0], 7);
    EXPECT_EQ(data1, data2);
    // 26 > page_size, so it uses big_alloc and moves the buffer out of the page
    auto [buf3, data3] = arena.ReallocRawBuffer(std::move(buf2), data2, 25, 26);
    VerifyCanReadUninitialized(data2, 25);  // buf2 is removed and reinitialized
    EXPECT_NE(data1, data3);
    EXPECT_EQ(reinterpret_cast<char*>(data3)[0], 7);
    auto [buf4, data4] = arena.ReallocRawBuffer(std::move(buf3), data3, 26, 10);
    EXPECT_NE(data1, data4);
    EXPECT_EQ(reinterpret_cast<char*>(data4)[0], 7);

    // Here we check that buf2 is removed and the page can be reused.
    auto [buf5, data5] = arena.CreateRawBuffer(20);
    VerifyCanReadUninitialized(data5, 20);
    auto [buf6, data6] = arena.ReallocRawBuffer(std::move(buf5), data5, 20, 15);
    VerifyCanReadUninitialized(static_cast<const char*>(data6) + 15, 5);
    EXPECT_EQ(data1, data5);
    EXPECT_EQ(data1, data6);

    // Test moving to the next page during realloc.
    auto [buf7, data7] = arena.CreateRawBuffer(8);
    VerifyCanReadUninitialized(data7, 8);
    EXPECT_EQ(reinterpret_cast<char*>(data1) + 16,
              reinterpret_cast<char*>(data7));
    reinterpret_cast<char*>(data7)[0] = 3;
    auto [buf8, data8] = arena.ReallocRawBuffer(std::move(buf7), data7, 8, 20);
    EXPECT_EQ(reinterpret_cast<char*>(data8)[0], 3);

    // Check that buf8 is not a big_alloc and was moved to the next page instead
    auto [buf9, data9] = arena.CreateRawBuffer(1);
    VerifyCanReadUninitialized(data9, 1);
    EXPECT_EQ(reinterpret_cast<char*>(data8) + 24,
              reinterpret_cast<char*>(data9));
  }
}

TEST(UnsafeArenaBufferFactory, BigAlloc) {
  UnsafeArenaBufferFactory arena1(32);

  google::protobuf::Arena proto_arena;
  ProtobufArenaBufferFactory proto_buf_factory(proto_arena);
  UnsafeArenaBufferFactory arena2(32, proto_buf_factory);

  for (UnsafeArenaBufferFactory* arena_ptr : {&arena1, &arena2}) {
    UnsafeArenaBufferFactory& arena = *arena_ptr;

    auto [buf1, data1] = arena.CreateRawBuffer(16);
    VerifyCanReadUninitialized(data1, 16);
    auto [buf2, data2] = arena.CreateRawBuffer(64);
    VerifyCanReadUninitialized(data2, 64);
    auto [buf3, data3] = arena.CreateRawBuffer(16);
    VerifyCanReadUninitialized(data3, 16);

    EXPECT_THAT(reinterpret_cast<char*>(data3),
                Eq(reinterpret_cast<char*>(data1) + 16));
    EXPECT_THAT(reinterpret_cast<char*>(data2) - reinterpret_cast<char*>(data1),
                AnyOf(Le(-64), Ge(32)));
    // Check data2 with address sanitizer.
    memset(data2, 0, 64);
    EXPECT_THAT(reinterpret_cast<int64_t*>(data2)[0], Eq(0));
  }
}

TEST(UnsafeArenaBufferFactory, Reset) {
  UnsafeArenaBufferFactory arena1(32);

  google::protobuf::Arena proto_arena;
  ProtobufArenaBufferFactory proto_buf_factory(proto_arena);
  UnsafeArenaBufferFactory arena2(32, proto_buf_factory);

  for (UnsafeArenaBufferFactory* arena_ptr : {&arena1, &arena2}) {
    UnsafeArenaBufferFactory& arena = *arena_ptr;

    arena.Reset();
    auto [buf1, data1] = arena.CreateRawBuffer(16);
    VerifyCanReadUninitialized(data1, 16);
    auto [buf2, data2] = arena.CreateRawBuffer(16);
    VerifyCanReadUninitialized(data2, 16);
    auto [buf3, data3] = arena.CreateRawBuffer(16);
    VerifyCanReadUninitialized(data3, 16);

    // initialize to verify that new allocations will be reinitialized.
    std::memset(data1, 255, 16);
    std::memset(data2, 255, 16);
    std::memset(data3, 255, 16);
    arena.Reset();
    auto [buf4, data4] = arena.CreateRawBuffer(8);
    VerifyCanReadUninitialized(data4, 16);
    auto [buf5, data5] = arena.CreateRawBuffer(16);
    VerifyCanReadUninitialized(data5, 16);
    auto [buf6, data6] = arena.CreateRawBuffer(24);
    VerifyCanReadUninitialized(data6, 16);

    EXPECT_EQ(data1, data4);
    EXPECT_EQ(reinterpret_cast<char*>(data2),
              reinterpret_cast<char*>(data5) + 8);
    EXPECT_EQ(data3, data6);
  }
}

TEST(UnsafeArenaBufferFactory, BaseFactory) {
  UnsafeArenaBufferFactory arena1(1024);
  auto [buf_before, ptr_before] = arena1.CreateRawBuffer(1);

  UnsafeArenaBufferFactory arena2(32, arena1);
  auto [buf_small, ptr_small] = arena2.CreateRawBuffer(8);
  auto [buf_big, ptr_big] = arena2.CreateRawBuffer(128);

  auto [buf_after, ptr_after] = arena1.CreateRawBuffer(1);

  EXPECT_LT(ptr_before, ptr_small);
  EXPECT_LT(ptr_before, ptr_big);
  EXPECT_GT(ptr_after, ptr_small);
  EXPECT_GT(ptr_after, ptr_big);
}

}  // namespace
}  // namespace arolla
