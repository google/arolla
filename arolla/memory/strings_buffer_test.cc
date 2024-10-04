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
#include <array>
#include <cstddef>
#include <initializer_list>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/buffer.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;
using ::testing::Not;

class StringsBufferTest : public ::testing::Test {
 public:
  Buffer<std::string> CreateTestBuffer(int num_rows) {
    std::vector<std::string> values(num_rows);
    for (int i = 0; i < num_rows; i++) {
      values[i] = absl::StrFormat("str%d", i);
    }
    return Buffer<std::string>::Create(values.begin(), values.end());
  }

  template <typename T>
  Buffer<std::string> CreateTestBuffer(std::initializer_list<T> values) {
    return Buffer<std::string>::Create(values.begin(), values.end());
  }
};

TEST_F(StringsBufferTest, Simple) {
  Buffer<std::string> buffer = CreateTestBuffer(4);

  EXPECT_TRUE(buffer.is_owner());
  EXPECT_THAT(buffer, ElementsAre("str0", "str1", "str2", "str3"));
  EXPECT_EQ(buffer[0], "str0");
  EXPECT_EQ(buffer[3], "str3");
}

TEST_F(StringsBufferTest, Empty) {
  Buffer<std::string> buffer1 = CreateTestBuffer(0);
  EXPECT_THAT(buffer1, IsEmpty());

  Buffer<std::string> buffer2 = buffer1.DeepCopy();
  EXPECT_THAT(buffer2, IsEmpty());

  Buffer<std::string> buffer3;
  EXPECT_THAT(buffer3, IsEmpty());
}

TEST_F(StringsBufferTest, Move) {
  // Create a buffer from iterators.
  size_t num_rows = 4;
  Buffer<std::string> buffer = CreateTestBuffer(num_rows);
  EXPECT_TRUE(buffer.is_owner());

  // Move via constructor.
  Buffer<std::string> buffer2 = std::move(buffer);

  // Ownership transferred to new buffer.
  EXPECT_TRUE(buffer2.is_owner());
  EXPECT_FALSE(buffer.is_owner());  // NOLINT misc-use-after-move

  EXPECT_THAT(buffer2, ElementsAre("str0", "str1", "str2", "str3"));

  // Move via assignment.
  Buffer<std::string> buffer3;

  // empty buffer is owned.
  EXPECT_TRUE(buffer3.is_owner());

  // Ownership transfers from buffer2 to buffer3
  buffer3 = std::move(buffer2);
  EXPECT_TRUE(buffer3.is_owner());

  // buffer2 loses ownership.
  EXPECT_FALSE(buffer2.is_owner());  // NOLINT misc-use-after-move
  EXPECT_THAT(buffer3, ElementsAre("str0", "str1", "str2", "str3"));
}

TEST_F(StringsBufferTest, MemoryUsage) {
  EXPECT_EQ(sizeof(Buffer<StringsBuffer::Offsets>), 4 * sizeof(void*));
  EXPECT_EQ(sizeof(Buffer<char>), 4 * sizeof(void*));
  EXPECT_EQ(sizeof(Buffer<std::string>),
            sizeof(Buffer<StringsBuffer::Offsets>) + sizeof(Buffer<char>) + 8);

  for (size_t sz = 0; sz < 10; sz += 1) {
    // Every string is 4 bytes iff sz in [0, 9].
    const size_t chars = sz * 4;
    const size_t offsets = sz * sizeof(StringsBuffer::Offsets);

    Buffer<std::string> buffer = CreateTestBuffer(sz);
    EXPECT_EQ(chars + offsets, buffer.memory_usage());
  }
}

TEST_F(StringsBufferTest, MoveSlice) {
  // Create a buffer from iterators.
  size_t num_rows = 10;
  Buffer<std::string> buffer = CreateTestBuffer(num_rows);
  EXPECT_TRUE(buffer.is_owner());

  // Shrink buffer in place
  buffer = std::move(buffer).Slice(0, 5);
  EXPECT_TRUE(buffer.is_owner());
  EXPECT_THAT(buffer, ElementsAre("str0", "str1", "str2", "str3", "str4"));

  // Slice buffer.
  Buffer<std::string> buffer2 = std::move(buffer).Slice(2, 3);
  EXPECT_TRUE(buffer2.is_owner());
  EXPECT_FALSE(buffer.is_owner());  // NOLINT misc-use-after-move
  EXPECT_THAT(buffer2, ElementsAre("str2", "str3", "str4"));
}

TEST_F(StringsBufferTest, ShallowCopy) {
  // Create a buffer from iterators.
  size_t num_rows = 10;
  Buffer<std::string> buffer = CreateTestBuffer(num_rows);

  // Copy without offset.
  Buffer<std::string> buffer_copy1 = buffer.ShallowCopy();

  // Verify properties.
  EXPECT_FALSE(buffer_copy1.is_owner());
  EXPECT_EQ(buffer.begin(), buffer_copy1.begin());
  EXPECT_EQ(buffer.end(), buffer_copy1.end());
  EXPECT_THAT(buffer, ElementsAreArray(buffer_copy1));

  // Copy with offset.
  Buffer<std::string> buffer_copy2 = buffer.Slice(5, 5);
  EXPECT_THAT(buffer, Not(ElementsAreArray(buffer_copy2)));

  // Verify properties.
  EXPECT_TRUE(buffer_copy2.is_owner());
  EXPECT_EQ(buffer[5], buffer_copy2[0]);
}

TEST_F(StringsBufferTest, DeepCopy) {
  size_t num_rows = 5;
  Buffer<std::string> buffer = CreateTestBuffer(num_rows);

  // Deep copy full buffer.
  Buffer<std::string> buffer_copy = buffer.DeepCopy();

  // Deep copy buffer slice.
  Buffer<std::string> buffer_slice_copy = buffer.Slice(1, 3).DeepCopy();

  // Release original buffer.
  buffer = Buffer<std::string>();

  // Verify contents of copies.
  EXPECT_TRUE(buffer_copy.is_owner());
  EXPECT_THAT(buffer_copy, ElementsAre("str0", "str1", "str2", "str3", "str4"));

  EXPECT_TRUE(buffer_slice_copy.is_owner());
  EXPECT_THAT(buffer_slice_copy, ElementsAre("str1", "str2", "str3"));

  // Make a deep copy of an empty buffer.
  buffer_copy = buffer.DeepCopy();
  EXPECT_THAT(buffer_copy, IsEmpty());
}

TEST_F(StringsBufferTest, EmptySlice) {
  size_t num_rows = 10;
  Buffer<std::string> buffer = CreateTestBuffer(num_rows);

  // Empty slice without ownership transfer.
  Buffer<std::string> copy = buffer.Slice(3, 0);
  EXPECT_THAT(copy, IsEmpty());

  // Shrink buffer to size zero.
  buffer = std::move(buffer).Slice(3, 0);
  EXPECT_THAT(buffer, IsEmpty());

  // Allow empty slice of empty buffer.
  copy = buffer.Slice(0, 0);
  EXPECT_THAT(copy, IsEmpty());
}

TEST_F(StringsBufferTest, HugeString) {
  StringsBuffer::Builder builder(2);
  builder.Set(0, "small string");
  std::string huge_string;
  for (int i = 0; i < 1000; ++i) huge_string.append("huge string; ");
  builder.Set(1, huge_string);

  StringsBuffer buffer = std::move(builder).Build(2);
  EXPECT_EQ(buffer.size(), 2);
  EXPECT_EQ(buffer[0], "small string");
  EXPECT_EQ(buffer[1], huge_string);
}

TEST_F(StringsBufferTest, SupportsAbslHash) {
  StringsBuffer empty;
  std::array<absl::string_view, 5> values = {"one", "two", "three", "four",
                                             "five"};
  StringsBuffer test1 = StringsBuffer::Create(values.begin(), values.end());
  StringsBuffer test2 = StringsBuffer::Create(values.rbegin(), values.rend());
  EXPECT_TRUE(
      absl::VerifyTypeImplementsAbslHashCorrectly({empty, test1, test2}));
}

TEST_F(StringsBufferTest, Fingerprint) {
  std::array<absl::string_view, 5> values = {"one", "two", "three", "four",
                                             "five"};
  StringsBuffer test1 = StringsBuffer::Create(values.begin(), values.end());
  StringsBuffer test2 = StringsBuffer::Create(values.begin(), values.end());
  StringsBuffer test3 = StringsBuffer::Create(values.rbegin(), values.rend());
  Fingerprint f1 = FingerprintHasher("salt").Combine(test1).Finish();
  Fingerprint f2 = FingerprintHasher("salt").Combine(test2).Finish();
  Fingerprint f3 = FingerprintHasher("salt").Combine(test3).Finish();
  EXPECT_EQ(f1, f2);
  EXPECT_NE(f1, f3);
}

TEST(StringsBufferBuilder, Inserter) {
  Buffer<std::string>::Builder builder(10);
  auto inserter = builder.GetInserter(1);
  for (int i = 0; i < 4; ++i) inserter.Add(absl::StrFormat("str%d", i));
  builder.Set(0, "aba");
  auto buffer = std::move(builder).Build(inserter);

  EXPECT_THAT(buffer, ElementsAre("aba", "str0", "str1", "str2", "str3"));
}

TEST(StringsBufferBuilder, InitialCharBufSize) {
  for (size_t buf_size = 0; buf_size <= 32; buf_size += 16) {
    Buffer<std::string>::Builder builder(5, buf_size);
    auto inserter = builder.GetInserter(1);
    for (int i = 0; i < 4; ++i) inserter.Add(absl::StrFormat("str%d", i));
    auto buffer = std::move(builder).Build(inserter);
    EXPECT_THAT(buffer, ElementsAre("", "str0", "str1", "str2", "str3"));
  }
}

TEST(StringsBufferBuilder, InserterCord) {
  Buffer<std::string>::Builder builder(10);
  auto inserter = builder.GetInserter(1);
  for (int i = 0; i < 4; ++i) {
    inserter.Add(absl::Cord(absl::StrFormat("str%d", i)));
  }
  builder.Set(0, "aba");
  auto buffer = std::move(builder).Build(inserter);

  EXPECT_THAT(buffer, ElementsAre("aba", "str0", "str1", "str2", "str3"));
}

TEST(StringsBufferBuilder, Generator) {
  Buffer<std::string>::Builder builder(10);
  builder.SetNConst(0, 10, "default");
  int i = 0;
  builder.SetN(2, 3, [&]() { return absl::StrFormat("str%d", ++i); });
  auto buffer = std::move(builder).Build(6);

  EXPECT_THAT(buffer, ElementsAre("default", "default", "str1", "str2", "str3",
                                  "default"));
}

TEST(StringsBufferBuilder, RandomAccess) {
  Buffer<std::string>::Builder builder(10);
  builder.Set(4, "s1");
  builder.Set(2, "s2");
  builder.Set(1, "s3");
  builder.Set(0, "s4");
  builder.Set(3, "s5");
  builder.Set(1, "s6");
  auto buffer = std::move(builder).Build(5);

  EXPECT_THAT(buffer, ElementsAre("s4", "s6", "s2", "s5", "s1"));
}

TEST(StringsBufferBuilder, RandomAccessCord) {
  Buffer<std::string>::Builder builder(10);
  builder.Set(4, absl::Cord("s1"));
  builder.Set(2, absl::Cord("s2"));
  builder.Set(1, absl::Cord("s3"));
  builder.Set(0, absl::Cord("s4"));
  builder.Set(3, absl::Cord("s5"));
  builder.Set(1, absl::Cord("s6"));
  auto buffer = std::move(builder).Build(5);

  EXPECT_THAT(buffer, ElementsAre("s4", "s6", "s2", "s5", "s1"));
}

TEST(StringsBufferBuilder, ReshuffleBuilder) {
  auto buf = CreateBuffer<std::string>({"5v", "4ab", "3", "2", "1"});
  {  // without default value
    Buffer<std::string>::ReshuffleBuilder bldr(7, buf, std::nullopt);
    bldr.CopyValue(3, 1);
    bldr.CopyValue(1, 2);
    bldr.CopyValue(2, 0);
    bldr.CopyValueToRange(4, 7, 0);
    auto res = std::move(bldr).Build();
    EXPECT_THAT(res, ElementsAre("", "3", "5v", "4ab", "5v", "5v", "5v"));
    EXPECT_EQ(res.characters().begin(), buf.characters().begin());
  }
  {  // with empty default value
    Buffer<std::string>::ReshuffleBuilder bldr(4, buf, {true, ""});
    bldr.CopyValue(3, 1);
    bldr.CopyValue(1, 2);
    bldr.CopyValue(2, 0);
    auto res = std::move(bldr).Build();
    EXPECT_THAT(res, ElementsAre("", "3", "5v", "4ab"));
    EXPECT_EQ(res.characters().begin(), buf.characters().begin());
  }
  {  // with default value
    Buffer<std::string>::ReshuffleBuilder bldr(4, buf, {true, "0abc"});
    bldr.CopyValue(3, 1);
    bldr.CopyValue(1, 2);
    bldr.CopyValue(2, 0);
    auto res = std::move(bldr).Build();
    EXPECT_THAT(res, ElementsAre("0abc", "3", "5v", "4ab"));
  }
}

}  // namespace
}  // namespace arolla
