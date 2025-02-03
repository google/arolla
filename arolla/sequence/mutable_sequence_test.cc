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
#include "arolla/sequence/mutable_sequence.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace {

TEST(MutableSequenceTest, DefaultConstructor) {
  MutableSequence seq;
  EXPECT_EQ(seq.value_qtype(), GetNothingQType());
  EXPECT_EQ(seq.size(), 0);
  EXPECT_EQ(seq.RawData(), nullptr);
}

TEST(MutableSequenceTest, MakeEmpty) {
  ASSERT_OK_AND_ASSIGN(auto seq, MutableSequence::Make(GetQTypeQType(), 0));
  EXPECT_EQ(seq.value_qtype(), GetQTypeQType());
  EXPECT_EQ(seq.size(), 0);
  EXPECT_EQ(seq.RawData(), nullptr);
}

TEST(MutableSequenceTest, MakeSize1) {
  ASSERT_OK_AND_ASSIGN(auto seq, MutableSequence::Make(GetQType<int32_t>(), 1));
  EXPECT_EQ(seq.value_qtype(), GetQType<int32_t>());
  EXPECT_EQ(seq.size(), 1);
  EXPECT_NE(seq.RawData(), nullptr);
}

TEST(MutableSequenceTest, RawAt) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  for (int i = 0; i < 100; i += 10) {
    EXPECT_EQ(seq.RawAt(i, sizeof(int32_t)),
              static_cast<char*>(seq.RawData()) + i * sizeof(int32_t));
  }
}

TEST(MutableSequenceTest, UnsafeSpan) {
  ASSERT_OK_AND_ASSIGN(auto seq, MutableSequence::Make(GetQType<float>(), 100));
  absl::Span<float> span = seq.UnsafeSpan<float>();
  EXPECT_EQ(span.data(), seq.RawData());
  EXPECT_EQ(span.size(), 100);
}

TEST(MutableSequenceTest, GetRef) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<double>(), 100));
  for (int i = 0; i < 100; i += 10) {
    auto ref = seq.GetRef(i);
    EXPECT_EQ(ref.GetType(), GetQType<double>());
    EXPECT_EQ(ref.GetRawPointer(), seq.RawAt(i, sizeof(double)));
  }
}

TEST(MutableSequenceTest, SetRef) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<double>(), 100));
  for (int i = 0; i < 100; i += 10) {
    auto val = TypedValue::FromValue<double>(i);
    seq.UnsafeSetRef(i, val.AsRef());
  }
  for (int i = 0; i < 100; i += 10) {
    EXPECT_EQ(seq.UnsafeSpan<double>()[i], i);
  }
}

TEST(MutableSequenceTest, Finish) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  auto span = seq.UnsafeSpan<int32_t>();
  for (size_t i = 0; i < span.size(); ++i) {
    span[i] = i;
  }
  const auto immutable_seq = std::move(seq).Finish();
  EXPECT_EQ(immutable_seq.value_qtype(), GetQType<int32_t>());
  EXPECT_EQ(immutable_seq.size(), 100);
  EXPECT_NE(immutable_seq.RawData(), nullptr);
  const auto immutable_span = immutable_seq.UnsafeSpan<int32_t>();
  for (size_t i = 0; i < 100; i += 10) {
    EXPECT_EQ(immutable_span[i], i);
  }
}

// A helper type to test constructor/destructor calls.
struct CountedType {
  static int counter;

  CountedType() { counter += 1; }
  ~CountedType() { counter -= 1; }
  CountedType(const CountedType&) { counter += 1; }
  CountedType& operator=(const CountedType&) {
    counter += 1;
    return *this;
  }
};

int CountedType::counter = 0;

}  // namespace

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(CountedType);
AROLLA_DECLARE_SIMPLE_QTYPE(COUNTED, CountedType);
AROLLA_DEFINE_SIMPLE_QTYPE(COUNTED, CountedType);

void FingerprintHasherTraits<CountedType>::operator()(
    FingerprintHasher* hasher, const CountedType&) const {
  hasher->Combine(absl::string_view("counted_value"));
}

namespace {

TEST(MutableSequenceTest, ConstructorDestructor) {
  {
    ASSERT_OK_AND_ASSIGN(auto seq,
                         MutableSequence::Make(GetQType<CountedType>(), 100));
    EXPECT_EQ(CountedType::counter, 100);
  }
  EXPECT_EQ(CountedType::counter, 0);
}

TEST(MutableSequenceTest, ConstructorFinishDestructor) {
  Sequence immutable_seq;
  {
    ASSERT_OK_AND_ASSIGN(auto seq,
                         MutableSequence::Make(GetQType<CountedType>(), 100));
    EXPECT_EQ(CountedType::counter, 100);
    auto immutable_seq = std::move(seq).Finish();
    EXPECT_EQ(CountedType::counter, 100);
  }
  EXPECT_EQ(CountedType::counter, 0);
}

#ifndef NDEBUG  // Test the runtime checks in debug builds.

TEST(MutableSequenceDeathTest, RawAtDCheckIndexIsOutOfRange) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  EXPECT_DEATH(seq.RawAt(100, sizeof(int32_t)),
               "index is out of range: 100 >= size=100");
}

TEST(MutableSequenceDeathTest, RawAtDCheckElementSizeMismatch) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  EXPECT_DEATH(seq.RawAt(0, 3), "element size mismatched: expected 4, got 3");
}

TEST(MutableSequenceDeathTest, UnsafeSpanDCheckElementTypeMismatch) {
  ASSERT_OK_AND_ASSIGN(auto seq, MutableSequence::Make(GetQType<int>(), 100));
  EXPECT_DEATH(seq.UnsafeSpan<float>(),
               "element type mismatched: expected int, got float");
}

TEST(MutableSequenceDeathTest, GetRefDCheckIndexIsOutOfRange) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  EXPECT_DEATH(seq.GetRef(100), "index is out of range: 100 >= size=100");
}

TEST(MutableSequenceDeathTest, UnsafeSetRefDCheckIndexIsOutOfRange) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  EXPECT_DEATH(seq.UnsafeSetRef(100, TypedRef::FromValue<int32_t>(0)),
               "index is out of range: 100 >= size=100");
}

TEST(MutableSequenceDeathTest, UnsafeSetRefDCheckElementQTypeMismatch) {
  ASSERT_OK_AND_ASSIGN(auto seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  EXPECT_DEATH(seq.UnsafeSetRef(0, TypedRef::FromValue<float>(0.)),
               "element qtype mismatched: expected INT32, got FLOAT32");
}

#endif  // NDEBUG

}  // namespace
}  // namespace arolla
