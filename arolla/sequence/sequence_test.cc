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
#include "arolla/sequence/sequence.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;

class SequenceTest : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

TEST_F(SequenceTest, DefaultConstructor) {
  Sequence seq;
  EXPECT_EQ(seq.value_qtype(), GetNothingQType());
  EXPECT_EQ(seq.size(), 0);
  EXPECT_EQ(seq.RawData(), nullptr);
}

TEST_F(SequenceTest, MakeSize1) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 1));
  const auto seq = std::move(mutable_seq).Finish();
  EXPECT_EQ(seq.value_qtype(), GetQType<int32_t>());
  EXPECT_EQ(seq.size(), 1);
  EXPECT_NE(seq.RawData(), nullptr);
}

TEST_F(SequenceTest, RawAt) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  for (int i = 0; i < 100; i += 10) {
    EXPECT_EQ(seq.RawAt(i, sizeof(int32_t)),
              static_cast<const char*>(seq.RawData()) + i * sizeof(int32_t));
  }
}

TEST_F(SequenceTest, UnsafeSpan) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<float>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  absl::Span<const float> span = seq.UnsafeSpan<float>();
  EXPECT_EQ(span.data(), seq.RawData());
  EXPECT_EQ(span.size(), 100);
}

TEST_F(SequenceTest, GetRef) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<double>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  for (int i = 0; i < 100; i += 10) {
    auto ref = seq.GetRef(i);
    EXPECT_EQ(ref.GetType(), GetQType<double>());
    EXPECT_EQ(ref.GetRawPointer(), seq.RawAt(i, sizeof(double)));
  }
}

TEST_F(SequenceTest, subsequence) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  for (int offset = 0; offset < 100; offset += 10) {
    for (int count = 10; count <= 100; count += 30) {
      const auto subseq = seq.subsequence(offset, count);
      EXPECT_EQ(subseq.value_qtype(), GetQType<int32_t>());
      EXPECT_EQ(subseq.size(), std::min(count, 100 - offset));
      EXPECT_EQ(
          static_cast<const char*>(subseq.RawData()),
          static_cast<const char*>(seq.RawData()) + offset * sizeof(int32_t));
    }
  }
  for (int offset = 0; offset < 100; offset += 10) {
    const auto subseq = seq.subsequence(offset, 0);
    EXPECT_EQ(subseq.size(), 0);
    EXPECT_EQ(subseq.RawData(), nullptr);
    EXPECT_EQ(subseq.value_qtype(), GetQType<int32_t>());
  }
  for (int count = 0; count <= 100; count += 25) {
    const auto subseq = seq.subsequence(100, count);
    EXPECT_EQ(subseq.size(), 0);
    EXPECT_EQ(subseq.RawData(), nullptr);
    EXPECT_EQ(subseq.value_qtype(), GetQType<int32_t>());
  }
}

#ifndef NDEBUG  // Test the runtime checks in debug builds.

using SequenceDeathTest = SequenceTest;

TEST_F(SequenceDeathTest, RawAtDCheckIndexIsOutOfRange) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  EXPECT_DEATH(seq.RawAt(100, sizeof(int32_t)),
               "index is out of range: 100 >= size=100");
}

TEST_F(SequenceDeathTest, RawAtDCheckElementSizeMismatch) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  EXPECT_DEATH(seq.RawAt(0, 3), "element size mismatched: expected 4, got 3");
}

TEST_F(SequenceDeathTest, UnsafeSpanDCheckElementTypeMismatch) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  EXPECT_DEATH(seq.UnsafeSpan<float>(),
               "element type mismatched: expected int, got float");
}

TEST_F(SequenceDeathTest, GetRefDCheckIndexIsOutOfRange) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 100));
  const auto seq = std::move(mutable_seq).Finish();
  EXPECT_DEATH(seq.GetRef(100), "index is out of range: 100 >= size=100");
}

#endif  // NDEBUG

TEST_F(SequenceTest, ReprEmpty) {
  EXPECT_THAT(GenReprToken(Sequence()),
              ReprTokenEq("sequence(value_qtype=NOTHING)"));
}

TEST_F(SequenceTest, Repr) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQTypeQType(), 4));
  auto mutable_span = mutable_seq.UnsafeSpan<QTypePtr>();
  mutable_span[0] = GetQType<Unit>();
  mutable_span[1] = GetQType<bool>();
  mutable_span[2] = GetQType<int32_t>();
  mutable_span[3] = GetQType<float>();
  auto seq = std::move(mutable_seq).Finish();
  EXPECT_THAT(
      GenReprToken(seq),
      ReprTokenEq(
          "sequence(UNIT, BOOLEAN, INT32, FLOAT32, value_qtype=QTYPE)"));
}

TEST_F(SequenceTest, ReprLarge) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQTypeQType(), 11));
  for (auto& x : mutable_seq.UnsafeSpan<QTypePtr>()) {
    x = GetQType<Unit>();
  }
  auto seq = std::move(mutable_seq).Finish();
  EXPECT_THAT(
      GenReprToken(seq),
      ReprTokenEq(
          "sequence(UNIT, UNIT, UNIT, UNIT, UNIT, UNIT, UNIT, UNIT, UNIT, "
          "UNIT, ..., size=11, value_qtype=QTYPE)"));
}

TEST_F(SequenceTest, Fingerprint) {
  std::vector<Sequence> sequences;
  { sequences.push_back(Sequence()); }
  {
    ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                         MutableSequence::Make(GetQTypeQType(), 0));
    sequences.push_back(std::move(mutable_seq).Finish());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                         MutableSequence::Make(GetQType<int32_t>(), 2));
    auto mutable_span = mutable_seq.UnsafeSpan<int32_t>();
    mutable_span[0] = 0;
    mutable_span[1] = 1;
    sequences.push_back(std::move(mutable_seq).Finish());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                         MutableSequence::Make(GetQType<int32_t>(), 2));
    auto mutable_span = mutable_seq.UnsafeSpan<int32_t>();
    mutable_span[0] = 0;
    mutable_span[1] = 0;
    sequences.push_back(std::move(mutable_seq).Finish());
  }
  for (auto& s0 : sequences) {
    for (auto& s1 : sequences) {
      const auto f0 = FingerprintHasher("salt").Combine(s0).Finish();
      const auto f1 =
          FingerprintHasher("salt").Combine(/*copy*/ Sequence(s1)).Finish();
      if (&s0 == &s1) {
        EXPECT_EQ(f0, f1) << Repr(s0) << ".fingerprint != " << Repr(s1)
                          << ".fingerprint";
      } else {
        EXPECT_NE(f0, f1) << Repr(s0) << ".fingerprint != " << Repr(s1)
                          << ".fingerprint";
      }
    }
  }
}

}  // namespace
}  // namespace arolla
