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
#include "arolla/sequence/sequence_qtype.h"

#include <cstdint>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace arolla {
namespace {

using ::arolla::testing::ReprTokenEq;

TEST(SequenceQTypeTest, Basics) {
  const auto* qtype = GetSequenceQType<QTypePtr>();
  EXPECT_EQ(qtype->name(), "SEQUENCE[QTYPE]");
  EXPECT_EQ(qtype->type_info(), typeid(Sequence));
  EXPECT_EQ(qtype->type_layout().AllocSize(), sizeof(Sequence));
  EXPECT_EQ(qtype->type_layout().AllocAlignment().value, alignof(Sequence));
  EXPECT_TRUE(qtype->type_fields().empty());
  EXPECT_EQ(qtype->value_qtype(), GetQTypeQType());
  EXPECT_EQ(qtype->qtype_specialization_key(), "::arolla::SequenceQType");
}

TEST(SequenceQTypeTest, IsSequenceQType) {
  EXPECT_TRUE(IsSequenceQType(GetSequenceQType<QTypePtr>()));
  EXPECT_TRUE(IsSequenceQType(GetSequenceQType<int32_t>()));
  EXPECT_TRUE(IsSequenceQType(GetSequenceQType<float>()));
  EXPECT_FALSE(IsSequenceQType(GetQTypeQType()));
  EXPECT_FALSE(IsSequenceQType(GetQType<int32_t>()));
  EXPECT_FALSE(IsSequenceQType(GetQType<float>()));
}

TEST(SequenceQTypeTest, TypedValue) {
  ASSERT_OK_AND_ASSIGN(auto mutable_seq,
                       MutableSequence::Make(GetQType<int32_t>(), 3));
  auto mutable_span = mutable_seq.UnsafeSpan<int32_t>();
  mutable_span[0] = 1;
  mutable_span[1] = 2;
  mutable_span[2] = 3;
  ASSERT_OK_AND_ASSIGN(auto typed_value, TypedValue::FromValueWithQType(
                                             std::move(mutable_seq).Finish(),
                                             GetSequenceQType<int32_t>()));
  EXPECT_EQ(typed_value.GetType()->name(), "SEQUENCE[INT32]");
  EXPECT_THAT(typed_value.GenReprToken(),
              ReprTokenEq("sequence(1, 2, 3, value_qtype=INT32)"));
}

}  // namespace
}  // namespace arolla
