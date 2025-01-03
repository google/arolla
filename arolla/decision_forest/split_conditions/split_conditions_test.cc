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
#include <cmath>
#include <cstdint>
#include <utility>

#include "gtest/gtest.h"
#include "arolla/decision_forest/split_condition.h"
#include "arolla/decision_forest/split_conditions/interval_split_condition.h"
#include "arolla/decision_forest/split_conditions/set_of_values_split_condition.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"

namespace arolla {
namespace {

TEST(SplitConditionTest, IntervalSplitCondition) {
  const IntervalSplitCondition interval_split(0, 2, 3);
  const SplitCondition* split = &interval_split;

  EXPECT_EQ(split->ToString(), "#0 in range [2.000000 3.000000]");
  EXPECT_EQ(split->RemapInputs({{0, 1}})->ToString(),
            "#1 in range [2.000000 3.000000]");

  EXPECT_EQ(split->GetInputSignatures().size(), 1);
  EXPECT_EQ(split->GetInputSignatures()[0].id, 0);
  EXPECT_EQ(split->GetInputSignatures()[0].type, GetOptionalQType<float>());

  EXPECT_EQ(interval_split.EvaluateCondition(2), true);
  EXPECT_EQ(interval_split.EvaluateCondition(2.5), true);
  EXPECT_EQ(interval_split.EvaluateCondition(3.5), false);
  EXPECT_EQ(interval_split.EvaluateCondition({}), false);
  EXPECT_EQ(interval_split.EvaluateCondition(NAN), false);
}

TEST(SplitConditionTest, SetOfValuesSplitConditionInt64) {
  const SetOfValuesSplitCondition<int64_t> set_of_values(1, {2, 4, 3}, true);
  const SplitCondition* split = &set_of_values;

  EXPECT_TRUE(set_of_values.GetDefaultResultForMissedInput());
  EXPECT_EQ(split->ToString(), "#1 in set [2, 3, 4] or missed");
  EXPECT_EQ(split->RemapInputs({{1, 0}})->ToString(),
            "#0 in set [2, 3, 4] or missed");

  EXPECT_EQ(split->GetInputSignatures().size(), 1);
  EXPECT_EQ(split->GetInputSignatures()[0].id, 1);
  EXPECT_EQ(split->GetInputSignatures()[0].type, GetOptionalQType<int64_t>());

  EXPECT_EQ(set_of_values.EvaluateCondition(2), true);
  EXPECT_EQ(set_of_values.EvaluateCondition(1), false);
  EXPECT_EQ(set_of_values.EvaluateCondition({}), true);

  const SetOfValuesSplitCondition<int64_t> set_of_values2(1, {2, 4, 3}, false);
  EXPECT_EQ(set_of_values2.EvaluateCondition(2), true);
  EXPECT_EQ(set_of_values2.EvaluateCondition(1), false);
  EXPECT_EQ(set_of_values2.EvaluateCondition({}), false);
}

TEST(SplitConditionTest, SetOfValuesSplitConditionBytes) {
  const SetOfValuesSplitCondition<Bytes> set_of_values(
      1, {Bytes("A"), Bytes("C"), Bytes("B")}, true);
  const SplitCondition* split = &set_of_values;

  EXPECT_TRUE(set_of_values.GetDefaultResultForMissedInput());
  EXPECT_EQ(split->ToString(), "#1 in set [b'A', b'B', b'C'] or missed");

  EXPECT_EQ(split->GetInputSignatures().size(), 1);
  EXPECT_EQ(split->GetInputSignatures()[0].id, 1);
  EXPECT_EQ(split->GetInputSignatures()[0].type, GetOptionalQType<Bytes>());

  EXPECT_EQ(set_of_values.EvaluateCondition(Bytes("B")), true);
  EXPECT_EQ(set_of_values.EvaluateCondition(Bytes("D")), false);
  EXPECT_EQ(set_of_values.EvaluateCondition({}), true);

  const SetOfValuesSplitCondition<Bytes> set_of_values2(
      1, {Bytes("A"), Bytes("C"), Bytes("B")}, false);
  EXPECT_EQ(set_of_values2.EvaluateCondition(Bytes("B")), true);
  EXPECT_EQ(set_of_values2.EvaluateCondition(Bytes("D")), false);
  EXPECT_EQ(set_of_values2.EvaluateCondition({}), false);
}

TEST(SplitConditionTest, Comparison) {
  auto int1 = IntervalSplitCondition(0, 2, 3);
  auto int2 = IntervalSplitCondition(0, 2, 3);
  auto int3 = IntervalSplitCondition(1, 2, 3);
  auto int4 = IntervalSplitCondition(0, 2, 4);
  auto int5 = IntervalSplitCondition(0, 1.9999999, 3);
  EXPECT_EQ(int1, int1);
  EXPECT_EQ(int1, int2);
  EXPECT_NE(int1, int3);
  EXPECT_NE(int1, int4);
  EXPECT_NE(int1, int5);

  auto set1 = SetOfValuesSplitCondition<int64_t>(1, {2, 3}, true);
  auto set2 = SetOfValuesSplitCondition<int64_t>(1, {2, 3}, true);
  auto set3 = SetOfValuesSplitCondition<int64_t>(1, {2, 3}, false);
  auto set4 = SetOfValuesSplitCondition<int32_t>(1, {2, 3}, true);
  auto set5 = SetOfValuesSplitCondition<int64_t>(1, {3, 2}, true);
  auto set6 = SetOfValuesSplitCondition<int64_t>(1, {2}, true);
  auto set7 = SetOfValuesSplitCondition<int64_t>(0, {2, 3}, true);
  EXPECT_EQ(set1, set2);
  EXPECT_NE(set1, set3);
  EXPECT_NE(set1, set4);
  EXPECT_EQ(set1, set5);
  EXPECT_NE(set1, set6);
  EXPECT_NE(set1, set7);

  EXPECT_NE(int3, set4);
}

TEST(SplitConditionTest, CombineToFingerprintHasher) {
  auto make_fgpt = [](const SplitCondition& split_condition) {
    FingerprintHasher hasher("salt");
    split_condition.CombineToFingerprintHasher(&hasher);
    return std::move(hasher).Finish();
  };
  auto int1 = make_fgpt(IntervalSplitCondition(0, 2, 3));
  auto int2 = make_fgpt(IntervalSplitCondition(0, 2, 3));
  auto int3 = make_fgpt(IntervalSplitCondition(1, 2, 3));
  EXPECT_EQ(int1, int2);
  EXPECT_NE(int1, int3);

  auto set1 = make_fgpt(SetOfValuesSplitCondition<int64_t>(1, {2, 3}, true));
  auto set2 = make_fgpt(SetOfValuesSplitCondition<int64_t>(1, {2, 3}, true));
  auto set3 = make_fgpt(SetOfValuesSplitCondition<int64_t>(1, {2, 3}, false));
  auto set4 = make_fgpt(SetOfValuesSplitCondition<int32_t>(1, {2, 3}, true));
  EXPECT_EQ(set1, set2);
  EXPECT_NE(set1, set3);
  EXPECT_NE(set1, set4);

  EXPECT_NE(int3, set4);
}

TEST(SplitConditionTest, StableHash) {
  auto int1 = IntervalSplitCondition(0, 2, 3).StableHash();
  auto int2 = IntervalSplitCondition(0, 2, 3).StableHash();
  auto int3 = IntervalSplitCondition(1, 2, 3).StableHash();
  EXPECT_EQ(int1, int2);
  EXPECT_NE(int1, int3);

  auto set1 = SetOfValuesSplitCondition<int64_t>(1, {2, 3}, true).StableHash();
  auto set2 = SetOfValuesSplitCondition<int64_t>(1, {2, 3}, true).StableHash();
  auto set3 = SetOfValuesSplitCondition<int64_t>(1, {2, 3}, false).StableHash();
  auto set4 = SetOfValuesSplitCondition<int32_t>(1, {2, 3}, true).StableHash();
  EXPECT_EQ(set1, set2);
  EXPECT_NE(set1, set3);
  EXPECT_NE(set1, set4);

  EXPECT_NE(int3, set4);
}

}  // namespace
}  // namespace arolla
