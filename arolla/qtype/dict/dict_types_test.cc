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
#include "arolla/qtype/dict/dict_types.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Ne;
using ::testing::Property;

TEST(DictTypes, GetKeyToRowDictQType) {
  // QType must be used in advance to make GetMappingQType(key) work.
  GetKeyToRowDictQType<int64_t>();
  EXPECT_THAT(GetKeyToRowDictQType<int64_t>()->value_qtype(),
              Eq(GetQType<int64_t>()));
  EXPECT_THAT(GetKeyToRowDictQType(GetQType<int64_t>()),
              IsOkAndHolds(GetQType<KeyToRowDict<int64_t>>()));
  EXPECT_THAT(GetKeyToRowDictQType(GetQType<int64_t>()),
              IsOkAndHolds(GetKeyToRowDictQType<int64_t>()));
  EXPECT_THAT(GetKeyToRowDictQType(GetQType<KeyToRowDict<int64_t>>()),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("no dict with DICT_INT64 keys found")));
}

TEST(DictTypes, GetDictQType) {
  // QType must be used in advance in order to register them in mappings.
  GetKeyToRowDictQType<int64_t>();
  GetDenseArrayQType<float>();
  GetDenseArrayQType<double>();

  ASSERT_OK_AND_ASSIGN(QTypePtr int_to_float_dict,
                       GetDictQType(GetQType<int64_t>(), GetQType<float>()));

  EXPECT_THAT(int_to_float_dict->name(), Eq("Dict<INT64,FLOAT32>"));
  EXPECT_THAT(GetDictKeyQTypeOrNull(int_to_float_dict),
              Eq(GetQType<int64_t>()));
  EXPECT_THAT(GetDictValueQTypeOrNull(int_to_float_dict),
              Eq(GetQType<float>()));
  EXPECT_THAT(
      int_to_float_dict->type_fields(),
      ElementsAre(
          Property(&TypedSlot::GetType, Eq(GetKeyToRowDictQType<int64_t>())),
          Property(&TypedSlot::GetType, Eq(GetDenseArrayQType<float>()))));

  EXPECT_THAT(GetDictQType(GetQType<int64_t>(), GetQType<float>()),
              IsOkAndHolds(Eq(int_to_float_dict)));
  EXPECT_THAT(GetDictQType(GetQType<int64_t>(), GetQType<double>()),
              IsOkAndHolds(Ne(int_to_float_dict)));
}

TEST(DictTypes, IsDictQType) {
  // QType must be used in advance in order to register them in mappings.
  GetKeyToRowDictQType<int64_t>();
  GetDenseArrayQType<float>();
  GetDenseArrayQType<Unit>();

  {
    ASSERT_OK_AND_ASSIGN(QTypePtr int_to_float_dict,
                         GetDictQType(GetQType<int64_t>(), GetQType<float>()));
    ASSERT_TRUE(IsDictQType(int_to_float_dict));
  }
  {
    ASSERT_OK_AND_ASSIGN(QTypePtr int_to_unit_dict,
                         GetDictQType(GetQType<int64_t>(), GetQType<Unit>()));
    ASSERT_TRUE(IsDictQType(int_to_unit_dict));
  }
  {
    // Can't create a dict from UNIT -> FLOAT (and it can't be registered).
    EXPECT_THAT(GetDictQType(GetQType<Unit>(), GetQType<float>()),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("no dict with UNIT keys found")));
  }
  {
    // Can't create a dict from FLOAT -> FLOAT (and it can't be registered).
    EXPECT_THAT(GetDictQType(GetQType<float>(), GetQType<float>()),
                StatusIs(absl::StatusCode::kNotFound,
                         HasSubstr("no dict with FLOAT32 keys found")));
  }
}

TEST(DictTypes, ReprTraits) {
  EXPECT_EQ(Repr(KeyToRowDict<float>{}), "dict{}");
  EXPECT_EQ(Repr(KeyToRowDict<float>{{{0.5, 1}}}), "dict{0.5:int64{1},}");
  EXPECT_EQ(Repr(KeyToRowDict<float>{{{0.5, 1}, {2.5, 3}}}),
            "dict{0.5:int64{1},2.5:int64{3},}");
  EXPECT_EQ(Repr(KeyToRowDict<Bytes>{{{Bytes("key"), 2}}}),
            "dict{b'key':int64{2},}");
}

}  // namespace
}  // namespace arolla
