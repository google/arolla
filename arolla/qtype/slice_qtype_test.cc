// Copyright 2025 Google LLC
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
#include "arolla/qtype/slice_qtype.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/bytes.h"

namespace arolla::testing {
namespace {

TEST(SliceQType, MakeSliceQType) {
  auto start = GetQType<int32_t>();
  auto stop = GetQType<double>();
  auto step = GetQType<Bytes>();
  auto qtype = MakeSliceQType(start, stop, step);
  EXPECT_EQ(qtype->name(), "slice<INT32,FLOAT64,BYTES>");

  auto derived_qtype_interface =
      dynamic_cast<const DerivedQTypeInterface*>(qtype);
  ASSERT_NE(derived_qtype_interface, nullptr);
  auto tuple_qtype = MakeTupleQType({start, stop, step});
  EXPECT_EQ(derived_qtype_interface->GetBaseQType(), tuple_qtype);

  {
    auto qtype2 = MakeSliceQType(start, stop, step);
    EXPECT_EQ(qtype, qtype2);
  }
  {
    auto qtype2 = MakeSliceQType(start, stop, start);
    EXPECT_EQ(qtype2->name(), "slice<INT32,FLOAT64,INT32>");
    EXPECT_NE(qtype, qtype2);
  }
}

TEST(SliceQType, IsSliceQType) {
  auto start = GetQType<int32_t>();
  auto stop = GetQType<double>();
  auto step = GetQType<Bytes>();
  auto tuple_qtype = MakeTupleQType({start, stop, step});
  EXPECT_FALSE(IsSliceQType(tuple_qtype));
  auto qtype = MakeSliceQType(start, stop, step);
  EXPECT_TRUE(IsSliceQType(qtype));
}

}  // namespace
}  // namespace arolla::testing
