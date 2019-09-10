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
#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/codegen/expr/types.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/testing/status_matchers_backport.h"

namespace {

using ::arolla::testing::IsOkAndHolds;

TEST(DictLiteralTest, Sanity) {
  EXPECT_THAT(
      arolla::codegen::CppTypeName(arolla::GetKeyToRowDictQType<int32_t>()),
      IsOkAndHolds("::arolla::KeyToRowDict<int32_t>"));
  EXPECT_THAT(arolla::codegen::CppLiteralRepr(arolla::TypedRef::FromValue(
                  ::arolla::KeyToRowDict<int32_t>())),
              IsOkAndHolds("::arolla::KeyToRowDict<int32_t>{}"));
  EXPECT_THAT(
      arolla::codegen::CppLiteralRepr(arolla::TypedRef::FromValue(
          arolla::KeyToRowDict<int32_t>{{{5, 2}, {2, 3}}})),
      IsOkAndHolds("::arolla::KeyToRowDict<int32_t>{{int32_t{2},int64_t{3}},{"
                   "int32_t{5},int64_t{2}},}"));
}

}  // namespace
