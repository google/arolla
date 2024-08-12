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
#include "arolla/codegen/expr/types.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

namespace {
struct NonExistentFake {};
}  // namespace

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(NonExistentFake);
void FingerprintHasherTraits<NonExistentFake>::operator()(
    FingerprintHasher* hasher, const NonExistentFake&) const {
  hasher->Combine(7);
}

AROLLA_DECLARE_SIMPLE_QTYPE(FAKE_TEST, NonExistentFake);
AROLLA_DEFINE_SIMPLE_QTYPE(FAKE_TEST, NonExistentFake);
}  // namespace arolla

namespace arolla::codegen {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;

// More tests can be found in scalar_types_gen_test.cc.golden
TEST(CppTypeName, Sanity) {
  EXPECT_THAT(CppTypeName(GetQType<float>()), IsOkAndHolds("float"));
}

// More tests can be found in scalar_types_gen_test.cc.golden
TEST(CppLiteralRepr, Sanity) {
  EXPECT_THAT(CppLiteralRepr(TypedRef::FromValue(1)),
              IsOkAndHolds("int32_t{1}"));
}

TEST(RegisterCppType, Sanity) {
  EXPECT_THAT(
      RegisterCppType(GetQType<NonExistentFake>(), "::arolla::NonExistentFake",
                      [](TypedRef) { return "::arolla::NonExistentFake{}"; }),
      IsOk());
  EXPECT_THAT(CppTypeName(GetQType<NonExistentFake>()),
              IsOkAndHolds("::arolla::NonExistentFake"));
  EXPECT_THAT(CppLiteralRepr(TypedRef::FromValue(NonExistentFake{})),
              IsOkAndHolds("::arolla::NonExistentFake{}"));
}

}  // namespace
}  // namespace arolla::codegen
