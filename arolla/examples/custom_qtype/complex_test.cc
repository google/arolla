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
#include "arolla/examples/custom_qtype/complex.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"

namespace my_namespace {
namespace {

using ::testing::Eq;
using ::testing::Ne;

class ComplexTest : public ::testing::Test {
  void SetUp() override { arolla::InitArolla(); }
};

TEST_F(ComplexTest, GetQType) {
  EXPECT_THAT(arolla::GetQType<MyComplex>()->name(), Eq("MY_COMPLEX"));
}

TEST_F(ComplexTest, Fingerprint) {
  MyComplex c{.re = 5.7, .im = 0.7};

  auto c_fingerprint = arolla::FingerprintHasher("").Combine(c).Finish();
  EXPECT_THAT(arolla::FingerprintHasher("").Combine(c).Finish(),
              Eq(c_fingerprint));
  EXPECT_THAT(arolla::FingerprintHasher("")
                  .Combine(MyComplex{.re = 0.7, .im = 5.7})
                  .Finish(),
              Ne(c_fingerprint));
}

TEST_F(ComplexTest, Repr) {
  EXPECT_THAT(arolla::Repr(MyComplex{}), Eq("0 + 0i"));
  EXPECT_THAT(arolla::Repr(MyComplex{.re = 5.7, .im = 0.7}), Eq("5.7 + 0.7i"));
}

}  // namespace
}  // namespace my_namespace
