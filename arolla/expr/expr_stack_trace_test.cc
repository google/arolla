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
#include "arolla/expr/expr_stack_trace.h"

#include "gtest/gtest.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"

namespace arolla::expr {
namespace {

class ExprStackTraceTest : public ::testing::Test {
 protected:
  void SetUp() override { InitArolla(); }
};

TEST_F(ExprStackTraceTest, ExprStackTraceSafeReturnsOnUnregisteredFingerprint) {
  DetailedExprStackTrace stack_trace;
  EXPECT_EQ(stack_trace.FullTrace(Fingerprint{0}), "");
}

}  // namespace
}  // namespace arolla::expr
