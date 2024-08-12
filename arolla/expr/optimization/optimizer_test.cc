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
#include "arolla/expr/optimization/optimizer.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::WithQTypeAnnotation;

class Optimizer : public ::testing::Test {
  void SetUp() override { InitArolla(); }
};

// Bad optimizations: float -> int32, double -> no type
absl::StatusOr<PeepholeOptimizationPack> ChangeTypeOptimizations() {
  PeepholeOptimizationPack result;
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     WithQTypeAnnotation(Placeholder("x"), GetQType<float>()));
    ASSIGN_OR_RETURN(ExprNodePtr to, WithQTypeAnnotation(Placeholder("x"),
                                                         GetQType<int32_t>()));
    ASSIGN_OR_RETURN(result.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     WithQTypeAnnotation(Placeholder("x"), GetQType<double>()));
    ExprNodePtr to = Placeholder("x");
    ASSIGN_OR_RETURN(result.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  return result;
}

TEST_F(Optimizer, TypeChangesAreNotAllowed) {
  ASSERT_OK_AND_ASSIGN(auto peephole_optimizer,
                       CreatePeepholeOptimizer({ChangeTypeOptimizations}));
  auto optimizer = MakeOptimizer(std::move(peephole_optimizer));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr float_x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<float>()));
  EXPECT_THAT(
      optimizer(float_x),
      StatusIs(absl::StatusCode::kInternal,
               "expression M.annotation.qtype(L.x, FLOAT32) was optimized into "
               "M.annotation.qtype(L.x, INT32), which changed its output type "
               "from FLOAT32 to INT32; this indicates incorrect optimization"));

  ASSERT_OK_AND_ASSIGN(ExprNodePtr double_x,
                       WithQTypeAnnotation(Leaf("x"), GetQType<double>()));
  EXPECT_THAT(
      optimizer(double_x),
      StatusIs(absl::StatusCode::kInternal,
               "expression M.annotation.qtype(L.x, FLOAT64) was optimized into "
               "L.x, which changed its output type from FLOAT64 to NULL; this "
               "indicates incorrect optimization"));
}

}  // namespace
}  // namespace arolla::expr
