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
#ifndef AROLLA_EXPR_EVAL_TEST_UTILS_H_
#define AROLLA_EXPR_EVAL_TEST_UTILS_H_

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/eval/dynamic_compiled_expr.h"
#include "arolla/qexpr/evaluation_engine.h"

namespace arolla::expr {

template <typename... Ts>
testing::Matcher<std::unique_ptr<BoundExpr>> InitOperationsAre(Ts... ops) {
  return testing::Pointer(
      testing::WhenDynamicCastTo<const eval_internal::DynamicBoundExpr*>(
          testing::Property(
              &eval_internal::DynamicBoundExpr::init_op_descriptions,
              testing::ElementsAre(ops...))));
}

template <typename... Ts>
testing::Matcher<std::unique_ptr<BoundExpr>> EvalOperationsAre(Ts... ops) {
  return testing::Pointer(
      testing::WhenDynamicCastTo<const eval_internal::DynamicBoundExpr*>(
          testing::Property(
              &eval_internal::DynamicBoundExpr::eval_op_descriptions,
              testing::ElementsAre(ops...))));
}

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EVAL_TEST_UTILS_H_
