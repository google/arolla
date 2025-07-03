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
#include "arolla/expr/eval/expr_stack_trace.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/testing/testing.h"

namespace arolla::expr {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::WithSourceLocationAnnotation;

TEST(DetailedStackTraceTest, AddTraceWithMultipleOperations) {
  ASSERT_OK_AND_ASSIGN(auto expr_a,
                       CallOp("math.add", {Leaf("a"), Literal(1)}));
  ASSERT_OK_AND_ASSIGN(auto expr_b,
                       CallOp("math.add", {Leaf("b"), Literal(1)}));
  ASSERT_OK_AND_ASSIGN(auto expr_c,
                       CallOp("math.add", {Leaf("c"), Literal(1)}));
  ASSERT_OK_AND_ASSIGN(auto expr_d,
                       CallOp("math.add", {Leaf("d"), Literal(1)}));
  ASSERT_OK_AND_ASSIGN(auto expr_e,
                       CallOp("math.add", {Leaf("e"), Literal(1)}));

  ASSERT_OK_AND_ASSIGN(
      auto expr_a_with_location,
      WithSourceLocationAnnotation(expr_a, "aaa", "file.txt", 1, 5, "a"));
  ASSERT_OK_AND_ASSIGN(
      auto expr_b_with_location,
      WithSourceLocationAnnotation(expr_b, "bbb", "file.txt", 2, 5, "b"));
  ASSERT_OK_AND_ASSIGN(
      auto expr_c_with_location,
      WithSourceLocationAnnotation(expr_c, "ccc", "file.txt", 3, 5, "c"));
  ASSERT_OK_AND_ASSIGN(
      auto expr_d_with_location,
      WithSourceLocationAnnotation(expr_d, "ddd", "file.txt", 4, 5, "d"));

  DetailedExprStackTrace stack_trace;
  stack_trace.AddSourceLocation(
      expr_a, ReadSourceLocationAnnotation(expr_a_with_location).value());
  stack_trace.AddSourceLocation(
      expr_b, ReadSourceLocationAnnotation(expr_b_with_location).value());
  stack_trace.AddSourceLocation(
      expr_c, ReadSourceLocationAnnotation(expr_c_with_location).value());
  stack_trace.AddSourceLocation(
      expr_d, ReadSourceLocationAnnotation(expr_d_with_location).value());
  stack_trace.AddTrace(expr_b, expr_a);
  // Both expr_c and expr_d originate from expr_b.
  stack_trace.AddTrace(expr_c, expr_b);
  stack_trace.AddTrace(expr_d, expr_b);
  stack_trace.AddTrace(expr_e, expr_d);

  auto bound_stack_trace = std::move(stack_trace).Finalize()();
  bound_stack_trace->RegisterIp(11, expr_a);
  bound_stack_trace->RegisterIp(22, expr_b);
  bound_stack_trace->RegisterIp(33, expr_c);
  bound_stack_trace->RegisterIp(44, expr_d);
  bound_stack_trace->RegisterIp(55, expr_e);
  auto annotate_error = std::move(*bound_stack_trace).Finalize();
  EXPECT_THAT(annotate_error(11, absl::InvalidArgumentError("error")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "error\n"
                       "\n"
                       "file.txt:1:5, in aaa\n"
                       "a"));
  EXPECT_THAT(annotate_error(22, absl::InvalidArgumentError("error")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "error\n"
                       "\n"
                       "file.txt:2:5, in bbb\n"
                       "b\n"
                       "\n"
                       "file.txt:1:5, in aaa\n"
                       "a"));
  EXPECT_THAT(annotate_error(33, absl::InvalidArgumentError("error")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "error\n"
                       "\n"
                       "file.txt:3:5, in ccc\n"
                       "c\n"
                       "\n"
                       "file.txt:2:5, in bbb\n"
                       "b\n"
                       "\n"
                       "file.txt:1:5, in aaa\n"
                       "a"));
  EXPECT_THAT(annotate_error(44, absl::InvalidArgumentError("error")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "error\n"
                       "\n"
                       "file.txt:4:5, in ddd\n"
                       "d\n"
                       "\n"
                       "file.txt:2:5, in bbb\n"
                       "b\n"
                       "\n"
                       "file.txt:1:5, in aaa\n"
                       "a"));
  EXPECT_THAT(annotate_error(55, absl::InvalidArgumentError("error")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "error\n"
                       "\n"
                       "file.txt:4:5, in ddd\n"
                       "d\n"
                       "\n"
                       "file.txt:2:5, in bbb\n"
                       "b\n"
                       "\n"
                       "file.txt:1:5, in aaa\n"
                       "a"));
  EXPECT_THAT(annotate_error(66, absl::InvalidArgumentError("error")),
              StatusIs(absl::StatusCode::kInvalidArgument, "error"));
}

}  // namespace
}  // namespace arolla::expr
