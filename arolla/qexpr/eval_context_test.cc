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
#include "arolla/qexpr/eval_context.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl//status/status.h"
#include "absl//status/status_matchers.h"
#include "absl//status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Not;

TEST(EvalContextTest, OperatorExample) {
  class AddOperator {
   public:
    AddOperator(FrameLayout::Slot<double> op1_slot,
                FrameLayout::Slot<double> op2_slot,
                FrameLayout::Slot<double> result_slot)
        : op1_slot_(op1_slot), op2_slot_(op2_slot), result_slot_(result_slot) {}

    void operator()(FramePtr frame) const {
      double op1 = frame.Get(op1_slot_);
      double op2 = frame.Get(op2_slot_);
      frame.Set(result_slot_, op1 + op2);
    }

   private:
    FrameLayout::Slot<double> op1_slot_;
    FrameLayout::Slot<double> op2_slot_;
    FrameLayout::Slot<double> result_slot_;
  };

  FrameLayout::Builder bldr;
  auto op1_slot = bldr.AddSlot<double>();
  auto op2_slot = bldr.AddSlot<double>();
  auto tmp_slot = bldr.AddSlot<double>();
  AddOperator add_op1(op1_slot, op2_slot, tmp_slot);
  auto op3_slot = bldr.AddSlot<double>();
  auto result_slot = bldr.AddSlot<double>();
  AddOperator add_op2(tmp_slot, op3_slot, result_slot);
  FrameLayout layout = std::move(bldr).Build();

  MemoryAllocation alloc(&layout);
  alloc.frame().Set(op1_slot, 1.0);
  alloc.frame().Set(op2_slot, 2.0);
  alloc.frame().Set(op3_slot, 3.0);
  add_op1(alloc.frame());
  add_op2(alloc.frame());

  EXPECT_EQ(6.0, alloc.frame().Get(result_slot));
}

TEST(EvalContextTest, SetStatus) {
  EvaluationContext ctx;
  absl::Status error = absl::InvalidArgumentError(
      "error message too long for a small string optimization");
  const char* message_ptr = error.message().data();
  ctx.set_status(std::move(error));
  EXPECT_THAT(ctx.status(), Not(IsOk()));
  // Error message was always moved and never copied.
  EXPECT_THAT(ctx.status().message().data(), Eq(message_ptr));
}

TEST(EvalContextTest, SetStatusInAssignOrReturn) {
  const char* message_ptr;
  auto fn = [&](EvaluationContext* ctx) {
    absl::Status error = absl::InvalidArgumentError(
        "error message too long for a small string optimization");
    message_ptr = error.message().data();
    absl::StatusOr<float> value_or = std::move(error);
    ASSIGN_OR_RETURN(float f32, std::move(value_or),
                     ctx->set_status(std::move(_)));
    return void(f32);
  };
  EvaluationContext ctx;
  fn(&ctx);
  EXPECT_THAT(ctx.status(), Not(IsOk()));
  // Error message was always moved and never copied.
  EXPECT_THAT(ctx.status().message().data(), Eq(message_ptr));
}

TEST(EvalContextTest, SetStatusInReturnIfError) {
  const char* message_ptr;
  auto fn = [&](EvaluationContext* ctx) {
    absl::Status error = absl::InvalidArgumentError(
        "error message too long for a small string optimization");
    message_ptr = error.message().data();
    RETURN_IF_ERROR(std::move(error)).With(ctx->set_status());
  };
  EvaluationContext ctx;
  fn(&ctx);
  EXPECT_THAT(ctx.status(), Not(IsOk()));
  // Error message was always moved and never copied.
  EXPECT_THAT(ctx.status().message().data(), Eq(message_ptr));
}

TEST(EvalContextTest, Status) {
  EvaluationContext ctx;
  EXPECT_THAT(ctx.status(), IsOk());
  EXPECT_THAT(ctx.signal_received(), IsFalse());

  // Setting OkStatus does not set the signal flag.
  ctx.set_status(absl::OkStatus());
  EXPECT_THAT(ctx.signal_received(), IsFalse());

  // Setting not-ok status sets the signal flag.
  ctx.set_status(absl::InvalidArgumentError("foo"));
  EXPECT_THAT(ctx.signal_received(), IsTrue());
  EXPECT_THAT(ctx.status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "foo"));

  // Setting back to OkStatus does not reset the signal flag.
  ctx.set_status(absl::OkStatus());
  EXPECT_THAT(ctx.signal_received(), IsTrue());
  EXPECT_THAT(ctx.status(), IsOk());

  // ResetSignals clears the error.
  ctx.set_status(absl::InvalidArgumentError("foo"));
  ctx.ResetSignals();
  EXPECT_THAT(ctx.signal_received(), IsFalse());
  EXPECT_THAT(ctx.status(), IsOk());
}

TEST(EvalContextTest, Jump) {
  EvaluationContext ctx;
  EXPECT_THAT(ctx.requested_jump(), Eq(0));
  EXPECT_THAT(ctx.signal_received(), IsFalse());

  // Requesting a jump sets the signal flag.
  ctx.set_requested_jump(-57);
  EXPECT_THAT(ctx.signal_received(), IsTrue());
  EXPECT_THAT(ctx.requested_jump(), Eq(-57));

  // ResetSignals clears the requested jump.
  ctx.ResetSignals();
  EXPECT_THAT(ctx.signal_received(), IsFalse());
  EXPECT_THAT(ctx.requested_jump(), Eq(0));
}

TEST(EvalContextTest, CheckInterrupt) {
  class CancelCheck final : public EvaluationContext::CancellationChecker {
    absl::Status SoftCheck() final { return absl::OkStatus(); }
    absl::Status Check() final { return absl::OkStatus(); }
  };
  CancelCheck cancel_check;
  EvaluationContext ctx(EvaluationContext::Options{
      .cancellation_checker = &cancel_check,
  });
  EXPECT_EQ(ctx.cancellation_checker(), &cancel_check);
  EXPECT_EQ(ctx.options().cancellation_checker, &cancel_check);
}

}  // namespace
}  // namespace arolla
