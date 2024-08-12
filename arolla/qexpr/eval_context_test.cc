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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
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

  RootEvaluationContext ctx(&layout);
  ctx.Set(op1_slot, 1.0);
  ctx.Set(op2_slot, 2.0);
  ctx.Set(op3_slot, 3.0);
  add_op1(ctx.frame());
  add_op2(ctx.frame());

  EXPECT_EQ(6.0, ctx.Get(result_slot));
}

TEST(EvalContextTest, SetByRValue) {
  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<std::unique_ptr<int>>();
  FrameLayout layout = std::move(bldr).Build();

  RootEvaluationContext ctx(&layout);
  ctx.Set(slot, std::make_unique<int>(5));
  EXPECT_EQ(5, *ctx.Get(slot));

  std::unique_ptr<int> ptr(new int(6));
  ctx.Set(slot, std::move(ptr));
  EXPECT_EQ(6, *ctx.Get(slot));
}

TEST(EvalContextTest, ImplicitTypeCastingOnSet) {
  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<int8_t>();
  FrameLayout layout = std::move(bldr).Build();

  RootEvaluationContext ctx(&layout);
  ctx.Set(slot, 5);

  EXPECT_EQ(5, ctx.Get(slot));
}

TEST(EvalContextTest, RegisterUnsafeSlot) {
  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<int32_t>();
  auto slot_1byte =
      FrameLayout::Slot<int8_t>::UnsafeSlotFromOffset(slot.byte_offset());
  auto slot_2bytes =
      FrameLayout::Slot<int16_t>::UnsafeSlotFromOffset(slot.byte_offset() + 2);
  ASSERT_OK(bldr.RegisterUnsafeSlot(slot_1byte));
  ASSERT_OK(bldr.RegisterUnsafeSlot(slot_2bytes));
  FrameLayout layout = std::move(bldr).Build();

  RootEvaluationContext ctx(&layout);

  ctx.Set(slot_1byte, 5);
  EXPECT_EQ(5, ctx.Get(slot_1byte));

  ctx.Set(slot_2bytes, 1024);
  EXPECT_EQ(1024, ctx.Get(slot_2bytes));
}

TEST(EvalContextTest, SetByConstReference) {
  FrameLayout::Builder bldr;
  auto slot = bldr.AddSlot<int32_t>();
  FrameLayout layout = std::move(bldr).Build();

  RootEvaluationContext ctx(&layout);
  const int32_t value = 12;
  ctx.Set(slot, value);
  EXPECT_EQ(12, ctx.Get(slot));
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
  bool interrupt = false;

  absl::AnyInvocable<absl::Status()> check_fn = [&interrupt]() -> absl::Status {
    return interrupt ? absl::CancelledError("interrupt") : absl::OkStatus();
  };

  EvaluationContext ctx(GetHeapBufferFactory(), &check_fn);

  EXPECT_THAT(ctx.signal_received(), IsFalse());
  EXPECT_TRUE(ctx.status().ok());

  for (int i = 0; i < 100; ++i) ctx.check_interrupt(10);

  EXPECT_THAT(ctx.signal_received(), IsFalse());
  EXPECT_TRUE(ctx.status().ok());

  interrupt = true;
  for (int i = 0; i < 100; ++i) ctx.check_interrupt(10);

  EXPECT_THAT(ctx.signal_received(), IsTrue());
  EXPECT_THAT(ctx.status(),
              StatusIs(absl::StatusCode::kCancelled, "interrupt"));
}

#ifndef NDEBUG
// Evaluation context performs runtime type checks in debug builds only.
TEST(EvalContextDeathTest, TypeMismatch) {
  FrameLayout::Builder builder1;
  auto slot1 = builder1.AddSlot<float>();
  auto descriptor1 = std::move(builder1).Build();
  RootEvaluationContext ctx1(&descriptor1);
  ctx1.Set(slot1, 1.0f);

  FrameLayout::Builder builder2;
  auto slot2 = builder2.AddSlot<std::string>();
  auto descriptor2 = std::move(builder2).Build();
  RootEvaluationContext ctx2(&descriptor2);
  ctx2.Set(slot2, "A string");

  // attempting to manipulate ctx1 with slot2 will cause program to crash
  // in a debug build.
  EXPECT_DEATH(ctx1.Set(slot2, "Another string"),
               "Field with given offset and type not found");
}

TEST(EvalContextDeathTest, TypeMismatchWithRegisteredExtraSlot) {
  FrameLayout::Builder builder;
  auto slot_int = builder.AddSlot<int>();
  auto slot_1byte =
      FrameLayout::Slot<char>::UnsafeSlotFromOffset(slot_int.byte_offset());
  auto slot_2bytes =
      FrameLayout::Slot<int16_t>::UnsafeSlotFromOffset(slot_int.byte_offset());
  ASSERT_OK(builder.RegisterUnsafeSlot(slot_1byte));
  auto descriptor = std::move(builder).Build();
  RootEvaluationContext ctx1(&descriptor);
  ctx1.Set(slot_int, 1);
  ctx1.Set(slot_1byte, 1);

  EXPECT_DEATH(ctx1.Set(slot_2bytes, 1),
               "Field with given offset and type not found");
}

#endif

}  // namespace
}  // namespace arolla
