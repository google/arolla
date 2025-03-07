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
#include "arolla/util/cancellation_context.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "arolla/util/testing/gmock_cancellation_context.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::MockCancellationContext;
using ::arolla::testing::MockCancellationScope;
using ::testing::Between;
using ::testing::Return;

TEST(CancellationContext, PreservesStatus) {
  MockCancellationContext cancel_ctx;
  EXPECT_CALL(cancel_ctx, DoCheck())
      .WillOnce(Return(absl::CancelledError("cancelled")));

  EXPECT_OK(cancel_ctx.status());
  EXPECT_TRUE(cancel_ctx.SoftCheck());
  EXPECT_FALSE(cancel_ctx.Check());
  EXPECT_FALSE(cancel_ctx.SoftCheck());
  EXPECT_FALSE(cancel_ctx.Check());
  EXPECT_THAT(cancel_ctx.status(),
              StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  EXPECT_THAT(cancel_ctx.status(),
              StatusIs(absl::StatusCode::kCancelled, "cancelled"));
}

TEST(CancellationContext, SoftCheck_CountdownPeriod) {
  {
    constexpr auto decrement = -1;
    MockCancellationContext cancel_ctx;
    EXPECT_CALL(cancel_ctx, DoCheck())
        .WillOnce(Return(absl::CancelledError("cancelled")));
    EXPECT_OK(cancel_ctx.status());
    EXPECT_FALSE(cancel_ctx.SoftCheck(decrement));
    EXPECT_THAT(cancel_ctx.status(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
  {
    constexpr auto decrement = (CancellationContext::kCountdownPeriod + 1) / 2;
    MockCancellationContext cancel_ctx;
    EXPECT_CALL(cancel_ctx, DoCheck())
        .WillOnce(Return(absl::CancelledError("cancelled")));
    EXPECT_OK(cancel_ctx.status());
    EXPECT_TRUE(cancel_ctx.SoftCheck(decrement));
    EXPECT_OK(cancel_ctx.status());
    EXPECT_FALSE(cancel_ctx.SoftCheck(decrement));
    EXPECT_THAT(cancel_ctx.status(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
  {
    constexpr auto decrement = (CancellationContext::kCountdownPeriod + 2) / 3;
    MockCancellationContext cancel_ctx;
    EXPECT_CALL(cancel_ctx, DoCheck())
        .WillOnce(Return(absl::CancelledError("cancelled")));
    EXPECT_OK(cancel_ctx.status());
    EXPECT_TRUE(cancel_ctx.SoftCheck(decrement));
    EXPECT_OK(cancel_ctx.status());
    EXPECT_TRUE(cancel_ctx.SoftCheck(decrement));
    EXPECT_OK(cancel_ctx.status());
    EXPECT_FALSE(cancel_ctx.SoftCheck(decrement));
    EXPECT_THAT(cancel_ctx.status(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
}

TEST(CancellationContext, SoftCheck_ReccuringCountdownPeriod) {
  auto decrement = (CancellationContext::kCountdownPeriod + 2) / 3;
  MockCancellationContext cancel_ctx;
  for (int expected_do_check_count = 0; expected_do_check_count < 10;
       ++expected_do_check_count) {
    EXPECT_CALL(cancel_ctx, DoCheck()).Times(0);
    EXPECT_TRUE(cancel_ctx.SoftCheck(decrement));  // countdown: 2/3
    EXPECT_TRUE(cancel_ctx.SoftCheck(decrement));  // countdown: 1/3
    EXPECT_CALL(cancel_ctx, DoCheck()).Times(1);
    EXPECT_TRUE(cancel_ctx.SoftCheck(decrement));  // do_check
  }
}

TEST(CancellationContext, SoftCheck_ReccuringCooldownPeriod) {
  constexpr auto kCooldownPeriod = absl::Milliseconds(50);
  constexpr auto kN = 5;
  const auto stop = absl::Now() + kN * kCooldownPeriod;

  MockCancellationContext cancel_ctx(kCooldownPeriod);
  EXPECT_CALL(cancel_ctx, DoCheck()).Times(Between(kN - 2, kN));
  while (absl::Now() < stop) {
    EXPECT_TRUE(cancel_ctx.SoftCheck(-1));
  }
}

TEST(CancellationContext, ScopeGuard) {
  MockCancellationContext cancel_ctx_1;
  MockCancellationContext cancel_ctx_2;
  {
    CancellationContext::ScopeGuard guard_1(&cancel_ctx_1);
    EXPECT_EQ(CancellationContext::ScopeGuard::active_cancellation_context(),
              &cancel_ctx_1);
    {
      CancellationContext::ScopeGuard guard_2(&cancel_ctx_2);
      EXPECT_EQ(CancellationContext::ScopeGuard::active_cancellation_context(),
                &cancel_ctx_2);
      {
        CancellationContext::ScopeGuard guard_3(nullptr);
        EXPECT_EQ(
            CancellationContext::ScopeGuard::active_cancellation_context(),
            nullptr);
      }
      EXPECT_EQ(CancellationContext::ScopeGuard::active_cancellation_context(),
                &cancel_ctx_2);
    }
    EXPECT_EQ(CancellationContext::ScopeGuard::active_cancellation_context(),
              &cancel_ctx_1);
  }
}

TEST(CancellationContext, IsCancelled) {
  {
    MockCancellationScope cancellation_scope;
    EXPECT_CALL(cancellation_scope.context, DoCheck())
        .WillOnce(Return(absl::CancelledError("cancelled")));
    EXPECT_FALSE(IsCancelled());
    EXPECT_FALSE(IsCancelled());
    EXPECT_FALSE(IsCancelled());
    EXPECT_TRUE(IsCancelled(-1));
  }
  {
    EXPECT_FALSE(IsCancelled());
    EXPECT_FALSE(IsCancelled(-1));
  }
}

TEST(CancellationContext, CheckCancellation) {
  {
    MockCancellationScope cancellation_scope;
    EXPECT_CALL(cancellation_scope.context, DoCheck())
        .WillOnce(Return(absl::CancelledError("cancelled")));
    EXPECT_OK(CheckCancellation());
    EXPECT_OK(CheckCancellation());
    EXPECT_OK(CheckCancellation());
    EXPECT_THAT(CheckCancellation(-1),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
  {
    EXPECT_OK(CheckCancellation());
    EXPECT_OK(CheckCancellation(-1));
  }
}

}  // namespace
}  // namespace arolla
