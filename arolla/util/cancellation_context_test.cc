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
#include "absl//status/status.h"
#include "absl//status/status_matchers.h"
#include "absl//time/clock.h"
#include "absl//time/time.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;

TEST(CancellationContext, PreservesStatus) {
  bool once = true;
  auto cancel_ctx = CancellationContext::Make(absl::Milliseconds(10), 16, [&] {
    if (once) {
      once = false;
      return absl::CancelledError("cancelled");
    }
    return absl::OkStatus();
  });

  EXPECT_OK(cancel_ctx->status());
  EXPECT_TRUE(cancel_ctx->SoftCheck());
  EXPECT_FALSE(cancel_ctx->Check());
  EXPECT_FALSE(cancel_ctx->SoftCheck());
  EXPECT_FALSE(cancel_ctx->Check());
  EXPECT_THAT(cancel_ctx->status(),
              StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  EXPECT_THAT(cancel_ctx->status(),
              StatusIs(absl::StatusCode::kCancelled, "cancelled"));
}

TEST(CancellationContext, SoftCheck_CountdownPeriod) {
  {  // countdown_period=0
    auto cancel_ctx = CancellationContext::Make(
        /*no cooldown*/ absl::Nanoseconds(-1), 0,
        [] { return absl::CancelledError("cancelled"); });
    EXPECT_OK(cancel_ctx->status());
    EXPECT_FALSE(cancel_ctx->SoftCheck());
    EXPECT_THAT(cancel_ctx->status(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
  {  // countdown_period=1
    auto cancel_ctx = CancellationContext::Make(
        /*no cooldown*/ absl::Nanoseconds(-1), 1,
        [] { return absl::CancelledError("cancelled"); });
    EXPECT_OK(cancel_ctx->status());
    EXPECT_TRUE(cancel_ctx->SoftCheck());
    EXPECT_OK(cancel_ctx->status());
    EXPECT_FALSE(cancel_ctx->SoftCheck());
    EXPECT_THAT(cancel_ctx->status(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
  {  // countdown_period=2
    auto cancel_ctx = CancellationContext::Make(
        /*no cooldown*/ absl::Nanoseconds(-1), 2,
        [] { return absl::CancelledError("cancelled"); });
    EXPECT_OK(cancel_ctx->status());
    EXPECT_TRUE(cancel_ctx->SoftCheck());
    EXPECT_OK(cancel_ctx->status());
    EXPECT_TRUE(cancel_ctx->SoftCheck());
    EXPECT_OK(cancel_ctx->status());
    EXPECT_FALSE(cancel_ctx->SoftCheck());
    EXPECT_THAT(cancel_ctx->status(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
}

TEST(CancellationContext, SoftCheck_ReccuringCountdownPeriod) {
  // countdown_period=2
  int do_check_count = 0;
  auto cancel_ctx = CancellationContext::Make(
      /*no cooldown*/ absl::Nanoseconds(-1), 2, [&] {
        do_check_count += 1;
        return absl::OkStatus();
      });
  for (int expected_do_check_count = 0; expected_do_check_count < 10;
       ++expected_do_check_count) {
    EXPECT_EQ(do_check_count, expected_do_check_count);
    EXPECT_TRUE(cancel_ctx->SoftCheck());  // countdown: 2 -> 1
    EXPECT_EQ(do_check_count, expected_do_check_count);
    EXPECT_TRUE(cancel_ctx->SoftCheck());  // countdown: 1 -> 0
    EXPECT_EQ(do_check_count, expected_do_check_count);
    EXPECT_TRUE(cancel_ctx->SoftCheck());  // do_check
  }
}

TEST(CancellationContext, SoftCheck_ReccuringCooldownPeriod) {
  constexpr auto kCooldownPeriod = absl::Milliseconds(50);
  constexpr auto kN = 5;
  const auto stop = absl::Now() + kN * kCooldownPeriod;
  int do_check_count = 0;
  auto cancel_ctx = CancellationContext::Make(kCooldownPeriod, 0, [&] {
    do_check_count += 1;
    return absl::OkStatus();
  });
  while (absl::Now() < stop) {
    EXPECT_TRUE(cancel_ctx->SoftCheck());
  }
  EXPECT_GE(do_check_count, kN - 2);
  EXPECT_LE(do_check_count, kN);
}

}  // namespace
}  // namespace arolla
