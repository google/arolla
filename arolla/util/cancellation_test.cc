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
#include "arolla/util/cancellation.h"

#include <atomic>
#include <future>  // NOLINT(build/c++11)
#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/notification.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;

TEST(Cancellation, CancellationContext) {
  auto cancellation_context = CancellationContext::Make();
  EXPECT_FALSE(cancellation_context->Cancelled());
  EXPECT_OK(cancellation_context->GetStatus());
  cancellation_context->Cancel();
  EXPECT_TRUE(cancellation_context->Cancelled());
  EXPECT_THAT(cancellation_context->GetStatus(),
              StatusIs(absl::StatusCode::kCancelled, "cancelled"));
}

TEST(Cancellation, CancellationContext_CustomCancellationStatus) {
  auto cancellation_context = CancellationContext::Make();
  cancellation_context->Cancel(absl::InvalidArgumentError("message"));
  EXPECT_TRUE(cancellation_context->Cancelled());
  EXPECT_THAT(cancellation_context->GetStatus(),
              StatusIs(absl::StatusCode::kInvalidArgument, "message"));
}

TEST(Cancellation, CancellationContext_SecondaryCancellation) {
  auto cancellation_context = CancellationContext::Make();
  cancellation_context->Cancel(absl::InvalidArgumentError("first message"));
  cancellation_context->Cancel(absl::InvalidArgumentError("second message"));
  EXPECT_THAT(cancellation_context->GetStatus(),
              StatusIs(absl::StatusCode::kInvalidArgument, "first message"));
}

TEST(Cancellation, CancellationContext_Subscribe) {
  auto cancellation_context = CancellationContext::Make();
  auto callback_1_done = std::make_shared<std::atomic_bool>(false);
  auto callback_2_done = std::make_shared<std::atomic_bool>(false);
  auto callback_3_done = std::make_shared<std::atomic_bool>(false);
  auto callback_4_done = std::make_shared<std::atomic_bool>(false);
  auto callback_5_done = std::make_shared<std::atomic_bool>(false);
  std::optional subscription_1 = cancellation_context->Subscribe(
      [callback_1_done] { *callback_1_done = true; });
  std::optional subscription_2 = cancellation_context->Subscribe(
      [callback_2_done] { *callback_2_done = true; });
  std::optional subscription_3 = cancellation_context->Subscribe(
      [callback_3_done] { *callback_3_done = true; });
  std::optional subscription_4 = cancellation_context->Subscribe(
      [callback_4_done] { *callback_4_done = true; });
  std::optional subscription_5 = cancellation_context->Subscribe(
      [callback_5_done] { *callback_5_done = true; });
  subscription_1.reset();
  subscription_3.reset();
  subscription_5.reset();
  std::move(*subscription_4).Detach();
  subscription_4.reset();
  EXPECT_FALSE(*callback_1_done);
  EXPECT_FALSE(*callback_2_done);
  EXPECT_FALSE(*callback_3_done);
  EXPECT_FALSE(*callback_4_done);
  EXPECT_FALSE(*callback_5_done);
  cancellation_context->Cancel();
  EXPECT_FALSE(*callback_1_done);
  EXPECT_TRUE(*callback_2_done);
  EXPECT_FALSE(*callback_3_done);
  EXPECT_TRUE(*callback_4_done);
  EXPECT_FALSE(*callback_5_done);
}

TEST(Cancellation, CancellationContext_Subscribe_AlreadyCancelled) {
  auto cancellation_context = CancellationContext::Make();
  cancellation_context->Cancel();
  auto callback_done = std::make_shared<std::atomic_bool>(false);
  cancellation_context->Subscribe([callback_done] { *callback_done = true; })
      .Detach();
  EXPECT_TRUE(*callback_done);
}

TEST(Cancellation, CancellationContext_Subscribe_NeverCancelled) {
  auto callback_done = std::make_shared<std::atomic_bool>(false);
  {
    auto cancellation_context = CancellationContext::Make();
    cancellation_context->Subscribe([callback_done] { *callback_done = true; })
        .Detach();
  }
  EXPECT_FALSE(*callback_done);
}

TEST(Cancellation, CancellationContext_Subscription_MoveSemantics) {
  auto callback_done = std::make_shared<std::atomic_bool>(false);
  auto cancellation_context = CancellationContext::Make();
  CancellationContext::Subscription subscription;
  {
    auto subscription_1 = cancellation_context->Subscribe(
        [callback_done] { *callback_done = true; });
    auto subscription_2 = std::move(subscription_1);
    subscription = std::move(subscription_2);
  }
  EXPECT_FALSE(*callback_done);
  cancellation_context->Cancel();
  EXPECT_TRUE(*callback_done);
}

TEST(Cancellation, CancellationContext_ToAbslNotification) {
  auto cancellation_context = CancellationContext::Make();
  auto notification = std::make_shared<absl::Notification>();
  cancellation_context->Subscribe([notification] { notification->Notify(); })
      .Detach();
  EXPECT_FALSE(notification->HasBeenNotified());
  auto future =
      std::async([cancellation_context] { cancellation_context->Cancel(); });
  notification->WaitForNotification();
  EXPECT_TRUE(notification->HasBeenNotified());
}

TEST(Cancellation, CancellationScope) {
  auto cancellation_context_1 = CancellationContext::Make();
  auto cancellation_context_2 = CancellationContext::Make();
  {
    CancellationContext::ScopeGuard guard_1(cancellation_context_1);
    EXPECT_EQ(CurrentCancellationContext(), cancellation_context_1);
    EXPECT_EQ(guard_1.cancellation_context(), cancellation_context_1.get());
    {
      CancellationContext::ScopeGuard guard_2(cancellation_context_2);
      EXPECT_EQ(CurrentCancellationContext(), cancellation_context_2);
      EXPECT_EQ(guard_1.cancellation_context(), cancellation_context_1.get());
      EXPECT_EQ(guard_2.cancellation_context(), cancellation_context_2.get());
      {
        CancellationContext::ScopeGuard guard_3(nullptr);
        EXPECT_EQ(CurrentCancellationContext(), nullptr);
        EXPECT_EQ(guard_1.cancellation_context(), cancellation_context_1.get());
        EXPECT_EQ(guard_2.cancellation_context(), cancellation_context_2.get());
        EXPECT_EQ(guard_3.cancellation_context(), nullptr);
      }
      EXPECT_EQ(CurrentCancellationContext(), cancellation_context_2);
    }
    EXPECT_EQ(CurrentCancellationContext(), cancellation_context_1);
  }
}

TEST(Cancellation, Cancelled) {
  {
    CancellationContext::ScopeGuard cancellation_scope;
    EXPECT_FALSE(Cancelled());
    CurrentCancellationContext()->Cancel();
    EXPECT_TRUE(Cancelled());
    {
      CancellationContext::ScopeGuard cancellation_scope;
      EXPECT_FALSE(Cancelled());
    }
    EXPECT_TRUE(Cancelled());
  }
  {
    EXPECT_FALSE(Cancelled());
  }
}

TEST(Cancellation, CheckCancellation) {
  {
    CancellationContext::ScopeGuard cancellation_scope;
    EXPECT_OK(CheckCancellation());
    CurrentCancellationContext()->Cancel();
    EXPECT_THAT(CheckCancellation(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
    {
      CancellationContext::ScopeGuard cancellation_scope;
      EXPECT_OK(CheckCancellation());
    }
    EXPECT_THAT(CheckCancellation(),
                StatusIs(absl::StatusCode::kCancelled, "cancelled"));
  }
  {
    EXPECT_OK(CheckCancellation());
  }
}

}  // namespace
}  // namespace arolla
