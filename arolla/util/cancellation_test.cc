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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using ::absl_testing::StatusIs;

TEST(Cancellation, CancellationContext) {
  auto cancel_ctx = CancellationContext::Make();
  EXPECT_FALSE(cancel_ctx->Cancelled());
  EXPECT_OK(cancel_ctx->GetStatus());
  cancel_ctx->Cancel();
  EXPECT_TRUE(cancel_ctx->Cancelled());
  EXPECT_THAT(cancel_ctx->GetStatus(),
              StatusIs(absl::StatusCode::kCancelled, "cancelled"));
}

TEST(Cancellation, CancellationContext_CustomCancellationStatus) {
  auto cancel_ctx = CancellationContext::Make();
  cancel_ctx->Cancel(absl::InvalidArgumentError("message"));
  EXPECT_TRUE(cancel_ctx->Cancelled());
  EXPECT_THAT(cancel_ctx->GetStatus(),
              StatusIs(absl::StatusCode::kInvalidArgument, "message"));
}

TEST(Cancellation, CancellationContext_SecondaryCancellation) {
  auto cancel_ctx = CancellationContext::Make();
  cancel_ctx->Cancel(absl::InvalidArgumentError("first message"));
  cancel_ctx->Cancel(absl::InvalidArgumentError("second message"));
  EXPECT_THAT(cancel_ctx->GetStatus(),
              StatusIs(absl::StatusCode::kInvalidArgument, "first message"));
}

TEST(Cancellation, CancellationScope) {
  auto cancel_ctx_1 = CancellationContext::Make();
  auto cancel_ctx_2 = CancellationContext::Make();
  {
    CancellationContext::ScopeGuard guard_1(cancel_ctx_1);
    EXPECT_EQ(CancellationContext::ScopeGuard::current_cancellation_context(),
              cancel_ctx_1.get());
    EXPECT_EQ(guard_1.cancellation_context(), cancel_ctx_1.get());
    {
      CancellationContext::ScopeGuard guard_2(cancel_ctx_2);
      EXPECT_EQ(CancellationContext::ScopeGuard::current_cancellation_context(),
                cancel_ctx_2.get());
      EXPECT_EQ(guard_1.cancellation_context(), cancel_ctx_1.get());
      EXPECT_EQ(guard_2.cancellation_context(), cancel_ctx_2.get());
      {
        CancellationContext::ScopeGuard guard_3(nullptr);
        EXPECT_EQ(
            CancellationContext::ScopeGuard::current_cancellation_context(),
            nullptr);
        EXPECT_EQ(guard_1.cancellation_context(), cancel_ctx_1.get());
        EXPECT_EQ(guard_2.cancellation_context(), cancel_ctx_2.get());
        EXPECT_EQ(guard_3.cancellation_context(), nullptr);
      }
      EXPECT_EQ(CancellationContext::ScopeGuard::current_cancellation_context(),
                cancel_ctx_2.get());
    }
    EXPECT_EQ(CancellationContext::ScopeGuard::current_cancellation_context(),
              cancel_ctx_1.get());
  }
}

TEST(Cancellation, Cancelled) {
  {
    CancellationContext::ScopeGuard cancellation_scope;
    EXPECT_FALSE(Cancelled());
    cancellation_scope.current_cancellation_context()->Cancel();
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
    cancellation_scope.current_cancellation_context()->Cancel();
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
