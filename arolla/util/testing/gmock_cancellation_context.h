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
#ifndef AROLLA_UTIL_TESTING_GMOCK_CANCELLATION_CONTEXT_H_
#define AROLLA_UTIL_TESTING_GMOCK_CANCELLATION_CONTEXT_H_

#include "gmock/gmock.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "arolla/util/cancellation_context.h"

namespace arolla::testing {

class MockCancellationContext final : public CancellationContext {
 public:
  // Creates a cancellation context without cooldown period.
  MockCancellationContext(/*no cooldown period*/)
      : MockCancellationContext{{}} {}

  // Creates a cancellation context with the specified cooldown period.
  explicit MockCancellationContext(absl::Duration cooldown_period)
      : CancellationContext(cooldown_period) {
    EXPECT_CALL(*this, DoCheck()).Times(::testing::AnyNumber());
  }

  MOCK_METHOD(absl::Status, DoCheck, (), (noexcept, final));
};

struct [[nodiscard]] MockCancellationScope {
  MockCancellationContext context;
  CancellationContext::ScopeGuard guard;

  // Creates a cancellation scope without cooldown period.
  MockCancellationScope(/*no cooldown period*/) : guard(&context) {}

  // Creates a cancellation scope with the specified cooldown period.
  explicit MockCancellationScope(absl::Duration cooldown_period)
      : context(cooldown_period), guard(&context) {}
};

}  // namespace arolla::testing

#endif  // AROLLA_UTIL_TESTING_GMOCK_CANCELLATION_CONTEXT_H_
