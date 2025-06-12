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
#include "arolla/util/permanent_event.h"

#include <future>  // NOLINT(build/c++11)
#include <vector>

#include "gtest/gtest.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace arolla {
namespace {

TEST(PermanentEventTest, Basic) {
  auto event = PermanentEvent::Make();
  EXPECT_FALSE(event->HasBeenNotified());
  event->Notify();
  EXPECT_TRUE(event->HasBeenNotified());
  event->Notify();
  EXPECT_TRUE(event->HasBeenNotified());
  event->Wait();
  EXPECT_TRUE(event->WaitWithDeadline(absl::InfiniteFuture()));
  EXPECT_TRUE(event->WaitWithDeadline(absl::InfinitePast()));
}

TEST(PermanentEventTest, Timeout) {
  auto event = PermanentEvent::Make();
  EXPECT_FALSE(event->WaitWithDeadline(absl::InfinitePast()));

  const auto deadline = absl::Now() + absl::Milliseconds(5);
  EXPECT_FALSE(event->WaitWithDeadline(deadline));
  EXPECT_GE(absl::Now(), deadline);
}

TEST(PermanentEventTest, Multithreaded) {
  auto event = PermanentEvent::Make();
  std::vector<std::future<void>> futures(100);
  for (auto& future : futures) {
    future = std::async(std::launch::async, [event] { event->Wait(); });
  }
  event->Notify();
  for (auto& future : futures) {
    future.get();
  }
}

}  // namespace
}  // namespace arolla
