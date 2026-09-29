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
#include "arolla/util/signal_safe_event.h"

#include <pthread.h>
#include <signal.h>

#include <atomic>
#include <cerrno>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace arolla {
namespace {

constexpr absl::Duration kTimeout = absl::Seconds(1);
constexpr absl::Duration kShortDelay = absl::Milliseconds(10);

TEST(SignalSafeEventTest, TryConsumeOnAFreshInstance) {
  SignalSafeEvent event;
  EXPECT_FALSE(event.TryConsume());
}

TEST(SignalSafeEventTest, NotifyThenTryConsume) {
  SignalSafeEvent event;
  event.Notify();
  EXPECT_TRUE(event.TryConsume());
  EXPECT_FALSE(event.TryConsume());
}

TEST(SignalSafeEventTest, NotifyThenWaitDoesNotBlock) {
  SignalSafeEvent event;
  event.Notify();
  event.WaitAndConsume();
  EXPECT_FALSE(event.TryConsume());
}

TEST(SignalSafeEventTest, EventsCollapse) {
  SignalSafeEvent event;
  event.Notify();
  event.Notify();
  event.Notify();
  event.WaitAndConsume();
  EXPECT_FALSE(event.TryConsume());
}

TEST(SignalSafeEventTest, WaitBlocksUntilNotify) {
  SignalSafeEvent event;
  absl::Notification about_to_wait;
  absl::Notification wait_returned;
  std::thread consumer([&] {
    about_to_wait.Notify();
    event.WaitAndConsume();
    wait_returned.Notify();
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  EXPECT_FALSE(wait_returned.WaitForNotificationWithTimeout(kShortDelay));

  event.Notify();
  EXPECT_TRUE(wait_returned.WaitForNotificationWithTimeout(kTimeout));
  consumer.join();
}

TEST(SignalSafeEventTest, NoStaleTokenAfterTryConsume) {
  SignalSafeEvent event;
  event.Notify();
  ASSERT_TRUE(event.TryConsume());

  absl::Notification about_to_wait;
  absl::Notification wait_returned;
  std::thread consumer([&] {
    about_to_wait.Notify();
    event.WaitAndConsume();
    wait_returned.Notify();
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  EXPECT_FALSE(wait_returned.WaitForNotificationWithTimeout(kShortDelay));

  event.Notify();
  EXPECT_TRUE(wait_returned.WaitForNotificationWithTimeout(kTimeout));
  consumer.join();
}

TEST(SignalSafeEventTest, ReusableAcrossManyRounds) {
  constexpr int kRounds = 1000;
  SignalSafeEvent request;
  SignalSafeEvent response;
  int counter = 0;
  std::thread consumer([&] {
    for (int i = 0; i < kRounds; ++i) {
      request.WaitAndConsume();
      ++counter;
      response.Notify();
    }
  });
  for (int i = 0; i < kRounds; ++i) {
    request.Notify();
    response.WaitAndConsume();
  }
  consumer.join();
  EXPECT_EQ(counter, kRounds);
}

TEST(SignalSafeEventTest, NoLostWakeupsUnderContention) {
  constexpr int kProducers = 8;
  constexpr int kItemsPerProducer = 2000;
  constexpr int kTotal = kProducers * kItemsPerProducer;

  SignalSafeEvent event;
  std::atomic<int> produced{0};
  std::atomic<int> consumed{0};

  std::thread consumer([&] {
    for (;;) {
      consumed.store(produced.load());
      if (consumed.load() >= kTotal) {
        return;
      }
      event.WaitAndConsume();
    }
  });

  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (int i = 0; i < kProducers; ++i) {
    producers.emplace_back([&] {
      for (int j = 0; j < kItemsPerProducer; ++j) {
        produced.fetch_add(1);
        event.Notify();
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  consumer.join();
  EXPECT_EQ(consumed.load(), kTotal);
}

TEST(SignalSafeEventTest, ConstructionAndDestructionPreserveErrno) {
  errno = ERANGE;
  {
    SignalSafeEvent event;
  }
  EXPECT_EQ(errno, ERANGE);
}

TEST(SignalSafeEventTest, NotifyPreservesErrno) {
  SignalSafeEvent event;
  errno = ERANGE;
  event.Notify();
  EXPECT_EQ(errno, ERANGE);
  EXPECT_TRUE(event.TryConsume());

  absl::Notification about_to_wait;
  std::thread consumer([&] {
    about_to_wait.Notify();
    event.WaitAndConsume();
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  absl::SleepFor(kShortDelay);  // Let the consumer reach `read()`.
  errno = ERANGE;
  event.Notify();
  EXPECT_EQ(errno, ERANGE);
  consumer.join();
}

TEST(SignalSafeEventTest, WaitAndConsumePreservesErrno) {
  SignalSafeEvent event;
  event.Notify();
  errno = ERANGE;
  event.WaitAndConsume();  // Returns without touching `event_fd_`.
  EXPECT_EQ(errno, ERANGE);

  absl::Notification about_to_wait;
  int consumer_errno = 0;
  std::thread consumer([&] {
    about_to_wait.Notify();
    errno = ERANGE;
    event.WaitAndConsume();  // Blocks in `read()`.
    consumer_errno = errno;
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  absl::SleepFor(kShortDelay);  // Let the consumer reach `read()`.
  event.Notify();
  consumer.join();
  EXPECT_EQ(consumer_errno, ERANGE);
}

std::atomic<SignalSafeEvent*> g_event{nullptr};
std::atomic<int> g_signals_handled{0};

void NotifyFromSignalHandler(int /*signo*/) {
  g_signals_handled.fetch_add(1);
  if (SignalSafeEvent* event = g_event.load()) {
    event->Notify();
  }
}

void CountingSignalHandler(int /*signo*/) { g_signals_handled.fetch_add(1); }

// Installs a SIGUSR1 handler for the duration of a test.
class ScopedSigusr1Handler {
 public:
  explicit ScopedSigusr1Handler(void (*handler)(int)) {
    g_signals_handled.store(0);
    struct sigaction action = {};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    CHECK_EQ(sigaction(SIGUSR1, &action, &previous_), 0);
  }

  ~ScopedSigusr1Handler() {
    g_event.store(nullptr);
    sigaction(SIGUSR1, &previous_, nullptr);
  }

 private:
  struct sigaction previous_ = {};
};

TEST(SignalSafeEventTest, NotifyFromASignalHandler) {
  SignalSafeEvent event;
  g_event.store(&event);
  ScopedSigusr1Handler handler(&NotifyFromSignalHandler);

  absl::Notification about_to_wait;
  absl::Notification wait_returned;
  pthread_t consumer_tid{};
  std::thread consumer([&] {
    consumer_tid = pthread_self();
    about_to_wait.Notify();
    event.WaitAndConsume();
    wait_returned.Notify();
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  absl::SleepFor(kShortDelay);  // Let the consumer reach `read()`.

  // Deliver the signal directly to the blocked consumer thread so the handler
  // interrupts `read()` and posts the notification from that signal context.
  ASSERT_EQ(pthread_kill(consumer_tid, SIGUSR1), 0);
  EXPECT_TRUE(wait_returned.WaitForNotificationWithTimeout(kTimeout));
  EXPECT_EQ(g_signals_handled.load(), 1);
  consumer.join();
}

TEST(SignalSafeEventTest, WaitIsNotInterruptedBySignals) {
  SignalSafeEvent event;
  ScopedSigusr1Handler handler(&CountingSignalHandler);

  absl::Notification about_to_wait;
  absl::Notification wait_returned;
  pthread_t consumer_tid{};
  std::thread consumer([&] {
    consumer_tid = pthread_self();
    about_to_wait.Notify();
    event.WaitAndConsume();
    wait_returned.Notify();
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  absl::SleepFor(kShortDelay);  // Let the consumer reach `read()`.

  ASSERT_EQ(pthread_kill(consumer_tid, SIGUSR1), 0);
  EXPECT_FALSE(wait_returned.WaitForNotificationWithTimeout(kShortDelay));
  EXPECT_EQ(g_signals_handled.load(), 1);

  event.Notify();
  EXPECT_TRUE(wait_returned.WaitForNotificationWithTimeout(kTimeout));
  consumer.join();
}

TEST(SignalSafeEventTest, WaitPreservesErrnoAcrossEINTR) {
  SignalSafeEvent event;
  ScopedSigusr1Handler handler(&CountingSignalHandler);

  absl::Notification about_to_wait;
  pthread_t consumer_tid{};
  int consumer_errno = 0;
  std::thread consumer([&] {
    consumer_tid = pthread_self();
    about_to_wait.Notify();
    errno = ERANGE;
    event.WaitAndConsume();
    consumer_errno = errno;
  });
  ASSERT_TRUE(about_to_wait.WaitForNotificationWithTimeout(kTimeout));
  absl::SleepFor(kShortDelay);  // Let the consumer reach `read()`.

  ASSERT_EQ(pthread_kill(consumer_tid, SIGUSR1), 0);
  absl::SleepFor(kShortDelay);  // Let the consumer return to `read()`.
  EXPECT_EQ(g_signals_handled.load(), 1);

  event.Notify();
  consumer.join();
  EXPECT_EQ(consumer_errno, ERANGE);
}

}  // namespace
}  // namespace arolla
