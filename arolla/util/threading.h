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
#ifndef AROLLA_UTIL_THREADING_H_
#define AROLLA_UTIL_THREADING_H_

#include <algorithm>
#include <functional>
#include <vector>

namespace arolla {

// Generic interface to start threads.
//   Usage example:
// void DoInParrallel(ThreadingInterface& threading) {
//   threading.WithThreading([&]{
//     int N = threading.GetRecommendedThreadCount();
//     std::vector<std::function<void()>> join_fns;
//     join_fns.reserve(N - 1);
//
//     for (int i = 1; i < N; ++i) {  // Start N-1 workers
//       join_fns.push_back(threading.StartThread([]{ DoWork(/*worker=*/i); }));
//     }
//     DoWork(/*worker=*/0);  // Run worker #0 in the main thread.
//     for (auto& join_fn : join_fns) { join_fn(); }  // Wait for workers.
//   }
// }
//
class ThreadingInterface {
 public:
  using TaskFn = std::function<void()>;
  using JoinFn = std::function<void()>;

  virtual ~ThreadingInterface() = default;

  // The number of threads that can efficiently run in parallel.
  virtual int GetRecommendedThreadCount() const = 0;

  // Runs `fn` as root thread and waits for it to finish.
  virtual void WithThreading(std::function<void()> fn) { fn(); }

  // Starts a new thread (regardless of recommended thread count) and returns
  // an std::function that holds the thread and can be used to wait for the
  // thread to finish. Should be used inside of `WithThreading`.
  [[nodiscard]] virtual JoinFn StartThread(TaskFn fn) = 0;
};

// Implementation of ThreadingInterface based on std::thread.
class StdThreading : public ThreadingInterface {
 public:
  StdThreading();  // thread_count = std::thread::hardware_concurrency()
  explicit StdThreading(int thread_count)
      : thread_count_(std::max(thread_count, 1)) {}
  int GetRecommendedThreadCount() const final { return thread_count_; };
  [[nodiscard]] JoinFn StartThread(TaskFn fn) final;

 private:
  int thread_count_;
};

// Executes the given set of tasks in parallel and then waits for them to all
// complete.
//
// @param max_parallelism  The max parallelism with which to run tasks.
// @param tasks  A list of tasks to run.
void ExecuteTasksInParallel(int max_parallelism,
                            const std::vector<std::function<void()>>& tasks);

}  // namespace arolla

#endif  // AROLLA_UTIL_THREADING_H_
