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
#include "arolla/util/threading.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <thread>  // NOLINT(build/c++11)
#include <utility>
#include <vector>

namespace arolla {

StdThreading::StdThreading()
    : StdThreading(std::thread::hardware_concurrency()) {}

std::function<void()> StdThreading::StartThread(std::function<void()> fn) {
  return [t = std::make_shared<std::thread>(std::move(fn))] { t->join(); };
}

void ExecuteTasksInParallel(int max_parallelism,
                            const std::vector<std::function<void()>>& tasks) {
  std::atomic<size_t> current_task_num(0);
  auto worker_fn = [&tasks, &current_task_num]() {
    while (true) {
      size_t id = current_task_num.fetch_add(1);
      if (id >= tasks.size()) {
        return;
      }
      tasks[id]();
    }
  };
  const int num_threads = std::min<int>(tasks.size(), max_parallelism);
  std::vector<std::unique_ptr<std::thread>> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(std::make_unique<std::thread>(worker_fn));
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i]->join();
  }
}

}  // namespace arolla
