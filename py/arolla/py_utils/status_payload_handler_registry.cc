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
#include "py/arolla/py_utils/status_payload_handler_registry.h"

#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

namespace arolla::python {

namespace {

// A centralised registry of status payload handlers.
//
// On the last step of returning from C++ to Python, the status might contains
// payload that need to be correctly processed. This registry allows customized
// handling of different payloads. This registry class provides a thread-safe
// access to the registered handlers.
class StatusPayloadHandlerRegistry final {
 public:
  // Returns the singleton instance of the class.
  static StatusPayloadHandlerRegistry& GetInstance() {
    static absl::NoDestructor<StatusPayloadHandlerRegistry> registry;
    return *registry;
  }
  // Use ExprOperatorRegistry::GetInstance() instead to access the singleton.
  StatusPayloadHandlerRegistry() = default;

  // No copying.
  StatusPayloadHandlerRegistry(const StatusPayloadHandlerRegistry&) = delete;
  StatusPayloadHandlerRegistry& operator=(const StatusPayloadHandlerRegistry&) =
      delete;

  absl::Status Register(StatusPayloadHandler handler)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    if (handler == nullptr) {
      return absl::InvalidArgumentError("status handler is empty");
    }
    absl::WriterMutexLock lock(&mutex_);
    registry_.emplace_back(std::move(handler));
    return absl::OkStatus();
  }

  bool CallStatusHandlers(const absl::Status& status) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    // TODO: Avoid copying the handlers.
    std::vector<StatusPayloadHandler> handlers_copy;
    {
      absl::ReaderMutexLock lock(&mutex_);
      handlers_copy = registry_;
    }

    for (const auto& handler : handlers_copy) {
      if (handler(status)) {
        return true;
      }
    }
    return false;
  }

 private:
  mutable absl::Mutex mutex_;
  std::vector<StatusPayloadHandler> registry_ ABSL_GUARDED_BY(mutex_);
};  // namespace

}  // namespace

absl::Status RegisterStatusHandler(StatusPayloadHandler handler) {
  return StatusPayloadHandlerRegistry::GetInstance().Register(
      std::move(handler));
}

bool CallStatusHandlers(const absl::Status& status) {
  return StatusPayloadHandlerRegistry::GetInstance().CallStatusHandlers(status);
}

}  // namespace arolla::python
