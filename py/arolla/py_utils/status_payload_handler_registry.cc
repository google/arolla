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

#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
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

  // Adds an handler to the registry. The type_url of the handler must be unique
  // among other registered handlers.
  absl::Status Register(absl::string_view type_url,
                        StatusPayloadHandler handler)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    if (handler == nullptr) {
      return absl::InvalidArgumentError("status handler is empty");
    }
    absl::WriterMutexLock lock(&mutex_);
    bool was_inserted =
        registry_.try_emplace(type_url, std::move(handler)).second;
    if (!was_inserted) {
      return absl::AlreadyExistsError(absl::StrCat(
          "status handler for type_url ", type_url, " is already registered"));
    }
    return absl::OkStatus();
  }

  // Returns registered handler instance keyed by type_url, if the handler is
  // present in the registry, or nullptr.
  StatusPayloadHandler GetHandlerOrNull(absl::string_view type_url) const
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock lock(&mutex_);
    auto it = registry_.find(type_url);
    if (it == registry_.end()) {
      return nullptr;
    }
    return it->second;
  }

 private:
  mutable absl::Mutex mutex_;
  absl::flat_hash_map<std::string, StatusPayloadHandler> registry_
      ABSL_GUARDED_BY(mutex_);
};  // namespace

}  // namespace

absl::Status RegisterStatusHandler(absl::string_view type_url,
                                   StatusPayloadHandler handler) {
  return StatusPayloadHandlerRegistry::GetInstance().Register(
      type_url, std::move(handler));
}

StatusPayloadHandler GetStatusHandlerOrNull(absl::string_view type_url) {
  return StatusPayloadHandlerRegistry::GetInstance().GetHandlerOrNull(type_url);
}

}  // namespace arolla::python
