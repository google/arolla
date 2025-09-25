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
#include "py/arolla/py_utils/error_converter_registry.h"

#include <memory>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/util/demangle.h"
#include "arolla/util/status.h"

namespace arolla::python {
namespace {

// A centralised registry of status payload handlers.
//
// On the last step of returning from C++ to Python, the status might contains
// payload that need to be correctly processed. This registry allows customized
// handling of different payloads. This registry class provides a thread-safe
// access to the registered handlers.
class ErrorConverterRegistry final {
 public:
  // Returns the singleton instance of the class.
  static ErrorConverterRegistry& GetInstance() {
    static absl::NoDestructor<ErrorConverterRegistry> registry;
    return *registry;
  }
  // Use ExprOperatorRegistry::GetInstance() instead to access the singleton.
  ErrorConverterRegistry() = default;

  // No copying.
  ErrorConverterRegistry(const ErrorConverterRegistry&) = delete;
  ErrorConverterRegistry& operator=(const ErrorConverterRegistry&) = delete;

  absl::Status Register(const std::type_info& payload_type,
                        ErrorConverter converter) ABSL_LOCKS_EXCLUDED(mutex_) {
    if (converter == nullptr) {
      return absl::InvalidArgumentError("error converter is empty");
    }
    auto converter_ptr = std::make_unique<ErrorConverter>(std::move(converter));
    absl::MutexLock lock(mutex_);
    auto [_, inserted] = registry_.emplace(std::type_index(payload_type),
                                           std::move(converter_ptr));
    if (!inserted) {
      return absl::InternalError(absl::StrCat("error converter for ",
                                              TypeName(payload_type),
                                              " payload already registered"));
    }
    return absl::OkStatus();
  }

  const ErrorConverter* absl_nullable GetErrorConverter(
      const std::type_info& payload_type) const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(mutex_);
    auto it = registry_.find(std::type_index(payload_type));
    return it == registry_.end() ? nullptr : it->second.get();
  }

 private:
  mutable absl::Mutex mutex_;
  absl::flat_hash_map<std::type_index, std::unique_ptr<ErrorConverter>>
      registry_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace

absl::Status RegisterErrorConverter(
    const std::type_info& payload_type,
    absl::AnyInvocable<void(const absl::Status& status) const> converter) {
  return ErrorConverterRegistry::GetInstance().Register(payload_type,
                                                        std::move(converter));
}

const ErrorConverter* absl_nullable GetRegisteredErrorConverter(
    const absl::Status& status) {
  const auto* payload = GetPayload(status);
  if (payload == nullptr) {
    return nullptr;
  }
  return ErrorConverterRegistry::GetInstance().GetErrorConverter(
      payload->type());
}

}  // namespace arolla::python
