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
#ifndef THIRD_PARTY_PY_AROLLA_PY_UTILS_STATUS_PAYLOAD_HANDLER_REGISTRY_H_
#define THIRD_PARTY_PY_AROLLA_PY_UTILS_STATUS_PAYLOAD_HANDLER_REGISTRY_H_

#include <functional>

#include "absl/status/status.h"

namespace arolla::python {

// The handler handles the non ok status and its payload when C++ returns to
// Python. The handler must either raise Python exception and return true, or be
// no-op and return false. It is the handler's responsibility to trigger
// recursive handling of the "cause" error, if one exists.
//
// NOTE: It's the caller's responsibility to make sure the Python C API is ready
// to be called.
//
// TODO: Consider switching back to use typeid as key once
// migration to structured errors is done.
using StatusPayloadHandler = std::function<bool(const absl::Status& status)>;

// Adds a handler to the registry.
absl::Status RegisterStatusHandler(StatusPayloadHandler handler);

// Calls the registered handlers until one of them returns true, otherwise
// returns false.
bool CallStatusHandlers(const absl::Status& status);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_PY_UTILS_STATUS_PAYLOAD_HANDLER_REGISTRY_H_
