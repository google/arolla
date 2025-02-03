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
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"

namespace arolla::python {

// The handler handles the non ok status and its payload when C++ returns to
// Python. The handler should not have side-effects other than raise Python
// exception.
//
// The handler will be skipped if the status has multiple payloads, i.e. only
// one type of payload is allowed.
//
// NOTE: It's the caller's responsibility to make sure the Python C API is ready
// to be called.
using StatusPayloadHandler =
    std::function<void(absl::Cord payload, const absl::Status& status)>;

// Adds an handler to the registry. The type_url that the handler can handle
// must be unique.
absl::Status RegisterStatusHandler(absl::string_view type_url,
                                   StatusPayloadHandler handler);

// Returns registered handler instance keyed by type_url, if the handler is
// present in the registry, or nullptr.
StatusPayloadHandler GetStatusHandlerOrNull(absl::string_view type_url);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_PY_UTILS_STATUS_PAYLOAD_HANDLER_REGISTRY_H_
