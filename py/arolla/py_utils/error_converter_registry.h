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
#ifndef THIRD_PARTY_PY_AROLLA_PY_UTILS_ERROR_CONVERTER_REGISTRY_H_
#define THIRD_PARTY_PY_AROLLA_PY_UTILS_ERROR_CONVERTER_REGISTRY_H_

#include <typeinfo>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"

namespace arolla::python {

// Converts non-ok statuses with a specific payload type when C++ returns to
// Python. The handler must raise a Python exception (it can use
// DefaultSetPyErrFromStatus for the cases when no specific handling is needed).
// It is the handler's responsibility to trigger recursive handling of the
// "cause" error, if one exists.
//
// NOTE: It's the caller's responsibility to make sure the Python C API is ready
// to be called.
//
using ErrorConverter =
    absl::AnyInvocable<void(const absl::Status& status) const>;

// Adds an error converter to the registry for the given payload type.
absl::Status RegisterErrorConverter(const std::type_info& payload_type,
                                    ErrorConverter converter);

// Adds an error converter to the registry for the given payload type.
template <typename Payload>
absl::Status RegisterErrorConverter(ErrorConverter converter) {
  return RegisterErrorConverter(typeid(Payload), std::move(converter));
}

// Returns the registered error converter for the given status, or nullptr.
const ErrorConverter* /*absl_nullable*/ GetRegisteredErrorConverter(
    const absl::Status& status);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_PY_UTILS_ERROR_CONVERTER_REGISTRY_H_
