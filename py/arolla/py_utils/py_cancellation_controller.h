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
#ifndef THIRD_PARTY_PY_AROLLA_PY_UTILS_PY_CANCELLATION_SCOPE_IMPL_H
#define THIRD_PARTY_PY_AROLLA_PY_UTILS_PY_CANCELLATION_SCOPE_IMPL_H

#include "absl/base/nullability.h"
#include "arolla/util/cancellation.h"

namespace arolla::python::py_cancellation_controller {

// Initializes the cancellation controller.
//
// The initialization of the cancellation controller might fail. Even if so,
// the remaining subsystems can be run safely, but the controller
// will not provide a cancellation context.

// Initializes the cancellation controller.
//
// The initialization of the cancellation controller might fail, however, even
// if so, the remaining subsystems can be run safely; however, the controller
// will not provide a cancellation context.
void Init();

// Returns a cancellation context for the current Python thread.
//
// If the current thread is not the main Python thread, or if the controller is
// non-operational (e.g., due to an initialization failure), returns `nullptr`.
absl::Nullable<CancellationContextPtr> AcquirePyCancellationContext();

// Performs post-processing on the cancellation context. Particularly, if the
// cancellation context has been manually canceled, notifies the controller.
//
// Should only be called with a context previously returned by
// `AcquirePyCancellationContext()`.
void ReleasePyCancellationContext(
    absl::Nullable<CancellationContext*> cancellation_context);

}  // namespace arolla::python::py_cancellation_controller

#endif  // THIRD_PARTY_PY_AROLLA_PY_UTILS_PY_CANCELLATION_SCOPE_IMPL_H
