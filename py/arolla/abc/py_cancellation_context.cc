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
#include "py/arolla/abc/py_cancellation_context.h"

#include <Python.h>

#include "absl//status/status.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {

absl::Status PyCancellationContext::DoCheck() {
  AcquirePyGIL guard;
  if (PyErr_CheckSignals() < 0) {
    return StatusCausedByPyErr(absl::StatusCode::kCancelled, "interrupted");
  }
  return absl::OkStatus();
}

}  // namespace arolla::python
