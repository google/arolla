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
#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_CONTEXT_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_CONTEXT_H_

#include "absl//status/status.h"
#include "absl//time/time.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/cancellation_context.h"

namespace arolla::python {

// A cancellation context based on PyErr_CheckSignals.
class PyCancellationContext : public CancellationContext {
 public:
  PyCancellationContext(absl::Duration cooldown_period = absl::Milliseconds(10))
      : CancellationContext(cooldown_period) {}

 private:
  absl::Status DoCheck() final;
};

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_CONTEXT_H_
