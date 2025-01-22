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
#include "py/arolla/abc/py_cancellation_checker.h"

#include <Python.h>

#include "absl//status/status.h"
#include "absl//time/clock.h"
#include "absl//time/time.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {

PyCancellationChecker::PyCancellationChecker(int countdown_period,
                                             absl::Duration cooldown_period)
    : countdown_(countdown_period),
      countdown_period_(countdown_period),
      cooldown_period_ns_(absl::ToInt64Nanoseconds(cooldown_period)) {
  cooldown_ns_ = absl::GetCurrentTimeNanos() + cooldown_period_ns_;
}

absl::Status PyCancellationChecker::SoftCheck() {
  if (--countdown_ < 0) [[unlikely]] {
    if (absl::GetCurrentTimeNanos() > cooldown_ns_) [[unlikely]] {
      return Check();
    }
    countdown_ = countdown_period_;
  }
  return absl::OkStatus();
}

absl::Status PyCancellationChecker::Check() {
  countdown_ = countdown_period_;
  cooldown_ns_ = absl::GetCurrentTimeNanos() + cooldown_period_ns_;
  AcquirePyGIL guard;
  if (PyErr_CheckSignals() < 0) {
    return StatusCausedByPyErr(absl::StatusCode::kCancelled, "interrupted");
  }
  return absl::OkStatus();
}

}  // namespace arolla::python
