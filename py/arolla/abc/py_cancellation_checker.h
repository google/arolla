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
#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_CHECKER_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_CHECKER_H_

#include <cstdint>

#include "absl//status/status.h"
#include "absl//time/clock.h"
#include "absl//time/time.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/qexpr/eval_context.h"

namespace arolla::python {

// A cancellation checker based on PyErr_CheckSignals.
//
// Calling PyErr_CheckSignals() is a relatively costly operation because it
// requires acquiring the Python GIL and may execute signal handlers. To reduce
// overhead, we limit the frequency of PyErr_CheckSignals() calls.
//
// There are two mechanisms for limiting the call rate to PyErr_CheckSignals():
// 1. For every SoftCheck() call, we decrement the `countdown`. When the counter
//    runs out, we proceed to check the timestamp.
// 2. If the time since the last PyErr_CheckSignals() call has elapsed beyond
//    the set cooldown period, we perform a full `Check()` call.
//
// The `countdown_period` is necessary because, even though checking
// the timestamp takes only a few dozen nanoseconds, it still adds measurable
// overhead to lightweight operations, such as scalar addition. This mechanism
// helps balance efficiency and responsiveness.
//
class PyCancellationChecker : public EvaluationContext::CancellationChecker {
 public:
  PyCancellationChecker(
      int countdown_period = 16,
      absl::Duration cooldown_period = absl::Milliseconds(10));

  absl::Status SoftCheck() final;

  absl::Status Check() final;

 private:
  int countdown_;
  const int countdown_period_;
  int64_t cooldown_ns_;
  const int64_t cooldown_period_ns_;
};

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_CANCELLATION_CHECKER_H_
