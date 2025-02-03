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
#include "arolla/util/cancellation_context.h"

#include <memory>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/time/time.h"

namespace arolla {
namespace {

class FnBasedCancellationContext final : public CancellationContext {
 public:
  FnBasedCancellationContext(absl::Duration cooldown_period,
                             absl::AnyInvocable<absl::Status()>&& do_check_fn)
      : CancellationContext(cooldown_period),
        do_check_fn_(std::move(do_check_fn)) {}

  absl::Status DoCheck() final {
    return do_check_fn_ ? do_check_fn_() : absl::OkStatus();
  }

  absl::AnyInvocable<absl::Status()> do_check_fn_;
};

}  // namespace

bool CancellationContext::Check() {
  if (status_.ok()) [[likely]] {
    status_ = DoCheck();
    return status_.ok();
  }
  return false;
}

std::unique_ptr<CancellationContext> CancellationContext::Make(
    absl::Duration cooldown_period,
    absl::AnyInvocable<absl::Status()> do_check_fn) {
  return std::make_unique<FnBasedCancellationContext>(cooldown_period,
                                                      std::move(do_check_fn));
}

}  // namespace arolla
