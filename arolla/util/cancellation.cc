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
#include "arolla/util/cancellation.h"

#include <atomic>
#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"

namespace arolla {
namespace {

constexpr std::atomic_flag kCancellationFlagStub = ATOMIC_FLAG_INIT;

}  // namespace

thread_local constinit CancellationContext::ScopeGuard::ThreadLocalData
    CancellationContext::ScopeGuard::thread_local_data_ = {
        &kCancellationFlagStub};

CancellationContextPtr CancellationContext::Make() {
  return CancellationContextPtr::Own(
      std::make_unique<CancellationContext>(PrivateConstructorTag()));
}

void CancellationContext::Cancel(absl::Status status) noexcept {
  DCHECK(!status.ok());
  if (status.ok() || Cancelled()) {
    return;
  }
  {
    absl::MutexLock lock(&mx_);
    if (status_.ok()) {
      status_ = std::move(status);
      cancelled_flag_.test_and_set(std::memory_order_relaxed);
    }
  }
}

absl::Status CancellationContext::GetStatus() const noexcept {
  absl::MutexLock lock(&mx_);
  return status_;
}

CancellationContext::ScopeGuard::ScopeGuard(
    absl::Nullable<CancellationContextPtr> cancellation_context) noexcept
    : cancellation_context_(std::move(cancellation_context)),
      previous_scope_guard_(thread_local_data_.scope_guard) {
  UpdateThreadLocalData(this);
}

CancellationContext::ScopeGuard::~ScopeGuard() noexcept {
  CHECK_EQ(thread_local_data_.scope_guard, this)
      << "CancellationContext::ScopeGuard: Scope nesting invariant "
         "violated!";
  UpdateThreadLocalData(previous_scope_guard_);
}

void CancellationContext::ScopeGuard::UpdateThreadLocalData(
    absl::Nullable<CancellationContext::ScopeGuard*> scope_guard) {
  if (scope_guard != nullptr && scope_guard->cancellation_context_ != nullptr) {
    thread_local_data_ = {
        &scope_guard->cancellation_context_->cancelled_flag_,
        scope_guard->cancellation_context_.get(),
        scope_guard,
    };
  } else {
    thread_local_data_ = {
        &kCancellationFlagStub,
        nullptr,
        scope_guard,
    };
  }
}

}  // namespace arolla
