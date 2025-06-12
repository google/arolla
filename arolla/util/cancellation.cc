// Copyright 2025 Google LLC
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
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"
#include "arolla/util/refcount_ptr.h"

namespace arolla {
namespace {

constexpr std::atomic_flag kCancellationFlagStub = ATOMIC_FLAG_INIT;

}  // namespace

thread_local constinit CancellationContext::ScopeGuard::ThreadLocalData
    CancellationContext::ScopeGuard::thread_local_data_ = {
        &kCancellationFlagStub};

struct CancellationContext::SubscriptionNode {
  absl::AnyInvocable<void() &&> callback;
  SubscriptionNode* prev = nullptr;
  SubscriptionNode* next = nullptr;
};

CancellationContext::~CancellationContext() noexcept {
  // Note: Locking the mutex `mx_` is not necessary in the destructor.
  for (auto* node = subscription_nodes_; node != nullptr;) {
    delete std::exchange(node, node->next);
  }
}

CancellationContextPtr CancellationContext::Make() {
  return CancellationContextPtr::Own(
      std::make_unique<CancellationContext>(PrivateConstructorTag()));
}

void CancellationContext::Cancel(absl::Status status) noexcept {
  DCHECK(!status.ok());
  if (status.ok() || Cancelled()) {
    return;
  }
  SubscriptionNode* node = nullptr;
  {
    absl::MutexLock lock(&mx_);
    if (status_.ok()) {
      status_ = std::move(status);
      node = std::exchange(subscription_nodes_, nullptr);
      cancelled_flag_.test_and_set(std::memory_order_relaxed);
    }
  }
  // Call the callbacks and immediately release the associated resources.
  while (node != nullptr) {
    std::move(node->callback)();
    delete std::exchange(node, node->next);
  }
}

absl::Status CancellationContext::GetStatus() const noexcept {
  absl::MutexLock lock(&mx_);
  return status_;
}

CancellationContext::Subscription CancellationContext::Subscribe(
    absl::AnyInvocable<void() &&>&& callback) {
  DCHECK(callback != nullptr);
  if (callback == nullptr) {
    return Subscription();
  }
  if (Cancelled()) {
    std::move(callback)();
    return Subscription();
  }
  auto node = std::make_unique<SubscriptionNode>();
  node->callback = std::move(callback);
  {
    absl::MutexLock lock(&mx_);
    if (!Cancelled()) {
      node->next = subscription_nodes_;
      if (subscription_nodes_ != nullptr) {
        subscription_nodes_->prev = node.get();
      }
      subscription_nodes_ = node.get();
      return Subscription(CancellationContextPtr::NewRef(this), node.release());
    }
  }
  std::move(node->callback)();
  return Subscription();
}

CancellationContext::ScopeGuard::ScopeGuard(
    /*absl_nullable*/ CancellationContextPtr cancellation_context) noexcept
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
    CancellationContext::ScopeGuard* /*absl_nullable*/ scope_guard) {
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

CancellationContext::Subscription::Subscription(
    /*absl_nullable*/ CancellationContextPtr&& cancellation_context,
    SubscriptionNode* node) noexcept
    : cancellation_context_(std::move(cancellation_context)), node_(node) {}

CancellationContext::Subscription::Subscription(Subscription&& other) noexcept {
  using std::swap;
  swap(cancellation_context_, other.cancellation_context_);
  swap(node_, other.node_);
}

CancellationContext::Subscription& CancellationContext::Subscription::operator=(
    Subscription&& other) noexcept {
  using std::swap;
  swap(cancellation_context_, other.cancellation_context_);
  swap(node_, other.node_);
  return *this;
}

CancellationContext::Subscription::~Subscription() noexcept {
  if (cancellation_context_ == nullptr || node_ == nullptr) {
    return;
  }
  if (cancellation_context_->Cancelled()) {
    return;
  }
  {
    absl::MutexLock lock(&cancellation_context_->mx_);
    if (cancellation_context_->Cancelled()) {
      return;
    }
    DCHECK(cancellation_context_->subscription_nodes_ != nullptr);
    if (node_->prev == nullptr) {
      cancellation_context_->subscription_nodes_ = node_->next;
    } else {
      node_->prev->next = node_->next;
    }
    if (node_->next != nullptr) {
      node_->next->prev = node_->prev;
    }
  }
  // Note: Release the mutex before triggering the destructor of
  // `node_->callback`.
  delete node_;
}

void CancellationContext::Subscription::Detach() && noexcept {
  cancellation_context_ = nullptr;
  node_ = nullptr;
}

}  // namespace arolla
