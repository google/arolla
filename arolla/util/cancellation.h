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
#ifndef AROLLA_UTIL_CANCELLATION_H_
#define AROLLA_UTIL_CANCELLATION_H_

#include <atomic>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "arolla/util/api.h"
#include "arolla/util/refcount_ptr.h"

namespace arolla {

// Cancellation context is a primitive for signalling cancellation to multiple
// control flows.
//
// All methods of this class are thread-safe.
class AROLLA_API CancellationContext final : public RefcountedBase {
  struct PrivateConstructorTag {};

 public:
  class [[nodiscard]] ScopeGuard;
  class [[nodiscard]] Subscription;

  // Returns a new cancellation context.
  static RefcountPtr<CancellationContext> Make();

  // Private constructor.
  explicit CancellationContext(PrivateConstructorTag) noexcept {}

  ~CancellationContext() noexcept;

  // Disable copy and move semantics.
  CancellationContext(const CancellationContext&) = delete;
  CancellationContext& operator=(const CancellationContext&) = delete;

  // Returns `true` if the context has been cancelled.
  bool Cancelled() const noexcept {
    return ABSL_PREDICT_FALSE(cancelled_flag_.test(std::memory_order_relaxed));
  }

  // Returns the stored status.
  absl::Status GetStatus() const noexcept;

  // Cancels the context.
  void Cancel(absl::Status status = absl::CancelledError("cancelled")) noexcept;

  // Subscribes a callback for cancellation notification and returns
  // a subscription handle.
  //
  // Example:
  //
  //   auto cancellation_context = CancellationContext::Make();
  //   auto notification = std::make_shared<absl::Notification>();
  //   cancellation_context->Subscribe([notification] {
  //       notification->Notify();
  //   }).Detach();
  //
  // IMPORTANT: Please use the subscription mechanism with caution.
  //
  // The callback MUST be prepared to be invoked from any thread and
  // potentially even after the `Subscription` handle has been destroyed, as
  // the invocation might be scheduled asynchronously. It is strongly advised to
  // use `std::shared_ptr<T>` or `RefcountPtr<T>` for managing shared state.
  //
  // Furthermore, the callback MUST NOT own the corresponding
  // `CancellationContext`, directly or indirectly; otherwise, it might cause
  // circular ownership.
  //
  Subscription Subscribe(absl::AnyInvocable<void() &&>&& callback);

 private:
  struct SubscriptionNode;

  std::atomic_flag cancelled_flag_ = ATOMIC_FLAG_INIT;
  absl::Status status_ ABSL_GUARDED_BY(mx_);
  SubscriptionNode* subscription_nodes_ ABSL_GUARDED_BY(mx_) = nullptr;
  mutable absl::Mutex mx_;
};

using CancellationContextPtr = RefcountPtr<CancellationContext>;

// ScopeGuard is a RAII mechanism for CancellationContext. Its constructor
// sets the provided cancellation context as the "current" for the current
// thread, and its destructor restores the previous cancellation context;
// so if ScopeGuard is a function-local variable (which it almost always
// should be), the cancellation context is guaranteed to be restored when
// control leaves the scope.
//
// Example:
//
//   absl::StatusOr<Result> Task(
//       CancellationContextPtr cancellation_context, ...) {
//     // Sets the "current" cancellation context for the lifetime
//     // of this function.
//     CancellationContext::ScopeGuard cancellation_scope(
//         std::move(cancellation_context));
//     ...
//     RETURN_IF_ERROR(CheckCancellation());
//     ...
//     ASSIGN_OR_RETURN(auto sub_result, SubTask(...));
//     ...
//   }
//
//   absl::StatusOr<SubResult> SubTask(...) {
//     ...
//     // Subtasks can access the "current" cancellation context implicitly.
//     RETURN_IF_ERROR(CheckCancellation());
//     ...
//   }
//
//   // Cancellation context can be shared with tasks running on other threads.
//   auto cancellation_context =
//       CancellationContext::ScopeGuard::current_cancellation_context();
//   auto future = std::async(Task, cancellation_context, ...);
//   ...
//   cancellation_context->Cancel();
//
// IMPORTANT:
//  * ScopeGuards must be destroyed in the reverse order of their construction.
//  * Construction and destruction must occur on the same thread, as they use
//    thread_local storage.
//
class AROLLA_API CancellationContext::ScopeGuard final {
 public:
  // Sets the provided cancellation context as the "current" for the current
  // thread.
  explicit ScopeGuard(/*absl_nullable*/ CancellationContextPtr
                          cancellation_context =
                              CancellationContext::Make()) noexcept;

  // Restores the previous cancellation context.
  ~ScopeGuard() noexcept;

  // Disable copy and move semantics.
  ScopeGuard(const ScopeGuard&) = delete;
  ScopeGuard& operator=(const ScopeGuard&) = delete;

  // Returns a raw pointer to the current cancellation context.
  static inline CancellationContext* /*absl_nullable*/
  current_cancellation_context() noexcept {
    return thread_local_data_.cancellation_context;
  }

  // Returns a raw pointer to the cancellation context of this scope guard.
  inline CancellationContext* /*absl_nullable*/ cancellation_context()
      const noexcept {
    return cancellation_context_.get();
  }

 private:
  /*absl_nullable*/ CancellationContextPtr cancellation_context_;
  ScopeGuard* /*absl_nullable*/ previous_scope_guard_;

  static void UpdateThreadLocalData(ScopeGuard* /*absl_nullable*/ scope_guard);

  struct ThreadLocalData {
    // Note: `nonull` for `cancelled_flag` is essential for performance,
    // because it allows using `test` without a null check.
    const std::atomic_flag* /*absl_nonnull*/ cancelled_flag;
    CancellationContext* /*absl_nullable*/ cancellation_context;
    ScopeGuard* /*absl_nullable*/ scope_guard;
  };
  // Note: `constinit` is essential for performance. Without it, a hidden guard
  // variable for initialization might be introduced.
  static thread_local constinit ThreadLocalData thread_local_data_;

  friend bool Cancelled() noexcept;
  friend absl::Status CheckCancellation() noexcept;
};

// Subscription "handle" is a RAII mechanism for a cancellation callback
// subscription. Its destructor removes the callback registration, releasing
// the corresponding resources. However, it's important to note that
// the callback invocation might still happen if it has already been scheduled.
//
// The handle provides a `Detach()` method, calling which disables
// unregistration, leaving the callback registration indefinite.
//
class AROLLA_API CancellationContext::Subscription final {
 public:
  // Default constructor.
  Subscription() noexcept = default;

  // Private constructor (depends on a private type).
  Subscription(/*absl_nullable*/ CancellationContextPtr&& cancellation_context,
               SubscriptionNode* node) noexcept;

  ~Subscription() noexcept;

  // Disable copy semantics.
  Subscription(const Subscription&) = delete;
  Subscription& operator=(const Subscription&) = delete;

  // Enable move semantics.
  Subscription(Subscription&& other) noexcept;
  Subscription& operator=(Subscription&& other) noexcept;

  // Makes the callback registration indefinite.
  void Detach() && noexcept;

 private:
  /*absl_nullable*/ CancellationContextPtr cancellation_context_;
  SubscriptionNode* /*absl_nullable*/ node_ = nullptr;
};

// A convenience wrapper for `!current_cancellation_context->Cancelled()`.
//
// IMPORTANT: The implementation uses thread_local storage.
inline bool Cancelled() noexcept {
  return CancellationContext::ScopeGuard::thread_local_data_.cancelled_flag
      ->test(std::memory_order_relaxed);
}

// A convenience wrapper for `current_cancellation_context->GetStatus()`.
//
// IMPORTANT: The implementation uses thread_local storage.
inline absl::Status CheckCancellation() noexcept {
  const auto& data = CancellationContext::ScopeGuard::thread_local_data_;
  if (data.cancelled_flag->test(std::memory_order_relaxed)) [[unlikely]] {
    DCHECK_NE(data.cancellation_context, nullptr);
    return data.cancellation_context->GetStatus();
  }
  return absl::OkStatus();
}

}  // namespace arolla

#endif  // AROLLA_UTIL_CANCELLATION_CONTEXT_H_
