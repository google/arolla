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
#ifndef AROLLA_UTIL_ONCE_CALLBACKS_H_
#define AROLLA_UTIL_ONCE_CALLBACKS_H_

#include <concepts>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"

namespace arolla {

// A move-only, single-allocation, one-shot callback wrapper compatible with
// `OnceCallbacks`.
//
// Implements an rvalue-qualified invocation contract: `void() &&`.
//
// `std::move(callback).Notify()` calls the callback and immediately releases
// all associated resources.
class ABSL_NULLABILITY_COMPATIBLE ABSL_ATTRIBUTE_TRIVIAL_ABI
    OnceCallback final {
 public:
  OnceCallback() noexcept = default;
  OnceCallback(std::nullptr_t) noexcept {}  // NOLINT(*-explicit-constructor)

  // Constructs a `OnceCallback` from any invocable type.
  template <typename Callback>
    requires(std::invocable<std::decay_t<Callback> &&>)
  OnceCallback(Callback&& callback);  // NOLINT(*-explicit-constructor)

  // Move-only.
  OnceCallback(OnceCallback&&) noexcept = default;
  OnceCallback& operator=(OnceCallback&&) noexcept = default;

  // Returns `true` if the callback is non-null.
  explicit operator bool() const noexcept { return node_ != nullptr; }

  // Call the callback and immediately release the associated resources.
  void Notify() && noexcept;

 private:
  struct Node;
  std::unique_ptr<Node> node_;

  friend class OnceCallbacks;
};

// A container for managing a collection of one-shot notification callbacks.
//
// `OnceCallbacks` is not thread-safe and must be externally synchronized.
class ABSL_ATTRIBUTE_TRIVIAL_ABI OnceCallbacks {
 public:
  class Token;

  OnceCallbacks() noexcept = default;
  ~OnceCallbacks() noexcept;

  // Move-only.
  OnceCallbacks(OnceCallbacks&& other) noexcept;
  OnceCallbacks& operator=(OnceCallbacks&& other) noexcept;

  // Returns `true` if the collection is empty.
  bool empty() const noexcept { return head_ == nullptr; }

  // Adds `callback` to the collection and returns a token identifying it.
  Token Add(OnceCallback absl_nonnull callback) noexcept;

  // Adds `callback` to the collection; `existing_token` must be a valid token
  // from this collection (or empty). The existing callback(s) and `callback`
  // are combined under the newly returned token.
  Token Add(OnceCallback absl_nonnull callback,
            Token&& existing_token) noexcept;

  // Removes and returns the callbacks identified by `existing_token`.
  OnceCallbacks Remove(Token&& existing_token) noexcept;

  // Call the callbacks and immediately release the associated resources.
  void Notify() && noexcept;

  friend void swap(OnceCallbacks& a, OnceCallbacks& b) noexcept {
    std::swap(a.head_, b.head_);
  }

 private:
  OnceCallback::Node* absl_nullable head_ = nullptr;
};

// An identifier for a callback in a `OnceCallbacks` collection.
//
// - `Add()` returns a `Token` that identifies an entry in this collection.
//   A default-constructed `Token` is always valid but identifies no entry.
// - Moving or swapping `OnceCallbacks` transfers their active tokens to
//   the destination collection.
// - `callbacks.Add(..., existing_token)` and `callbacks.Remove(existing_token)`
//   invalidate the passed `existing_token`.
// - `std::move(callbacks).Notify()` removes and invalidates all tokens
//   associated with the collection.
class OnceCallbacks::Token {
 public:
  Token() noexcept = default;

  // Returns `true` if the token is not empty.
  //
  // IMPORTANT: This method doesn't check if the token is valid.
  explicit operator bool() const noexcept { return node_ != nullptr; }

 private:
  explicit Token(OnceCallback::Node* absl_nonnull node) noexcept
      : node_(node) {}

  OnceCallback::Node* absl_nullable node_ = nullptr;

  friend class OnceCallbacks;
};

// A type-erased, heap-allocated callback node in an intrusive list.
struct OnceCallback::Node {
  Node* absl_nullable prev = nullptr;
  Node* absl_nullable next = nullptr;
  virtual void NotifyAndDestroy() noexcept = 0;
  virtual ~Node() noexcept = default;
};

template <typename Callback>
  requires(std::invocable<std::decay_t<Callback> &&>)
OnceCallback::OnceCallback(Callback&& callback) {
  struct NodeImpl final : public Node {
    [[no_unique_address]] std::decay_t<Callback> callback;
    explicit NodeImpl(Callback&& cb) : callback(std::forward<Callback>(cb)) {}
    void NotifyAndDestroy() noexcept final {
      std::move(callback)();
      delete this;
    }
  };
  node_.reset(new NodeImpl(std::forward<Callback>(callback)));
}

inline void OnceCallback::Notify() && noexcept {
  DCHECK(node_ != nullptr);
  if (node_ != nullptr) [[likely]] {
    node_.release()->NotifyAndDestroy();
  }
}

inline OnceCallbacks::~OnceCallbacks() noexcept {
  for (auto* node = head_; node != nullptr;) {
    delete std::exchange(node, node->next);
  }
}

inline OnceCallbacks::OnceCallbacks(OnceCallbacks&& other) noexcept
    : head_(std::exchange(other.head_, nullptr)) {}

inline OnceCallbacks& OnceCallbacks::operator=(OnceCallbacks&& other) noexcept {
  OnceCallbacks tmp(std::move(other));
  swap(*this, tmp);
  return *this;
}

}  // namespace arolla

#endif  // AROLLA_UTIL_ONCE_CALLBACKS_H_
