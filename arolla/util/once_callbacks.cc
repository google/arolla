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
#include "arolla/util/once_callbacks.h"

#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"

namespace arolla {

// Node list structure when `head_ != nullptr`:
// - `next` pointers form a null-terminated singly-linked list in LIFO order
//   from `head_` to the end of the list.
// - The list is partitioned into contiguous clusters of nodes, each
//   associated with a `Token`.
// - The first node of each cluster has `prev != nullptr`: `head_->prev` points
//   to `head_` itself, and the first node of every subsequent cluster has
//   `prev` pointing to the last node of the preceding cluster.
// - Continuation nodes within a cluster have `prev == nullptr`.

void OnceCallbacks::Notify() && noexcept {
  for (auto* node = std::exchange(head_, nullptr); node != nullptr;) {
    std::exchange(node, node->next)->NotifyAndDestroy();
  }
}

OnceCallbacks::Token OnceCallbacks::Add(  // clang-format hint
    OnceCallback absl_nonnull callback) noexcept {
  auto* node = callback.node_.release();
  DCHECK(node != nullptr);
  DCHECK(node->prev == nullptr);
  DCHECK(node->next == nullptr);
  node->prev = node;
  if (head_ != nullptr) {
    node->next = head_;
    head_->prev = node;
  }
  head_ = node;
  return Token(node);
}

OnceCallbacks::Token OnceCallbacks::Add(OnceCallback absl_nonnull callback,
                                        Token&& existing_token) noexcept {
  auto* first = std::exchange(existing_token.node_, nullptr);
  if (first == nullptr) {
    return Add(std::move(callback));
  }
  DCHECK(head_ != nullptr);
  DCHECK(first->prev != nullptr);
  auto* node = callback.node_.release();
  DCHECK(node != nullptr);
  DCHECK(node->prev == nullptr);
  DCHECK(node->next == nullptr);
  node->next = first;
  if (first == head_) {
    DCHECK(first->prev == first);
    first->prev = nullptr;
    node->prev = node;
    head_ = node;
  } else {
    node->prev = std::exchange(first->prev, nullptr);
    DCHECK(node->prev->next == first);
    node->prev->next = node;
  }
  return Token(node);
}

OnceCallbacks OnceCallbacks::Remove(Token&& existing_token) noexcept {
  auto* first = std::exchange(existing_token.node_, nullptr);
  if (first == nullptr) {
    return OnceCallbacks();
  }
  DCHECK(head_ != nullptr);
  DCHECK(first->prev != nullptr);
  auto* last = first;
  auto* next = last->next;
  while (next != nullptr && next->prev == nullptr) {
    last = next;
    next = last->next;
  }
  if (first == head_) {
    DCHECK(first->prev == first);
    head_ = next;
    if (next != nullptr) {
      next->prev = next;
      last->next = nullptr;
    }
  } else {
    auto* prev = first->prev;
    DCHECK(prev->next == first);
    prev->next = next;
    first->prev = first;
    if (next != nullptr) {
      next->prev = prev;
      last->next = nullptr;
    }
  }
  OnceCallbacks result;
  result.head_ = first;
  return result;
}

}  // namespace arolla
