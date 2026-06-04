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
#ifndef AROLLA_EXPR_EVAL_SLOT_USAGE_TRACKER_H_
#define AROLLA_EXPR_EVAL_SLOT_USAGE_TRACKER_H_

#include <cstddef>
#include <string>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_map.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {

// A very Expr compiler specific wrapper around FrameLayout::Builder. It allows
// to reuse slots during expr compilation.
//
// Usage restrictions:
//   1. SlotAllocator works only for the compilation corresponding to
//      the `post_order` passed to the constructor.
//   2. Nodes must be processed in the same order as in `post_order`.
//   3. An output slot for a node can be allocated using one of the following
//      methods: `AllocateSlot()`, `AllocatePermanentSlot()`, or
//      `InheritSlotFrom()`. This allocation may be done at most once.
//   4. The `ReleaseSlotsNotNeededAfter()` method must not be used before
//      allocating a slot for the same node.
//
class SlotAllocator {
 public:
  // Initialize SlotAllocator for compilation the given expression (specified as
  // `post_order`). During the initialization SlotAllocator will collect
  // tentative last usages for each expr node.
  //
  // NOTE: correspondence between the expression leaves and `input_slots` must
  // be verified externally.
  SlotAllocator(  // clang-format hint
      const PostOrder& post_order ABSL_ATTRIBUTE_LIFETIME_BOUND,
      FrameLayout::Builder& layout_builder ABSL_ATTRIBUTE_LIFETIME_BOUND,
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      bool allow_reusing_input_slots);

  // Assigns and returns a permanent slot for `node_idx`. The slot will not be
  // released or reused.
  TypedSlot AllocatePermanentSlot(size_t node_idx, QTypePtr type);

  // Assigns and returns a slot for `node_idx`.
  TypedSlot AllocateSlot(size_t node_idx, QTypePtr type);

  // Makes `node_idx` inherit the slot from node `from_node_idx`.
  void InheritSlotFrom(size_t node_idx, size_t from_node_idx);

  // Releases the slots last used by node.
  //
  // Must only be called after all the preceding nodes have already been
  // processed, and `Allocate*Slot()` or `InheritSlotFrom()` has been called for
  // `node_idx`.
  void ReleaseSlotsNotNeededAfter(size_t node_idx);

  // Returns a current list of reusable slots. The list may be useful for
  // cleanup operations at the end of the program. However the returned slots
  // must not be used directly as it will conflict with AddSlotForNode.
  std::vector<TypedSlot> ReusableSlots() const;

 private:
  const PostOrder& post_order_;
  FrameLayout::Builder& layout_builder_;

  absl::flat_hash_map<QTypePtr, std::vector<TypedSlot>> reusable_slots_;

  // Last (known) usage for the node with given index. It can be extended
  // dynamically with InheritSlotForNode.
  absl::FixedArray<size_t, 0> last_usages_;

  // Index of the node that "owns" the output slot for the node with
  // given index. When a node inherits a slot from another node,
  // the origin index is updated to the origin index of the inherited slot.
  absl::FixedArray<size_t, 0> node_origin_;

  // Output slot for the node with the given index. Stores only reusable slots
  // (all non-reusable slots have to be in an "unset" state).
  absl::FixedArray<TypedSlot, 0> node_result_slot_;
};

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_SLOT_USAGE_TRACKER_H_
