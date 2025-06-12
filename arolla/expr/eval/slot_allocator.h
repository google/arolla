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

#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "arolla/expr/expr_node.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::eval_internal {

// A very Expr compiler specific wrapper around FrameLayout::Builder. It allows
// to reuse slots during expr compilation.
//
// Usage restrictions:
//   1. SlotAllocator works only for compilation of expr passed to the
//   constructor.
//   2. Nodes must be processed in VisitorOrder.
//   3. For each node user is responsible to call AddSlotForNode and/or
//   ExtendSlotLifetime and ReleaseSlotsNotNeededAfter in proper order (see the
//   function comments).
//
class SlotAllocator {
 public:
  // Initialize SlotAllocator for compilation the `root` expression. During the
  // initialization SlotAllocator will collect tentative last usages for each
  // expr node, that can be later modified by calling ExtendSlotLifetime.
  // NOTE: correspondence between `root` leaves and `input_slots` must be
  // verified externally.
  SlotAllocator(const ExprNodePtr& root, FrameLayout::Builder& layout_builder,
                const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
                bool allow_reusing_leaves);

  // Creates or returns a reused slot of type `type`. Always creates a new slot
  // if `allow_recycled=false`.
  TypedSlot AddSlotForNode(const ExprNodePtr& node, QTypePtr type,
                           bool allow_recycled);

  // Extends lifetime of the resulting slot of the node `of` to the resulting
  // slot of the node `to`. Must never be called after
  // ReleaseSlotsNotNeededAfter for the current last usage of `of`.
  absl::Status ExtendSlotLifetime(const ExprNodePtr& of, const ExprNodePtr& to);

  // Releases all the slots last used by `node`.
  absl::Status ReleaseSlotsNotNeededAfter(const ExprNodePtr& node);

  // Returns a current list of reusable slots. The list may be useful for
  // cleanup operations at the end of the program. However the returned slots
  // must not be used directly as it will conflict with AddSlotForNode.
  std::vector<TypedSlot> ReusableSlots() const;

 private:
  struct SlotUsage {
    // Fingerprint of expr that uses slot.
    Fingerprint node_fingerprint;
    // Position of the usage in VisitorOrder node sequence.
    int64_t node_number;
  };

  FrameLayout::Builder* layout_builder_;
  absl::flat_hash_map<QTypePtr, std::vector<TypedSlot>> reusable_slots_;
  // Last (known) usage for the node with given fingerprint. It can be exended
  // dynamically with ExtendSlotLifetime.
  // The usage may not exist for:
  //   - nodes where the corresponding slot is already released.
  //   - non-origin nodes. Look for node_origin_ before accessing.
  absl::flat_hash_map<Fingerprint, SlotUsage> last_usages_;
  // Output slot for the node with given fingerprint. Does not contain slots
  // created not by SlotAllocator (i.e. leaves, outputs).
  absl::flat_hash_map<Fingerprint, TypedSlot> node_result_slot_;
  // The node that initially creates the output slot for the node with given
  // fingerprint. Must be populated only for nodes that return (sub)slots of
  // their child nodes. For example for expression `M.core.has(M.core.get_nth(5,
  // M.core.make_tuple(...)))` node_origin_ will contain `make_tuple` for both
  // `has` and `get_nth` nodes.
  absl::flat_hash_map<Fingerprint, ExprNodePtr> node_origin_;
  // Slots for leaves are coming from outside, and some other expressions may
  // rely on their values. So we reuse them only if allowed explicitly.
  bool allow_reusing_leaves_;
};

}  // namespace arolla::expr::eval_internal

#endif  // AROLLA_EXPR_EVAL_SLOT_USAGE_TRACKER_H_
