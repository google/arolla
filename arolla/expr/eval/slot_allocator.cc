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
#include "arolla/expr/eval/slot_allocator.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::expr::eval_internal {
namespace {

constexpr size_t kUnset = std::numeric_limits<size_t>::max();

TypedSlot UnsetSlot() {
  static const QTypePtr kNothingQType = GetNothingQType();
  return TypedSlot::UnsafeFromOffset(kNothingQType, kUnset);
}

bool IsUnsetSlot(const TypedSlot& slot) { return slot.byte_offset() == kUnset; }

}  // namespace

SlotAllocator::SlotAllocator(
    const PostOrder& post_order, FrameLayout::Builder& layout_builder,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    bool allow_reusing_input_slots)
    : post_order_(post_order),
      layout_builder_(layout_builder),
      last_usages_(post_order.nodes_size()),
      node_origin_(post_order.nodes_size()),
      node_result_slot_(post_order.nodes_size(), UnsetSlot()) {
  for (size_t i = 0; i < post_order.nodes_size(); ++i) {
    for (size_t j : post_order.dep_indices(i)) {
      last_usages_[j] = i;
    }
    last_usages_[i] = i;
    node_origin_[i] = i;
  }
  if (allow_reusing_input_slots) {
    for (size_t j = 0; j < post_order.nodes_size(); ++j) {
      const auto& node = post_order.node(j);
      if (node->is_leaf()) {
        node_result_slot_[j] = input_slots.at(node->leaf_key());
      }
    }
  }
}

TypedSlot SlotAllocator::AllocatePermanentSlot(size_t node_idx, QTypePtr type) {
  DCHECK_EQ(node_origin_[node_idx], node_idx);
  return ::arolla::AddSlot(type, &layout_builder_);
}

TypedSlot SlotAllocator::AllocateSlot(size_t node_idx, QTypePtr type) {
  DCHECK_EQ(node_origin_[node_idx], node_idx);
  auto& reusable_slots = reusable_slots_[type];
  std::optional<TypedSlot> slot;
  if (reusable_slots.empty()) {
    slot = ::arolla::AddSlot(type, &layout_builder_);
  } else {
    slot = reusable_slots.back();
    reusable_slots.pop_back();
  }
  node_result_slot_[node_idx] = *slot;
  return *std::move(slot);
}

void SlotAllocator::InheritSlotFrom(size_t node_idx, size_t from_node_idx) {
  DCHECK_EQ(node_origin_[node_idx], node_idx);
  from_node_idx = node_origin_[from_node_idx];
  node_origin_[node_idx] = from_node_idx;
  last_usages_[from_node_idx] =
      std::max(last_usages_[from_node_idx], last_usages_[node_idx]);
}

void SlotAllocator::ReleaseSlotsNotNeededAfter(size_t node_idx) {
  for (size_t dep_idx : post_order_.dep_indices(node_idx)) {
    dep_idx = node_origin_[dep_idx];
    if (last_usages_[dep_idx] == node_idx &&
        !IsUnsetSlot(node_result_slot_[dep_idx])) {
      reusable_slots_[node_result_slot_[dep_idx].GetType()].push_back(
          node_result_slot_[dep_idx]);
      node_result_slot_[dep_idx] = UnsetSlot();
    }
  }
}

std::vector<TypedSlot> SlotAllocator::ReusableSlots() const {
  std::vector<TypedSlot> result;
  for (const auto& [_, slots] : reusable_slots_) {
    result.insert(result.end(), slots.begin(), slots.end());
  }
  return result;
}

}  // namespace arolla::expr::eval_internal
