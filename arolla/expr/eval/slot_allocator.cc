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
#include <vector>

#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr_debug_string.h"
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
    bool allow_reusing_leaves)
    : post_order_(post_order),
      layout_builder_(layout_builder),
      node_result_slot_(post_order.nodes_size(), UnsetSlot()),
      node_origin_(post_order.nodes_size(), kUnset),
      allow_reusing_leaves_(allow_reusing_leaves) {
  last_usages_.resize(post_order.nodes_size());
  for (size_t i = 0; i < post_order.nodes_size(); ++i) {
    for (size_t j : post_order.dep_indices(i)) {
      last_usages_[j] = i;
    }
    last_usages_[i] = i;
  }
  for (size_t j = 0; j < post_order.nodes_size(); ++j) {
    const auto& node = post_order.node(j);
    if (node->is_leaf()) {
      node_result_slot_[j] = input_slots.at(node->leaf_key());
    }
  }
}

TypedSlot SlotAllocator::AddSlotForNode(size_t node_idx, QTypePtr type,
                                        bool allow_recycled) {
  auto& reusable_slots = reusable_slots_[type];
  std::optional<TypedSlot> slot;
  if (!allow_recycled || reusable_slots.empty()) {
    slot = ::arolla::AddSlot(type, &layout_builder_);
  } else {
    slot = reusable_slots.back();
    reusable_slots.pop_back();
  }
  node_result_slot_[node_idx] = *slot;
  return *slot;
}

absl::Status SlotAllocator::ExtendSlotLifetime(size_t of_idx, size_t to_idx) {
  if (to_idx == of_idx) {
    return absl::OkStatus();
  }
  size_t of_origin_idx = of_idx;
  if (node_origin_[of_idx] != kUnset) {
    of_origin_idx = node_origin_[of_idx];
    // We must always use `of_origin` instead of `of` so we remove `of` from
    // last_usages to avoid accidental usage.
    last_usages_[of_idx] = kUnset;
  }
  node_origin_[to_idx] = of_origin_idx;
  if (last_usages_[to_idx] == kUnset) [[unlikely]] {
    return absl::InternalError(
        absl::StrFormat("missing last usage for node %s",
                        GetDebugSnippet(post_order_.node(to_idx))));
  }
  if (last_usages_[of_origin_idx] == kUnset) [[unlikely]] {
    return absl::InternalError(
        absl::StrFormat("missing last usage for node %s",
                        GetDebugSnippet(post_order_.node(of_origin_idx))));
  }
  if (last_usages_[to_idx] > last_usages_[of_origin_idx]) {
    last_usages_[of_origin_idx] = last_usages_[to_idx];
  }
  return absl::OkStatus();
}

absl::Status SlotAllocator::ReleaseSlotsNotNeededAfter(size_t node_idx) {
  const auto dep_indices = post_order_.dep_indices(node_idx);
  absl::FixedArray<size_t> dep_indices_to_process(dep_indices.begin(),
                                                  dep_indices.end());
  for (size_t& idx : dep_indices_to_process) {
    if (node_origin_[idx] != kUnset) {
      idx = node_origin_[idx];
    }
  }
  std::sort(dep_indices_to_process.begin(), dep_indices_to_process.end());
  size_t last_idx = kUnset;
  for (size_t dep_idx : dep_indices_to_process) {
    if (dep_idx == last_idx) {
      continue;
    }
    last_idx = dep_idx;
    const auto& dep = post_order_.node(dep_idx);
    if (last_usages_[dep_idx] == kUnset) [[unlikely]] {
      return absl::InternalError(absl::StrFormat(
          "missing last usage for node %s", GetDebugSnippet(dep)));
    }
    if ((dep->is_op() || (dep->is_leaf() && allow_reusing_leaves_)) &&
        last_usages_[dep_idx] == node_idx) {
      if (IsUnsetSlot(node_result_slot_[dep_idx])) [[unlikely]] {
        return absl::InternalError(absl::StrFormat(
            "missing slot information for node %s", GetDebugSnippet(dep)));
      }
      reusable_slots_[node_result_slot_[dep_idx].GetType()].push_back(
          node_result_slot_[dep_idx]);
      node_result_slot_[dep_idx] = UnsetSlot();
      last_usages_[dep_idx] = kUnset;
    }
  }
  return absl::OkStatus();
}

std::vector<TypedSlot> SlotAllocator::ReusableSlots() const {
  std::vector<TypedSlot> result;
  for (const auto& [_, slots] : reusable_slots_) {
    result.insert(result.end(), slots.begin(), slots.end());
  }
  return result;
}

}  // namespace arolla::expr::eval_internal
