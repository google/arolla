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
#include "arolla/expr/eval/slot_allocator.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr::eval_internal {

SlotAllocator::SlotAllocator(
    const ExprNodePtr& root, FrameLayout::Builder& layout_builder,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    bool allow_reusing_leaves)
    : layout_builder_(&layout_builder),
      allow_reusing_leaves_(allow_reusing_leaves) {
  auto node_order = VisitorOrder(root);
  last_usages_.reserve(node_order.size());
  for (int64_t i = 0; i < node_order.size(); ++i) {
    const auto& node = node_order[i];
    for (const auto& d : node->node_deps()) {
      last_usages_[d->fingerprint()] = SlotUsage{node->fingerprint(), i};
    }
    last_usages_[node->fingerprint()] = SlotUsage{node->fingerprint(), i};
    if (allow_reusing_leaves_ && node->is_leaf()) {
      node_result_slot_.emplace(node->fingerprint(),
                                input_slots.at(node->leaf_key()));
    }
  }
}

TypedSlot SlotAllocator::AddSlotForNode(const ExprNodePtr& node, QTypePtr type,
                                        bool allow_recycled) {
  auto& reusable_slots = reusable_slots_[type];
  std::optional<TypedSlot> slot;
  if (!allow_recycled || reusable_slots.empty()) {
    slot = ::arolla::AddSlot(type, layout_builder_);
  } else {
    slot = reusable_slots.back();
    reusable_slots.pop_back();
  }
  node_result_slot_.emplace(node->fingerprint(), *slot);
  return *slot;
}

absl::Status SlotAllocator::ExtendSlotLifetime(const ExprNodePtr& of,
                                               const ExprNodePtr& to) {
  if (to->fingerprint() == of->fingerprint()) {
    return absl::OkStatus();
  }
  ExprNodePtr of_origin = of;
  if (node_origin_.contains(of->fingerprint())) {
    of_origin = node_origin_.at(of->fingerprint());
    // We must always use `of_origin` instead of `of` so we remove `of` from
    // last_usages to avoid accidental usage.
    last_usages_.erase(of->fingerprint());
  }
  node_origin_[to->fingerprint()] = of_origin;
  if (!last_usages_.contains(to->fingerprint())) {
    return absl::InternalError(
        absl::StrFormat("missing last usage for node %s", GetDebugSnippet(to)));
  }
  if (!last_usages_.contains(of_origin->fingerprint())) {
    return absl::InternalError(absl::StrFormat("missing last usage for node %s",
                                               GetDebugSnippet(of_origin)));
  }
  if (last_usages_.at(to->fingerprint()).node_number >
      last_usages_.at(of_origin->fingerprint()).node_number) {
    last_usages_[of_origin->fingerprint()] = last_usages_.at(to->fingerprint());
  }
  return absl::OkStatus();
}

absl::Status SlotAllocator::ReleaseSlotsNotNeededAfter(
    const ExprNodePtr& node) {
  absl::flat_hash_set<Fingerprint> processed_deps;
  for (ExprNodePtr dep : node->node_deps()) {
    if (node_origin_.contains(dep->fingerprint())) {
      dep = node_origin_.at(dep->fingerprint());
    }
    const auto& [_, inserted] = processed_deps.insert(dep->fingerprint());
    if (!inserted) {
      continue;
    }
    auto last_usage_it = last_usages_.find(dep->fingerprint());
    if (last_usage_it == last_usages_.end()) {
      return absl::InternalError(absl::StrFormat(
          "missing last usage for node %s", GetDebugSnippet(dep)));
    }
    if ((dep->is_op() || (dep->is_leaf() && allow_reusing_leaves_)) &&
        last_usage_it->second.node_fingerprint == node->fingerprint()) {
      auto slot_it = node_result_slot_.find(dep->fingerprint());
      if (slot_it == node_result_slot_.end()) {
        return absl::InternalError(absl::StrFormat(
            "missing slot information for node %s", GetDebugSnippet(dep)));
      }
      reusable_slots_[slot_it->second.GetType()].push_back(slot_it->second);
      node_result_slot_.erase(slot_it);
      last_usages_.erase(last_usage_it);
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
