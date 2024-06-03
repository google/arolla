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
#ifndef AROLLA_CODEGEN_IO_MULTI_LOADER_H_
#define AROLLA_CODEGEN_IO_MULTI_LOADER_H_

// Low level library with common for codegen input loader utilities.
// Code is highly specific to generated code
// and not supposed to be used elsewhere.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::codegen::io {

// Special value to mark not requested slot.
constexpr size_t kSkippedOffset = std::numeric_limits<size_t>::max();

// Information required for clearing requested inputs when data is missed.
struct HierarchicalSingleValueClearInfo {
  // Range of requested slots as indices in requested_offsets.
  uint16_t range_begin = std::numeric_limits<uint16_t>::max();
  uint16_t range_end = 0;
  // NOTE: nodes are ordered in post order, so we always can put offsets
  // in such a way that each node has single range to clear.

  bool operator==(const HierarchicalSingleValueClearInfo& other) const {
    return range_begin == other.range_begin && range_end == other.range_end;
  }
};

// All requested inputs are organized in a tree hierarchy.
// Each leaf is input, which may or may not be requested to populate.
// Struct provides information for fast verification whenever
// any leaf was requested under the node.
// NOTE: we store data in C arrays instead of vectors to avoid indirection
// and accesses to the heap.
template <size_t kLeafCount, size_t kNodeCount>
struct HierarchicalRequestedInputsData {
  // offsets in the ::arolla::Frame for each leaf in post visit order.
  // kSkippedOffset means a not requested slot
  size_t leaf_frame_offsets[kLeafCount];

  // NOTE: all intermediate nodes are numbered consequently without leaves.
  // This is done in order to significantly reduce size of the memory layout.
  // The simplest way to compute "transformed" id for the intermediate node is
  // `original_node_id - #number of leaves with smaller id`.

  // true iff at least one leaf is requested under the node.
  // Separated from HierarchicalSingleValueClearInfo to have faster access.
  bool node_requested[kNodeCount - kLeafCount] = {false};
};

// For single values in addition we need to access the inputs that need
// to be cleaned if value in the node is missed.
// NOTE: we store data in C arrays instead of vectors to avoid indirection
// and accesses to the heap.
template <size_t kLeafCount, size_t kNodeCount>
struct HierarchicalSingleValueRequestedInputsData {
  template <class T>
  using value_type =
      ::arolla::OptionalValue<::arolla::proto::arolla_single_value_t<T>>;
  using size_type = ::arolla::DenseArrayShape;

  HierarchicalRequestedInputsData<kLeafCount, kNodeCount> common;

  // Range of slots for optional values.
  HierarchicalSingleValueClearInfo
      node_optional_clear_infos[kNodeCount - kLeafCount];

  // Offsets to the presence `bool` values of optional values and/or
  // Offsets to the size leaf values.
  // All offsets are stored at the beginning of the array.
  size_t requested_offsets[kLeafCount];

  // Range of slots for size values.
  // Size values are less often and may even not used at all.
  HierarchicalSingleValueClearInfo
      node_size_clear_infos[kNodeCount - kLeafCount];
};

// Separate structure for multi values inputs data that may be extended with
// more information.
template <size_t kLeafCount, size_t kNodeCount>
struct HierarchicalMultiValueRequestedInputsData {
  template <class T>
  using value_type =
      ::arolla::DenseArray<::arolla::proto::arolla_single_value_t<T>>;
  using size_type = ::arolla::DenseArray<::arolla::proto::arolla_size_t>;

  HierarchicalRequestedInputsData<kLeafCount, kNodeCount> common;
};

namespace multi_loader_internal {

struct HierarchicalRequestedInputsDataView {
  absl::Span<size_t> leaf_frame_offsets;
  absl::Span<bool> node_requested;
};

struct HierarchicalSingleValueRequestedInputsDataView {
  absl::Span<HierarchicalSingleValueClearInfo> node_optional_clear_infos;
  absl::Span<size_t> requested_offsets;
  absl::Span<HierarchicalSingleValueClearInfo> node_size_clear_infos;
};

void CreateHierarchicalRequestedInputs(
    // slots for leaves in post order
    // Intermediate nodes are not included. leaf_slots.size() == kLeafCount
    const std::vector<std::optional<TypedSlot>>& leaf_slots,
    // Children for each node in increasing order.
    // Nodes are numbered in post order.
    const std::vector<std::vector<size_t>>& tree,
    HierarchicalRequestedInputsDataView output);

void CreateHierarchicalSingleValueRequestedInputs(
    // slots for leaves in post order
    const std::vector<std::optional<TypedSlot>>& leaf_slots,
    // list of leaf ids corresponded to DenseArrayShape in sorted order
    const std::vector<size_t>& size_leaves,
    // Children for each node in increasing order.
    // Nodes are numbered in post order.
    const std::vector<std::vector<size_t>>& tree,
    HierarchicalSingleValueRequestedInputsDataView output);

template <size_t kLeafCount, size_t kNodeCount>
void CreateHierarchicalRequestedInputs(
    // slots for leaves in post order
    // Intermediate nodes are not included. leaf_slots.size() == kLeafCount
    const std::vector<std::optional<TypedSlot>>& leaf_slots,
    // Children for each node in increasing order.
    // Nodes are numbered in post order.
    const std::vector<std::vector<size_t>>& tree,
    HierarchicalRequestedInputsData<kLeafCount, kNodeCount>* inputs) {
  static_assert(kLeafCount < (1 << 16),
                "Too many input leaves for generated code");
  multi_loader_internal::CreateHierarchicalRequestedInputs(
      leaf_slots, tree,
      multi_loader_internal::HierarchicalRequestedInputsDataView{
          absl::MakeSpan(inputs->leaf_frame_offsets),
          absl::MakeSpan(inputs->node_requested)});
}

}  // namespace multi_loader_internal

// NOTE: we have two different ids for each node.
// 1. Leaf: node_id (all nodes in post order) and leaf_id (only leaves in
//   post order)
// 2. Intermediate node: node_id (all nodes in post order) and
//   intermediate_node_id (non leaf nodes in post order)

template <size_t kLeafCount, size_t kNodeCount>
void CreateHierarchicalSingleValueRequestedInputs(
    // slots for leaves in post order
    const std::vector<std::optional<TypedSlot>>& leaf_slots,
    // list of leaf ids corresponded to DenseArrayShape in sorted order
    // leaves are numbered in post order
    const std::vector<size_t>& size_leaves,
    // Children for each node in increasing order.
    // Nodes are numbered in post order.
    const std::vector<std::vector<size_t>>& tree,
    HierarchicalSingleValueRequestedInputsData<kLeafCount, kNodeCount>*
        inputs) {
  static_assert(kLeafCount < (1 << 16),
                "Too many input leaves for generated code");
  multi_loader_internal::CreateHierarchicalRequestedInputs(leaf_slots, tree,
                                                           &inputs->common);
  multi_loader_internal::CreateHierarchicalSingleValueRequestedInputs(
      leaf_slots, size_leaves, tree,
      multi_loader_internal::HierarchicalSingleValueRequestedInputsDataView{
          absl::MakeSpan(inputs->node_optional_clear_infos),
          absl::MakeSpan(inputs->requested_offsets),
          absl::MakeSpan(inputs->node_size_clear_infos)});
}

template <size_t kLeafCount, size_t kNodeCount>
void CreateHierarchicalMultiValueRequestedInputs(
    // slots for leaves in post order
    // Intermediate nodes are not included. leaf_slots.size() == kLeafCount
    const std::vector<std::optional<TypedSlot>>& leaf_slots,
    // Children for each node in increasing order.
    // Nodes are numbered in post order.
    const std::vector<std::vector<size_t>>& tree,
    HierarchicalMultiValueRequestedInputsData<kLeafCount, kNodeCount>* inputs) {
  static_assert(kLeafCount < (1 << 16),
                "Too many input leaves for generated code");
  multi_loader_internal::CreateHierarchicalRequestedInputs(leaf_slots, tree,
                                                           &inputs->common);
}

template <class T>
void ResizeRepeatedProtoField(google::protobuf::RepeatedPtrField<T>* field, size_t size) {
  arolla::proto::ResizeContainer(*field, size);
}

}  // namespace arolla::codegen::io

#endif  // AROLLA_CODEGEN_IO_MULTI_LOADER_H_
