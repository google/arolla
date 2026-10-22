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
#ifndef AROLLA_CODEGEN_IO_MULTI_LOADER_H_
#define AROLLA_CODEGEN_IO_MULTI_LOADER_H_

// Low level library with common for codegen input loader utilities.
// Code is highly specific to generated code
// and not supposed to be used elsewhere.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace arolla::codegen::io {

// Special value to mark not requested slot.
inline constexpr size_t kSkippedOffset = std::numeric_limits<size_t>::max();

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
  template <class T>
  using value_type =
      ::arolla::OptionalValue<::arolla::proto::arolla_single_value_t<T>>;
  using size_type = ::arolla::DenseArrayShape;

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

using multi_loader_internal::CreateHierarchicalRequestedInputs;

// Reconstructs a post-order protopath tree (children list for each node) from
// the number of children of each node in post-order traversal.
std::vector<std::vector<size_t>> BuildProtopathTreeFromChildrenCounts(
    absl::Span<const uint16_t> children_counts);

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

// Builds a DenseArray<T> for a single leaf of a generated multi-value
// InputLoader on top of `Buffer<T>::Builder` and `bitmap::AlmostFullBuilder`.
//
// Generated loaders contain one function per leaf (thousands of them for big
// models), so buffer allocation, DenseArray finalization and cleanup are
// deliberately kept out of line: they are instantiated once per value type
// instead of being inlined into every leaf. Only the per-value insertion stays
// inline.
template <typename T>
class DenseArrayLeafBuilder {
 public:
  using Inserter = typename Buffer<T>::Inserter;

  ABSL_ATTRIBUTE_NOINLINE DenseArrayLeafBuilder(size_t size,
                                                RawBufferFactory* factory)
      : bitmap_bldr_(size, factory) {
    // The values builder is constructed on the stack and moved into the member
    // on purpose: it lets the compiler inline `Buffer<T>::Builder`'s
    // constructor (its temporaries stay in registers), which it refuses to do
    // when the member is constructed in place.
    ValuesBuilder values_bldr(size, factory);
    ::new (static_cast<void*>(&values_bldr_))
        ValuesBuilder(std::move(values_bldr));
  }

  ~DenseArrayLeafBuilder() {
    if (ABSL_PREDICT_FALSE(!built_)) {
      DestroyBuildersOutOfLine();
    }
  }

  DenseArrayLeafBuilder(const DenseArrayLeafBuilder&) = delete;
  DenseArrayLeafBuilder& operator=(const DenseArrayLeafBuilder&) = delete;

  // Returns an inserter for values. Missing values must be skipped with
  // `Inserter::SkipN(1)` and marked with `AddMissed`.
  Inserter GetInserter() { return values_bldr_.GetInserter(); }

  // Marks the value with the given id as missing.
  void AddMissed(int64_t id) { bitmap_bldr_.AddMissed(id); }

  // Builds the result and moves it into `*output`. The builder must not be
  // used afterwards.
  ABSL_ATTRIBUTE_NOINLINE void BuildInto(DenseArray<T>* output) && {
    DCHECK(!built_);
    *output = DenseArray<T>{std::move(values_bldr_).Build(),
                            std::move(bitmap_bldr_).Build()};
    DestroyBuilders();
  }

 private:
  using ValuesBuilder = typename Buffer<T>::Builder;

  void DestroyBuilders() {
    values_bldr_.~ValuesBuilder();
    bitmap_bldr_.~AlmostFullBuilder();
    built_ = true;
  }

  // Cold path of the destructor: `BuildInto` was never called.
  ABSL_ATTRIBUTE_NOINLINE void DestroyBuildersOutOfLine() { DestroyBuilders(); }

  // The builders are destroyed by `BuildInto` (out of line) rather than by the
  // implicit member destruction, so that the destructor of this class only
  // needs to check `built_` inline. Unions prevent the implicit destruction.
  union {
    ValuesBuilder values_bldr_;
  };
  union {
    bitmap::AlmostFullBuilder bitmap_bldr_;
  };
  bool built_ = false;
};

// The out-of-line parts of DenseArrayLeafBuilder for the common value types are
// compiled once in multi_loader.cc instead of in every generated loader.
extern template class DenseArrayLeafBuilder<bool>;
extern template class DenseArrayLeafBuilder<int32_t>;
extern template class DenseArrayLeafBuilder<int64_t>;
extern template class DenseArrayLeafBuilder<float>;
extern template class DenseArrayLeafBuilder<double>;
extern template class DenseArrayLeafBuilder<Bytes>;
extern template class DenseArrayLeafBuilder<Text>;

}  // namespace arolla::codegen::io

#endif  // AROLLA_CODEGEN_IO_MULTI_LOADER_H_
