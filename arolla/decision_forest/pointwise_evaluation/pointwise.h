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
#ifndef AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_POINTWISE_H_
#define AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_POINTWISE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <type_traits>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/util/status_macros_backport.h"

// There are two ways to use library: single tree or boosted trees evaluation.
// Single tree evaluation using PRED algorithms.
// Boosted trees evaluation use ideas from VPRED.
// TODO: use QuickScorer for the first few layers
// See pages.di.unipi.it/rossano/wp-content/uploads/sites/7/2015/11/sigir15.pdf
// for the details.
// In short, multiple trees evaluated layer by layer. This gives a processor
// chance to reduce data hazzard and access memory more in parallel.
// It is a bit different from VPRED evaluating one tree on several data points.
//
// User needs first to compile his trees and later use compiled version
// for evaluation. Compilation converts data to internal compact format.
// NodeTest template parameter control decision tree traversal flow.
// It must implement:
// bool operator()(const FeatureContainer& values) const
// true will signal to move to the left_id, otherwise to the right_id
//
// Single tree usage example:
// Compilation:
//   PredictorCompiler<float, LessTest<float>> compiler(3);
//   CHECK_OK(compiler.SetNode(0, 2, 1, {0, 13.0}));
//   CHECK_OK(compiler.SetLeaf(1, 0.0));
//   CHECK_OK(compiler.SetLeaf(2, 1.0));
//   auto eval = compiler.Compile().ConsumeValueOrDie();
// Evaluation:
//   // Note: Predict() accepts a container with begin() and accesses each
//   // feature by *(container.begin() + feature_id).
//   CHECK_EQ(eval.Predict(std::vector<float>{15.0}), 0.0);
//   CHECK_EQ(eval.Predict(std::vector<float>{11.0}), 1.0);
//
// Boosted tree usage example (float values and double-precision accumulator):
// Compilation:
//   BoostedPredictorCompiler<
//       float, LessTest<float>, std::plus<double>> boosted_compiler;
//   auto compiler1 = boosted_compiler.AddTree(3);
//   CHECK_OK(compiler1.SetNode(0, 2, 1, {0, 13.0}));
//   CHECK_OK(compiler1.SetLeaf(1, 0.0));
//   CHECK_OK(compiler1.SetLeaf(2, 1.0));
//   auto compiler2 = boosted_compiler.AddTree(3);
//   CHECK_OK(compiler2.SetNode(0, 2, 1, {0, 4.0}));
//   CHECK_OK(compiler2.SetLeaf(1, 3.0));
//   CHECK_OK(compiler2.SetLeaf(2, 5.0));
//   auto eval = compiler.Compile().ConsumeValueOrDie();
// Evaluation:
//   CHECK_EQ(eval.Predict(std::vector<float>{5.0}), 1.0 + 3.0);
//
// Boosted evaluation is approximately twice faster than single tree evaluation.
// Benchmarks are in decision_forest/pointwise_evaluation:benchmarks
// (run with --benchmarks=BM_LowLevel).

namespace arolla {

namespace internal {

struct NodeId {
  static NodeId Split(int32_t split_node_id) { return {split_node_id}; }
  static NodeId Leaf(int32_t adjustment_id) { return {~adjustment_id}; }

  bool is_leaf() const { return val < 0; }
  int32_t split_node_id() const {
    DCHECK(!this->is_leaf());
    return val;
  }
  int32_t adjsutment_id() const {
    DCHECK(this->is_leaf());
    return ~val;
  }

  int32_t val;
};

template <class NodeTest>
struct CompactCondition {
  NodeTest test;
  // Node ids correspond to boolean result of the test, i. e.
  // next_id will be set to next_node_ids[test_result]
  std::array<NodeId, 2> next_node_ids;
};

template <class OutT, class NodeTest>
struct CompactDecisionTree {
  std::vector<CompactCondition<NodeTest>> splits;
  std::vector<OutT> adjustments;

  NodeId RootNodeId() const {
    return splits.empty() ? NodeId::Leaf(0) : NodeId::Split(0);
  }
};

template <class OutT, class NodeTest>
struct SingleTreeCompilationImpl {
  explicit SingleTreeCompilationImpl(size_t node_cnt)
      : nodes_(node_cnt), node_used_(node_cnt), node_used_as_child_(node_cnt) {
    if (node_cnt > 0) {
      node_used_as_child_[0] = true;
    }
  }

  absl::Status SetNode(size_t node_id, size_t left_id, size_t right_id,
                       const NodeTest& test) {
    RETURN_IF_ERROR(TestNode(node_id, &node_used_));
    RETURN_IF_ERROR(TestNode(left_id, &node_used_as_child_));
    RETURN_IF_ERROR(TestNode(right_id, &node_used_as_child_));
    nodes_[node_id].left_id = left_id;
    nodes_[node_id].right_id = right_id;
    nodes_[node_id].test = test;
    return absl::OkStatus();
  }

  absl::Status SetLeaf(size_t node_id, OutT leaf_value) {
    RETURN_IF_ERROR(TestNode(node_id, &node_used_));
    nodes_[node_id].leaf_id = leaf_values_.size();
    leaf_values_.push_back(leaf_value);
    return absl::OkStatus();
  }

  absl::StatusOr<CompactDecisionTree<OutT, NodeTest>> Compile() {
    if (nodes_.empty()) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "Empty trees are not supported.");
    }
    for (size_t i = 0; i < nodes_.size(); ++i) {
      if (!node_used_as_child_[i] || !node_used_[i]) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "Id is not used");
      }
    }
    CompactDecisionTree<OutT, NodeTest> tree;
    const size_t inner_node_count = nodes_.size() - leaf_values_.size();
    leaf_values_.shrink_to_fit();
    tree.adjustments = std::move(leaf_values_);
    std::vector<NodeId> node_mapping(nodes_.size());
    int cur_new_node_id = 0;
    for (int id = 0; id < nodes_.size(); ++id) {
      const auto& node = nodes_[id];
      if (node.leaf_id == -1) {
        node_mapping[id] = NodeId::Split(cur_new_node_id++);
      } else {
        node_mapping[id] = NodeId::Leaf(node.leaf_id);
      }
    }
    tree.splits.resize(inner_node_count);
    for (int id = 0; id < nodes_.size(); ++id) {
      const auto& node = nodes_[id];
      if (node.leaf_id == -1) {
        auto& compact_node = tree.splits[node_mapping[id].split_node_id()];
        compact_node.test = node.test;
        compact_node.next_node_ids[1] = node_mapping[node.left_id];
        compact_node.next_node_ids[0] = node_mapping[node.right_id];
      }
    }
    return tree;
  }

 private:
  absl::Status TestNode(size_t id, std::vector<bool>* used) {
    if (id >= used->size()) {
      return absl::Status(absl::StatusCode::kOutOfRange, "Id out of range");
    }
    if ((*used)[id]) {
      return absl::Status(absl::StatusCode::kInvalidArgument, "Id duplicated");
    }
    (*used)[id] = true;
    return absl::OkStatus();
  }

  struct Node {
    size_t leaf_id = -1;  // -1 for inner nodes
    size_t left_id = -1;
    size_t right_id = -1;
    NodeTest test = {};
  };
  std::vector<Node> nodes_;
  std::vector<bool> node_used_;
  std::vector<bool> node_used_as_child_;
  std::vector<OutT> leaf_values_;
};

template <class OutT, class NodeTest>
class DecisionTreeTraverser {
 public:
  explicit DecisionTreeTraverser(
      const internal::CompactDecisionTree<OutT, NodeTest>& tree)
      : node_id_(tree.RootNodeId()), tree_(tree) {}

  bool CanStep() const { return !node_id_.is_leaf(); }

  template <class FeatureContainer>
  void MakeStep(const FeatureContainer& values) {
    const auto& split = tree_.splits[node_id_.split_node_id()];
    node_id_ = split.next_node_ids[split.test(values)];
  }

  OutT GetValue() const { return tree_.adjustments[node_id_.adjsutment_id()]; }

 private:
  NodeId node_id_;
  const internal::CompactDecisionTree<OutT, NodeTest>& tree_;
};

struct EmptyFilterTag {};

}  // namespace internal

// =====  Predictors

template <class OutT, class NodeTest>
class SinglePredictor {
 public:
  explicit SinglePredictor(internal::CompactDecisionTree<OutT, NodeTest> tree)
      : tree_(std::move(tree)) {}

  template <class FeatureContainer>
  OutT Predict(const FeatureContainer& values) const {
    internal::DecisionTreeTraverser<OutT, NodeTest> traverser(tree_);
    while (ABSL_PREDICT_TRUE(traverser.CanStep())) {
      traverser.MakeStep(values);
    }
    return traverser.GetValue();
  }

 private:
  internal::CompactDecisionTree<OutT, NodeTest> tree_;
};

template <class TreeOutT, class NodeTest, class BinaryOp,
          class FilterTag = internal::EmptyFilterTag>
class BoostedPredictor {
 public:
  using OutT = std::decay_t<decltype(BinaryOp()(TreeOutT(), TreeOutT()))>;
  using NodeTestType = NodeTest;
  explicit BoostedPredictor(
      std::vector<internal::CompactDecisionTree<TreeOutT, NodeTest>> trees,
      std::vector<FilterTag> filter_tags, BinaryOp op)
      : trees_(std::move(trees)),
        filter_tags_(std::move(filter_tags)),
        op_(op) {}

  // FilterFn is a function FilterTag->bool to control which trees will be used
  // for the prediction. If filter(X) returns false then the tree with tag X
  // will be ignored. Tag can be passed as optional second argument in AddTree.
  template <class FeatureContainer, class FilterFn>
  OutT Predict(const FeatureContainer& values, OutT start,
               FilterFn filter) const {
    constexpr int kBatchSize = 16;
    absl::InlinedVector<internal::DecisionTreeTraverser<TreeOutT, NodeTest>,
                        kBatchSize>
        traversers;
    uint32_t ids[kBatchSize];

    for (int first = 0; first < trees_.size(); first += kBatchSize) {
      int count = std::min<int>(trees_.size() - first, kBatchSize);
      traversers.clear();
      uint32_t* ids_end = ids;

      for (int i = 0; i < count; ++i) {
        if (!filter(filter_tags_[first + i])) continue;
        *(ids_end++) = traversers.size();
        traversers.emplace_back(trees_[first + i]);
      }

      while (ABSL_PREDICT_TRUE(ids != ids_end)) {
        auto out_it = ids;
        for (auto it = ids; it != ids_end; ++it) {
          auto& traverser = traversers[*it];
          if (ABSL_PREDICT_TRUE(traverser.CanStep())) {
            traverser.MakeStep(values);
            *(out_it++) = *it;
          } else {
            start = op_(start, traverser.GetValue());
          }
        }
        ids_end = out_it;
      }
    }
    return start;
  }

  template <class FeatureContainer>
  OutT Predict(const FeatureContainer& values, OutT start = OutT()) const {
    if (trees_.empty()) return start;
    return Predict(values, start, [](FilterTag tag) { return true; });
  }

 private:
  std::vector<internal::CompactDecisionTree<TreeOutT, NodeTest>> trees_;
  std::vector<FilterTag> filter_tags_;
  BinaryOp op_;
};

// =====  Compilers

template <class OutT, class NodeTest>
class PredictorCompiler {
 public:
  explicit PredictorCompiler(size_t node_cnt) : impl_(node_cnt) {}

  // Sets an internal node information. NodeTest must implement
  // bool operator()(const FeatureContainer& values) const
  // true will signal to move to the left_id, otherwise to the right_id
  absl::Status SetNode(size_t node_id, size_t left_id, size_t right_id,
                       const NodeTest& test) {
    return impl_.SetNode(node_id, left_id, right_id, test);
  }

  absl::Status SetLeaf(size_t node_id, OutT leaf_value) {
    return impl_.SetLeaf(node_id, leaf_value);
  }

  absl::StatusOr<SinglePredictor<OutT, NodeTest>> Compile() {
    if (compiled_) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "Already compiled.");
    }
    compiled_ = true;
    auto tree_or = impl_.Compile();
    RETURN_IF_ERROR(tree_or.status());
    return SinglePredictor<OutT, NodeTest>(*std::move(tree_or));
  }

 private:
  bool compiled_ = false;
  internal::SingleTreeCompilationImpl<OutT, NodeTest> impl_;
};

// TODO: Can we have the same class as PredictorCompiler?
template <class OutT, class NodeTest>
class OneTreeCompiler {
 public:
  explicit OneTreeCompiler(
      internal::SingleTreeCompilationImpl<OutT, NodeTest>* impl)
      : impl_(impl) {}

  // Sets an internal node information. NodeTest must implement
  // bool operator()(const FeatureContainer& values) const
  // true will signal to move to the left_id, otherwise to the right_id
  absl::Status SetNode(size_t node_id, size_t left_id, size_t right_id,
                       const NodeTest& test) {
    return impl_->SetNode(node_id, left_id, right_id, test);
  }

  absl::Status SetLeaf(size_t node_id, OutT leaf_value) {
    return impl_->SetLeaf(node_id, leaf_value);
  }

 private:
  internal::SingleTreeCompilationImpl<OutT, NodeTest>* impl_;
};

// BinaryOp is used to combine result of evaluation of every trees.
template <class OutT, class NodeTest, class BinaryOp,
          class FilterTag = internal::EmptyFilterTag>
class BoostedPredictorCompiler {
 public:
  BoostedPredictorCompiler() {}
  explicit BoostedPredictorCompiler(BinaryOp op) : op_(op) {}

  OneTreeCompiler<OutT, NodeTest> AddTree(size_t node_count,
                                          FilterTag tag = FilterTag()) {
    impls_.emplace_back(node_count);
    filter_tags_.push_back(tag);
    return OneTreeCompiler<OutT, NodeTest>(&impls_.back());
  }

  absl::StatusOr<BoostedPredictor<OutT, NodeTest, BinaryOp, FilterTag>>
  Compile() {
    if (compiled_) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "Already compiled.");
    }
    compiled_ = true;
    std::vector<internal::CompactDecisionTree<OutT, NodeTest>> trees;
    trees.reserve(impls_.size());
    for (auto& impl : impls_) {
      auto tree_or = impl.Compile();
      RETURN_IF_ERROR(tree_or.status());
      trees.push_back(*std::move(tree_or));
    }
    return BoostedPredictor<OutT, NodeTest, BinaryOp, FilterTag>(
        std::move(trees), std::move(filter_tags_), op_);
  }

 private:
  bool compiled_ = false;
  BinaryOp op_;
  // deque used to avoid invalidating pointers after push_back
  std::deque<internal::SingleTreeCompilationImpl<OutT, NodeTest>> impls_;
  std::vector<FilterTag> filter_tags_;
};

}  // namespace arolla

#endif  // AROLLA_DECISION_FOREST_POINTWISE_EVALUATION_POINTWISE_H_
