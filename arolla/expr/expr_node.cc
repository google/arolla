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
#include "arolla/expr/expr_node.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace arolla::expr {

std::ostream& operator<<(std::ostream& os, ExprNodeType t) {
  switch (t) {
    case expr::ExprNodeType::kLiteral: {
      return os << "kLiteral";
    }
    case expr::ExprNodeType::kLeaf: {
      return os << "kLeaf";
    }
    case expr::ExprNodeType::kOperator: {
      return os << "kOperator";
    }
    case expr::ExprNodeType::kPlaceholder: {
      return os << "kPlaceholder";
    }
  }
  return os << "ExprNodeType(" << static_cast<int>(t) << ")";
}

ExprNodePtr ExprNode::MakeLiteralNode(TypedValue&& qvalue) {
  FingerprintHasher hasher("LiteralNode");
  hasher.Combine(qvalue.GetFingerprint());
  auto self = std::make_unique<ExprNode>(PrivateConstructorTag());
  self->type_ = ExprNodeType::kLiteral;
  self->attr_ = ExprAttributes(std::move(qvalue));
  self->fingerprint_ = std::move(hasher).Finish();
  return ExprNodePtr::Own(std::move(self));
}

ExprNodePtr ExprNode::MakeLeafNode(absl::string_view leaf_key) {
  auto self = std::make_unique<ExprNode>(PrivateConstructorTag());
  self->type_ = ExprNodeType::kLeaf;
  self->leaf_key_ = std::string(leaf_key);
  self->fingerprint_ = FingerprintHasher("LeafNode").Combine(leaf_key).Finish();
  return ExprNodePtr::Own(std::move(self));
}

ExprNodePtr ExprNode::MakePlaceholderNode(absl::string_view placeholder_key) {
  auto self = std::make_unique<ExprNode>(PrivateConstructorTag());
  self->type_ = ExprNodeType::kPlaceholder;
  self->placeholder_key_ = std::string(placeholder_key);
  self->fingerprint_ =
      FingerprintHasher("PlaceholderNode").Combine(placeholder_key).Finish();
  return ExprNodePtr::Own(std::move(self));
}

ExprNodePtr ExprNode::UnsafeMakeOperatorNode(
    ExprOperatorPtr&& op, std::vector<ExprNodePtr>&& node_deps,
    ExprAttributes&& attr) {
  FingerprintHasher hasher("OpNode");
  DCHECK(op);
  hasher.Combine(op->fingerprint());
  for (const auto& node_dep : node_deps) {
    DCHECK(node_dep != nullptr);
    hasher.Combine(node_dep->fingerprint());
  }
  hasher.Combine(attr);
  auto self = std::make_unique<ExprNode>(PrivateConstructorTag());
  self->type_ = ExprNodeType::kOperator;
  self->op_ = std::move(op);
  self->node_deps_ = std::move(node_deps);
  self->attr_ = std::move(attr);
  self->fingerprint_ = std::move(hasher).Finish();
  return ExprNodePtr::Own(std::move(self));
}

// Implement non-recursive destruction to avoid potential stack overflow on
// deep expressions.
ExprNode::~ExprNode() {
  if (node_deps_.empty()) {
    return;
  }
  constexpr size_t kMaxDepth = 32;

  // Array for postponing removing dependant ExprNodePtr's.
  //
  // NOTE: NoDestructor is used to avoid issues with the
  // destruction order of globals vs thread_locals.
  thread_local absl::NoDestructor<std::deque<std::vector<ExprNodePtr>>> deps;

  // The first destructed node will perform clean up of postponed removals.
  thread_local size_t destructor_depth = 0;

  if (destructor_depth > kMaxDepth) {
    // Postpone removing to avoid deep recursion.
    deps->push_back(std::move(node_deps_));
    return;
  }

  destructor_depth++;
  absl::Cleanup decrease_depth = [&] { --destructor_depth; };
  // Will cause calling ~ExprNode for node_deps_ elements
  // with increased destructor_depth.
  node_deps_.clear();

  if (destructor_depth == 1 && !deps->empty()) {
    while (!deps->empty()) {
      // Move out the first element of `deps`.
      // Destructor may cause adding more elements to the `deps`.
      auto tmp = std::move(deps->back());
      // `pop_back` will remove empty vector, so
      // `pop_back` will *not* cause any ExprNode destructions.
      deps->pop_back();
    }
    // Avoid holding heap memory for standby threads.
    deps->shrink_to_fit();
  }
}

}  // namespace arolla::expr
