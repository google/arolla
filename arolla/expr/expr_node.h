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
#ifndef AROLLA_EXPR_EXPR_NODE_H_
#define AROLLA_EXPR_EXPR_NODE_H_

#include <cstdint>
#include <iosfwd>
#include <optional>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node_ptr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"

namespace arolla::expr {

// Exhaustive list of Expr node types.
enum class ExprNodeType : uint8_t {
  kLiteral = 0,
  kLeaf = 1,
  kOperator = 2,
  kPlaceholder = 3,
};

// For better test output:
std::ostream& operator<<(std::ostream& os, ExprNodeType t);

// An expression node.
class ExprNode : public RefcountedBase {
  struct PrivateConstructorTag {};

 public:
  // Returns a literal node.
  static ExprNodePtr MakeLiteralNode(TypedValue&& qvalue);

  // Returns a leaf node.
  static ExprNodePtr MakeLeafNode(absl::string_view leaf_key);

  // Returns a leaf node.
  static ExprNodePtr MakePlaceholderNode(absl::string_view placeholder_key);

  // Returns an operator node.
  //
  // This is a low-level factory method that is not intended for general use.
  // Only use it if you understand the implications of its use.
  //
  // Precondition: The op and node_deps must not be nullptr; the attr must be
  // consistent with op and node_deps.
  static ExprNodePtr UnsafeMakeOperatorNode(
      ExprOperatorPtr&& op, std::vector<ExprNodePtr>&& node_deps,
      ExprAttributes&& attr);

  explicit ExprNode(PrivateConstructorTag) {}
  ~ExprNode();

  ExprNodeType type() const { return type_; }
  bool is_literal() const { return type_ == ExprNodeType::kLiteral; }
  bool is_leaf() const { return type_ == ExprNodeType::kLeaf; }
  bool is_op() const { return type_ == ExprNodeType::kOperator; }
  bool is_placeholder() const { return type_ == ExprNodeType::kPlaceholder; }

  const ExprAttributes& attr() const { return attr_; }
  const QType* /*nullable*/ qtype() const { return attr_.qtype(); }
  const std::optional<TypedValue>& qvalue() const { return attr_.qvalue(); }

  const std::string& leaf_key() const { return leaf_key_; }
  const std::string& placeholder_key() const { return placeholder_key_; }
  const ExprOperatorPtr& op() const { return op_; }
  const std::vector<ExprNodePtr>& node_deps() const { return node_deps_; }

  const Fingerprint& fingerprint() const { return fingerprint_; }

 private:
  ExprNodeType type_;
  std::string leaf_key_;
  std::string placeholder_key_;
  ExprOperatorPtr op_;
  std::vector<ExprNodePtr> node_deps_;
  ExprAttributes attr_;
  Fingerprint fingerprint_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_NODE_H_
