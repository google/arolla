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
#include "arolla/expr/operator_repr_functions.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"
#include "arolla/util/text.h"

namespace arolla::expr {

namespace {

struct InfixOp {
  enum Kind : int8_t { kUnary, kBinary } kind;
  ReprToken::Precedence precedence;
  absl::string_view symbol;
};

// TODO: Use constexpr `gtl::fixed_flat_map_of` instead, as shown
// in go/totw/170.
static const auto* const kUnaryInfixOps =
    new absl::flat_hash_map<absl::string_view, InfixOp>{
        {"math.pos", {InfixOp::kUnary, {1, 1}, "+"}},
        {"math.neg", {InfixOp::kUnary, {1, 1}, "-"}},
        {"core.presence_not", {InfixOp::kUnary, {1, 1}, "~"}},
    };

static const auto* const kBinaryInfixOps =
    new absl::flat_hash_map<absl::string_view, InfixOp>{
        {"math.pow", {InfixOp::kBinary, {1, 2}, " ** "}},
        {"math.multiply", {InfixOp::kBinary, {3, 2}, " * "}},
        {"math.divide", {InfixOp::kBinary, {3, 2}, " / "}},
        {"math.floordiv", {InfixOp::kBinary, {3, 2}, " // "}},
        {"math.mod", {InfixOp::kBinary, {3, 2}, " % "}},
        {"math.add", {InfixOp::kBinary, {5, 4}, " + "}},
        {"math.subtract", {InfixOp::kBinary, {5, 4}, " - "}},
        {"core.presence_and", {InfixOp::kBinary, {7, 6}, " & "}},
        {"core.presence_or", {InfixOp::kBinary, {9, 8}, " | "}},
        {"core.less", {InfixOp::kBinary, {10, 10}, " < "}},
        {"core.less_equal", {InfixOp::kBinary, {10, 10}, " <= "}},
        {"core.equal", {InfixOp::kBinary, {10, 10}, " == "}},
        {"core.not_equal", {InfixOp::kBinary, {10, 10}, " != "}},
        {"core.greater_equal", {InfixOp::kBinary, {10, 10}, " >= "}},
        {"core.greater", {InfixOp::kBinary, {10, 10}, " > "}},
    };

// Returns the ReprTokens corresponding to the given node's deps.
std::vector<const ReprToken*> GetNodeDepsTokens(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  std::vector<const ReprToken*> inputs(node->node_deps().size());
  for (size_t i = 0; i < node->node_deps().size(); ++i) {
    inputs[i] = &node_tokens.at(node->node_deps()[i]->fingerprint());
  }
  return inputs;
}

std::optional<ReprToken> UnaryReprFn(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  auto it = kUnaryInfixOps->find(node->op()->display_name());
  const auto inputs = GetNodeDepsTokens(node, node_tokens);
  if (it == kUnaryInfixOps->end() || inputs.size() != 1) {
    return std::nullopt;
  }
  const auto& infix_op = it->second;
  ReprToken result;
  if (inputs[0]->precedence.left < infix_op.precedence.right) {
    result.str = absl::StrCat(infix_op.symbol, inputs[0]->str);
  } else {
    result.str = absl::StrCat(infix_op.symbol, "(", inputs[0]->str, ")");
  }
  result.precedence.left = infix_op.precedence.left;
  result.precedence.right = infix_op.precedence.right;
  return result;
}

std::optional<ReprToken> BinaryReprFn(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  auto it = kBinaryInfixOps->find(node->op()->display_name());
  const auto inputs = GetNodeDepsTokens(node, node_tokens);
  if (it == kBinaryInfixOps->end() || inputs.size() != 2) {
    return std::nullopt;
  }
  const auto& infix_op = it->second;
  ReprToken result;
  const bool left_precedence =
      (inputs[0]->precedence.right < infix_op.precedence.left);
  const bool right_precedence =
      (inputs[1]->precedence.left < infix_op.precedence.right);
  if (left_precedence && right_precedence) {
    result.str = absl::StrCat(inputs[0]->str, infix_op.symbol, inputs[1]->str);
  } else if (left_precedence && !right_precedence) {
    result.str =
        absl::StrCat(inputs[0]->str, infix_op.symbol, "(", inputs[1]->str, ")");
  } else if (!left_precedence && right_precedence) {
    result.str =
        absl::StrCat("(", inputs[0]->str, ")", infix_op.symbol, inputs[1]->str);
  } else {
    result.str = absl::StrCat("(", inputs[0]->str, ")", infix_op.symbol, "(",
                              inputs[1]->str, ")");
  }
  result.precedence.left = infix_op.precedence.left;
  result.precedence.right = infix_op.precedence.right;
  return result;
}

std::optional<ReprToken> GetAttrReprFn(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  DCHECK_EQ(node->op()->display_name(), "core.getattr");
  constexpr ReprToken::Precedence kGetAttrPrecedence{0, -1};
  const auto& node_deps = node->node_deps();
  if (node_deps.size() != 2 || !node_deps[1]->is_literal()) {
    return std::nullopt;
  }
  const auto& attr = node_deps[1]->qvalue();
  if (!attr.has_value() || attr->GetType() != GetQType<Text>() ||
      !IsIdentifier(attr->UnsafeAs<Text>().view())) {
    return std::nullopt;
  }
  ReprToken result;
  const auto inputs = GetNodeDepsTokens(node, node_tokens);
  DCHECK_EQ(inputs.size(), 2);
  if (inputs[0]->precedence.right < kGetAttrPrecedence.left) {
    result.str =
        absl::StrCat(inputs[0]->str, ".", attr->UnsafeAs<Text>().view());
  } else {
    result.str =
        absl::StrCat("(", inputs[0]->str, ").", attr->UnsafeAs<Text>().view());
  }
  result.precedence = kGetAttrPrecedence;
  return result;
}

std::optional<std::string> MakeSliceRepr(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  if (!IsRegisteredOperator(node->op()) ||
      node->op()->display_name() != "core.make_slice") {
    return std::nullopt;
  }
  auto is_unspecified = [](const ExprNodePtr& node) {
    return node->is_literal() && node->qtype() == GetUnspecifiedQType();
  };
  constexpr ReprToken::Precedence kSlicePrecedence{11, 11};
  const auto& node_deps = node->node_deps();
  if (node_deps.size() != 3) {
    return std::nullopt;
  }
  std::string result;
  const auto inputs = GetNodeDepsTokens(node, node_tokens);
  DCHECK_EQ(inputs.size(), 3);
  // Handle "a:" in "a:b:c".
  if (is_unspecified(node_deps[0])) {
    result = ":";
  } else if (inputs[0]->precedence.right < kSlicePrecedence.left) {
    result = absl::StrCat(inputs[0]->str, ":");
  } else {
    result = absl::StrCat("(", inputs[0]->str, "):");
  }
  // Handle "b" in "a:b:c".
  if (!is_unspecified(node_deps[1])) {
    if (inputs[1]->precedence.left < kSlicePrecedence.right &&
        (inputs[1]->precedence.right < kSlicePrecedence.left ||
         is_unspecified(node_deps[2]))) {
      absl::StrAppend(&result, inputs[1]->str);
    } else {
      absl::StrAppend(&result, "(", inputs[1]->str, ")");
    }
  }
  // Handle ":c" in "a:b:c".
  if (!is_unspecified(node_deps[2])) {
    if (inputs[2]->precedence.left < kSlicePrecedence.right) {
      absl::StrAppend(&result, ":", inputs[2]->str);
    } else {
      absl::StrAppend(&result, ":(", inputs[2]->str, ")");
    }
  }
  return result;
}

std::optional<ReprToken> GetItemReprFn(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  DCHECK_EQ(node->op()->display_name(), "core.getitem");
  constexpr ReprToken::Precedence kGetItemPrecedence{0, -1};
  if (node->node_deps().size() != 2) {
    return std::nullopt;
  }
  const auto& lhs = node_tokens.at(node->node_deps()[0]->fingerprint());
  const auto maybe_slice = MakeSliceRepr(node->node_deps()[1], node_tokens);
  const std::string& rhs_str =
      maybe_slice ? *maybe_slice
                  : node_tokens.at(node->node_deps()[1]->fingerprint()).str;
  ReprToken result;
  if (lhs.precedence.right < kGetItemPrecedence.left) {
    result.str = absl::StrCat(lhs.str, "[", rhs_str, "]");
  } else {
    result.str = absl::StrCat("(", lhs.str, ")[", rhs_str, "]");
  }
  result.precedence = kGetItemPrecedence;
  return result;
}

class OpReprRegistry {
 public:
  void Set(std::string key, OperatorReprFn op_repr_fn)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    registry_[std::move(key)] = std::move(op_repr_fn);
  }

  OperatorReprFn Get(absl::string_view key) const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    if (const auto it = registry_.find(key); it != registry_.end()) {
      return it->second;
    }
    return nullptr;
  }

 private:
  mutable absl::Mutex mutex_;
  absl::flat_hash_map<std::string, OperatorReprFn> registry_
      ABSL_GUARDED_BY(mutex_);
};

OpReprRegistry* GetOpReprRegistryForRegisteredOp() {
  static OpReprRegistry* result = []() {
    auto* registry = new OpReprRegistry;
    for (const auto& [key, _] : *kUnaryInfixOps) {
      registry->Set(std::string(key), UnaryReprFn);
    }
    for (const auto& [key, _] : *kBinaryInfixOps) {
      registry->Set(std::string(key), BinaryReprFn);
    }
    registry->Set("core.getattr", GetAttrReprFn);
    registry->Set("core.getitem", GetItemReprFn);
    return registry;
  }();
  return result;
}

std::optional<ReprToken> RegisteredOperatorReprFn(
    const ExprNodePtr& expr_node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  DCHECK(expr_node->is_op() && IsRegisteredOperator(expr_node->op()));
  if (auto op_repr_fn = GetOpReprRegistryForRegisteredOp()->Get(
          expr_node->op()->display_name());
      op_repr_fn != nullptr) {
    return op_repr_fn(expr_node, node_tokens);
  }
  return std::nullopt;
}

OpReprRegistry* GetOpReprRegistryForQValueSpecialization() {
  static OpReprRegistry* result = []() {
    auto* registry = new OpReprRegistry;
    registry->Set("::arolla::expr::RegisteredOperator",
                  RegisteredOperatorReprFn);
    return registry;
  }();
  return result;
}

}  // namespace

void RegisterOpReprFnByQValueSpecializationKey(
    std::string qvalue_specialization_key, OperatorReprFn op_repr_fn) {
  GetOpReprRegistryForQValueSpecialization()->Set(
      std::move(qvalue_specialization_key), std::move(op_repr_fn));
}

void RegisterOpReprFnByByRegistrationName(std::string op_name,
                                          OperatorReprFn op_repr_fn) {
  GetOpReprRegistryForRegisteredOp()->Set(std::move(op_name),
                                          std::move(op_repr_fn));
}

std::optional<ReprToken> FormatOperatorNodePretty(
    const ExprNodePtr& node,
    const absl::flat_hash_map<Fingerprint, ReprToken>& node_tokens) {
  if (auto op_repr_fn = GetOpReprRegistryForQValueSpecialization()->Get(
          node->op()->py_qvalue_specialization_key());
      op_repr_fn != nullptr) {
    if (auto res = op_repr_fn(node, node_tokens)) {
      return *std::move(res);
    }
  }
  return std::nullopt;
}

}  // namespace arolla::expr
