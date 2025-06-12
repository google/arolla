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
#include "arolla/expr/eval/expr_stack_trace.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"

namespace arolla::expr {
namespace {

inline std::string TransformationString(ExprStackTrace::TransformationType t) {
  switch (t) {
    case ExprStackTrace::TransformationType::kLowering:
      return "was lowered to";
    case ExprStackTrace::TransformationType::kOptimization:
      return "was optimized to";
    case ExprStackTrace::TransformationType::kUntraced:
      return "untraced";
    case ExprStackTrace::TransformationType::kChildTransform:
      return "had transformations applied to its children";
    case ExprStackTrace::TransformationType::kCausedByAncestorTransform:
      return "which contains";
    default:
      return "unknown";
  }
}

}  // namespace

// TODO: The current API does not allow to save anything when a
// node was not transformed at all. Consider adding AddNode() method.
void LightweightExprStackTrace::AddTrace(ExprNodePtr target_node,
                                         ExprNodePtr source_node,
                                         TransformationType t) {
  if (!target_node->is_op() || !source_node->is_op()) {
    return;
  }
  if (target_node->fingerprint() == source_node->fingerprint()) {
    return;
  }
  auto it = original_node_op_name_.find(source_node->fingerprint());
  bool source_node_is_original = (it == original_node_op_name_.end());
  if (!source_node_is_original) {
    original_node_op_name_.emplace(target_node->fingerprint(),
                                   // Explicitly copy the string before .emplace
                                   // operation invalidated the iterator.
                                   std::string(it->second));
    return;
  }
  bool source_operator_is_ignored =
      absl::StartsWith(source_node->op()->display_name(), "anonymous.");
  bool source_node_is_irrelevant =
      t == TransformationType::kCausedByAncestorTransform;
  if (!source_operator_is_ignored && !source_node_is_irrelevant) {
    original_node_op_name_.emplace(target_node->fingerprint(),
                                   source_node->op()->display_name());
  }
}

std::string LightweightExprStackTrace::GetOriginalOperatorName(
    Fingerprint fp) const {
  auto it = original_node_op_name_.find(fp);
  return it != original_node_op_name_.end() ? it->second : "";
}

namespace {

class LightweightBoundExprStackTrace : public BoundExprStackTrace {
 public:
  explicit LightweightBoundExprStackTrace(
      std::shared_ptr<const absl::flat_hash_map<Fingerprint, std::string>>
          original_node_op_name)
      : original_node_op_name_(std::move(original_node_op_name)) {}

  void RegisterIp(int64_t ip, const ExprNodePtr& node) final {
    auto it = original_node_op_name_->find(node->fingerprint());
    absl::string_view op_name = it != original_node_op_name_->end()
                                    ? it->second
                                    : node->op()->display_name();
    op_display_name_.emplace(ip, std::string(op_name));
    num_operators_ = std::max(num_operators_, ip + 1);
  }

  AnnotateEvaluationError Finalize() && final {
    // Use dense arrays instead of std::vector for more compact storage.
    DenseArrayBuilder<Text> display_names_builder(num_operators_);
    for (int64_t i = 0; i < num_operators_; ++i) {
      if (auto it = op_display_name_.find(i); it != op_display_name_.end()) {
        display_names_builder.Add(i, Text{std::move(it->second)});
      }
    }
    return [display_names = std::move(display_names_builder).Build()](
               int64_t failed_ip, const absl::Status& status) -> absl::Status {
      absl::string_view topmost_operator_name =
          failed_ip < display_names.size() ? display_names[failed_ip].value
                                           : "";
      return WithPayloadAndCause(
          absl::Status(status.code(), status.message()),
          VerboseRuntimeError{.operator_name =
                                  std::string(topmost_operator_name)},
          status);
    };
  }

 private:
  std::shared_ptr<const absl::flat_hash_map<Fingerprint, std::string>>
      original_node_op_name_;
  absl::flat_hash_map<int64_t, std::string> op_display_name_;
  int64_t num_operators_ = 0;
};

}  // namespace

BoundExprStackTraceFactory LightweightExprStackTrace::Finalize() && {
  return [original_node_op_name = std::make_shared<
              const absl::flat_hash_map<Fingerprint, std::string>>(
              std::move(original_node_op_name_))]() {
    return std::make_unique<LightweightBoundExprStackTrace>(
        original_node_op_name);
  };
}

void DetailedExprStackTrace::AddTrace(ExprNodePtr target_node,
                                      ExprNodePtr source_node,
                                      ExprStackTrace::TransformationType t) {
  lightweight_stack_trace_.AddTrace(target_node, source_node, t);
  if (!target_node->is_op()) {
    return;
  }
  if (target_node->fingerprint() == source_node->fingerprint()) {
    return;
  }

  // Store first trace for a node in case of multiple.
  traceback_.insert(
      {target_node->fingerprint(), {source_node->fingerprint(), t}});

  // We only store the representation of the source node when it is the
  // original node, i.e. it has no traceback.
  if (traceback_.find(source_node->fingerprint()) == traceback_.end()) {
    repr_[source_node->fingerprint()] = source_node;
  }

  // If the transformation is traced, we store the representation of the
  // target node.
  if (t != ExprStackTrace::TransformationType::kUntraced) {
    repr_[target_node->fingerprint()] = target_node;
  }
}

std::optional<std::pair<Fingerprint, ExprStackTrace::TransformationType>>
DetailedExprStackTrace::GetTrace(Fingerprint fp) const {
  auto it = traceback_.find(fp);
  if (it == traceback_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::string DetailedExprStackTrace::GetRepr(Fingerprint fp) const {
  if (auto it = repr_.find(fp); it != repr_.end()) {
    return GetDebugSnippet(it->second);
  } else {
    return absl::StrCat("Could not find representation for node ",
                        fp.AsString());
  }
}

std::vector<DetailedExprStackTrace::Transformation>
DetailedExprStackTrace::GetTransformations(Fingerprint fp) const {
  auto current_fp = fp;
  std::vector<Transformation> transformations;

  // There are conditions where there may be cycles, see below.
  absl::flat_hash_set<Fingerprint> visited;
  visited.insert(current_fp);

  auto nxt = GetTrace(current_fp);
  while (nxt.has_value()) {
    if (nxt->second != TransformationType::kUntraced) {
      transformations.push_back({current_fp, nxt->first, nxt->second});
    }
    current_fp = nxt->first;
    if (!visited.insert(current_fp).second) {
      // The only condition that creates cycles in current Expr processing is
      // the adding/removal of QType Annotations.
      // Annotations are added through PopulateQtypes transformation during
      // PrepareExpression. PrepareExpression is guaranteed to not create
      // cycles.
      // Annotations are removed through ExtractQTypesForCompilation, which only
      // happens after PrepareExpression is complete.
      // Thus, we can only have cycles that are variations of the form
      // L.x -> annotation.qtype(L.x, ...) -> L.x.
      // We stop after one iteration of the cycle.
      break;
    }

    nxt = GetTrace(current_fp);
  }

  std::reverse(transformations.begin(), transformations.end());

  // Set the first node to the absolute original node(ignoring untraced
  // transformations). This is the only source_fp for which we have stored
  // a representation.
  if (!transformations.empty()) {
    transformations.begin()->source_fp = current_fp;
  }

  return transformations;
}

std::string DetailedExprStackTrace::FullTrace(Fingerprint fp) const {
  auto transformations = GetTransformations(fp);

  if (transformations.empty()) return "";

  // Show the original and final nodes most prominently.
  std::string stack_trace = absl::StrCat(
      "ORIGINAL NODE: ", GetRepr(transformations.front().source_fp),
      "\nCOMPILED NODE: ", GetRepr(transformations.back().target_fp));

  if (transformations.size() == 1) return stack_trace;

  // We show the transformations in the order in which they happened.
  absl::StrAppend(&stack_trace, "\nDETAILED STACK TRACE:\n",
                  GetRepr(transformations.begin()->source_fp));
  for (auto it = transformations.begin(); it != transformations.end(); ++it) {
    absl::StrAppend(&stack_trace, "\n  ", TransformationString(it->type), "\n",
                    GetRepr(it->target_fp));
  }

  return stack_trace;
}

BoundExprStackTraceFactory DetailedExprStackTrace::Finalize() && {
  // Dummy implementation that ignores information from DetailedExprStackTrace.
  // TODO: Rewrite this function to include detailed information.
  return std::move(lightweight_stack_trace_).Finalize();
}

}  // namespace arolla::expr
