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
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"

namespace arolla::expr {

void LightweightExprStackTrace::AddTrace(const ExprNodePtr& transformed_node,
                                         const ExprNodePtr& original_node) {
  if (!transformed_node->is_op() || !original_node->is_op()) {
    return;
  }
  if (transformed_node->fingerprint() == original_node->fingerprint()) {
    return;
  }
  auto it = original_node_op_name_.find(original_node->fingerprint());
  bool source_node_is_original = (it == original_node_op_name_.end());
  if (!source_node_is_original) {
    original_node_op_name_.emplace(transformed_node->fingerprint(),
                                   // Explicitly copy the string before .emplace
                                   // operation invalidated the iterator.
                                   std::string(it->second));
    return;
  }
  bool source_operator_is_ignored =
      absl::StartsWith(original_node->op()->display_name(), "anonymous.");
  if (!source_operator_is_ignored) {
    original_node_op_name_.emplace(transformed_node->fingerprint(),
                                   original_node->op()->display_name());
  }
}

std::string LightweightExprStackTrace::GetOriginalOperatorName(
    Fingerprint fp) const {
  auto it = original_node_op_name_.find(fp);
  return it != original_node_op_name_.end() ? it->second : "";
}

namespace {

// BoundExprStackTrace interface implementation for the
// LightweightExprStackTrace. See the comment on top of the header file for more
// details.
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

void DetailedExprStackTrace::AddTrace(const ExprNodePtr& transformed_node,
                                      const ExprNodePtr& original_node) {
  lightweight_stack_trace_.AddTrace(transformed_node, original_node);

  if (!transformed_node->is_op()) {
    return;
  }
  if (transformed_node->fingerprint() == original_node->fingerprint()) {
    return;
  }
  if (original_nodes_.contains(transformed_node->fingerprint())) {
    return;  // Avoid dependency cycles.
  }
  original_nodes_.insert(original_node->fingerprint());

  shared_data_->traceback.emplace(transformed_node->fingerprint(),
                                  original_node->fingerprint());
}

void DetailedExprStackTrace::AddSourceLocation(
    const ExprNodePtr& node, SourceLocationView source_location) {
  shared_data_->source_locations.emplace(
      node->fingerprint(),
      SourceLocationPayload{
          .function_name = std::string(source_location.function_name),
          .file_name = std::string(source_location.file_name),
          .line = source_location.line,
          .column = source_location.column,
          .line_text = std::string(source_location.line_text),
      });
}

// BoundExprStackTrace interface implementation for the DetailedExprStackTrace.
// See the comment on top of the header file for more details.
class DetailedBoundExprStackTrace : public BoundExprStackTrace {
 public:
  explicit DetailedBoundExprStackTrace(
      std::unique_ptr<BoundExprStackTrace> lightweight_bound_stack_trace,
      std::shared_ptr<const DetailedExprStackTrace::SharedData> shared_data)
      : lightweight_bound_stack_trace_(
            std::move(lightweight_bound_stack_trace)),
        ip_to_fp_(
            std::make_shared<absl::flat_hash_map<int64_t, Fingerprint>>()),
        shared_data_(std::move(shared_data)) {}

  void RegisterIp(int64_t ip, const ExprNodePtr& node) final {
    lightweight_bound_stack_trace_->RegisterIp(ip, node);
    ip_to_fp_->emplace(ip, node->fingerprint());
  }

  AnnotateEvaluationError Finalize() && final {
    return [lightweight_annotate_error =
                std::move(*lightweight_bound_stack_trace_).Finalize(),
            ip_to_fp = std::move(ip_to_fp_),
            shared_data = std::move(shared_data_)](
               int64_t failed_ip, absl::Status status) -> absl::Status {
      status = lightweight_annotate_error(failed_ip, std::move(status));

      auto fp_it = ip_to_fp->find(failed_ip);
      if (fp_it == ip_to_fp->end()) {
        return status;
      }
      Fingerprint failed_fp = fp_it->second;
      if (auto loc_it = shared_data->source_locations.find(failed_fp);
          loc_it != shared_data->source_locations.end()) {
        status = WithSourceLocation(std::move(status), loc_it->second);
      }
      auto trace_it = shared_data->traceback.find(failed_fp);
      while (trace_it != shared_data->traceback.end()) {
        failed_fp = trace_it->second;
        if (auto loc_it = shared_data->source_locations.find(failed_fp);
            loc_it != shared_data->source_locations.end()) {
          status = WithSourceLocation(std::move(status), loc_it->second);
        }
        trace_it = shared_data->traceback.find(failed_fp);
      }
      return status;
    };
  }

 private:
  std::unique_ptr<BoundExprStackTrace> lightweight_bound_stack_trace_;
  // Instruction pointer to the corresponding (lowest level) ExprNode
  // fingerprint.
  std::shared_ptr<absl::flat_hash_map<int64_t, Fingerprint>> ip_to_fp_;
  std::shared_ptr<const DetailedExprStackTrace::SharedData> shared_data_;
};

BoundExprStackTraceFactory DetailedExprStackTrace::Finalize() && {
  return [lightweight_bound_stack_trace_factory =
              std::move(lightweight_stack_trace_).Finalize(),
          data = std::move(shared_data_)]() {
    return std::make_unique<DetailedBoundExprStackTrace>(
        lightweight_bound_stack_trace_factory(), data);
  };
}

}  // namespace arolla::expr
