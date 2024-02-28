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
#ifndef AROLLA_EXPR_EXPR_STACK_TRACE_H_
#define AROLLA_EXPR_EXPR_STACK_TRACE_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"

namespace arolla::expr {

// The transformation type categorizes different ExprNode transformations. It is
// used to add this information to the stack trace.
// kUntraced denotes transformations that will not be printed, and can spare
// memory in how they are stored.
enum class TransformationType {
  kUntraced = 0,
  kLowering = 1,
  kOptimization = 2,
  kChildTransform = 3,
  kCausedByAncestorTransform = 4,
};

inline std::string TransformationString(TransformationType t) {
  switch (t) {
    case TransformationType::kLowering:
      return "was lowered to";
    case TransformationType::kOptimization:
      return "was optimized to";
    case TransformationType::kUntraced:
      return "untraced";
    case TransformationType::kChildTransform:
      return "had transformations applied to its children";
    case TransformationType::kCausedByAncestorTransform:
      return "which contains";
    default:
      return "unknown";
  }
}

// Interface for a stack trace tracking Expr transformation
// (e.g in prepare_expression.cc).
class ExprStackTrace {
 public:
  virtual ~ExprStackTrace() = default;

  // Creates a traceback from a target node to a source node including a
  // transformation type. Stores representations of nodes when appropriate.
  virtual void AddTrace(ExprNodePtr target_node, ExprNodePtr source_node,
                        TransformationType t) = 0;

  // Produces the stack trace for the operator associated with a fingerprint.
  virtual std::string FullTrace(Fingerprint fp) const = 0;
};

// TODO: remove this class if we end up using only
// LightweightExprStackTrace.
// Detailed Expr stack trace that tracks the transformation histories of
// nodes storing all intermediate node transformations.
class DetailedExprStackTrace : public ExprStackTrace {
 public:
  // Creates a traceback from a target node to a source node including a
  // transformation type. Will store every intermediate state of a node's
  // compilation, except when the transformation type is untraced.
  void AddTrace(ExprNodePtr target_node, ExprNodePtr source_node,
                TransformationType t) final;

  // Produces the stack trace for the operator associated with a fingerprint.
  // The format is
  //
  // ORIGINAL: GetDebugSnippet(original_node)
  // COMPILED NODE: GetDebugSnippet(compiled_node)
  // DETAILED STACK TRACE:
  // GetDebugSnippet(original)
  //   first_transformation (e.g. "was lowered to")
  // GetDebugSnippet(intermediate_node)
  // ...
  //   final_transformation
  // GetDebugSnippet(compiled_node)
  std::string FullTrace(Fingerprint fp) const final;

 private:
  struct Transformation {
    Fingerprint target_fp;
    Fingerprint source_fp;
    TransformationType type;
  };

  // Returns the source node and transformation type associated with a target
  // node.
  std::optional<std::pair<Fingerprint, TransformationType>> GetTrace(
      Fingerprint fp) const;

  // Returns a string representation for a given fingerprint 'safely', i.e.
  // without throwing an error in case fingerprint is not found.
  std::string GetRepr(Fingerprint fp) const;

  // Returns transformations in the order in which they happened.
  std::vector<Transformation> GetTransformations(Fingerprint fp) const;

  absl::flat_hash_map<Fingerprint, std::pair<Fingerprint, TransformationType>>
      traceback_;
  absl::flat_hash_map<Fingerprint, ExprNodePtr> repr_;
};

// Lightweight Expr stack trace that maps compiled node to original nodes. Only
// fingerprints are stored for intermediate nodes.
class LightweightExprStackTrace : public ExprStackTrace {
 public:
  void AddTrace(ExprNodePtr target_node, ExprNodePtr source_node,
                TransformationType t) final;

  std::string FullTrace(Fingerprint fp) const final;

  // Adds representations of all required nodes given compiled expr and original
  // expr. Note: AddTrace does not add representations so calling this function
  // at the end of compilation is necessary.
  void AddRepresentations(ExprNodePtr compiled_node, ExprNodePtr original_node);

 private:
  // Returns a string representation for a given fingerprint 'safely', i.e.
  // without throwing an error in case fingerprint is not found.
  std::string GetRepr(Fingerprint fp) const;

  absl::flat_hash_map<Fingerprint, Fingerprint> original_node_mapping_;
  absl::flat_hash_map<Fingerprint, ExprNodePtr> repr_;
};

// Bound Stack Trace takes an Expr Stack Traces and matches instruction pointers
// to fingerprints of nodes, and prints a full trace given an instruction
// pointer.
class BoundExprStackTraceBuilder {
 public:
  explicit BoundExprStackTraceBuilder(
      std::shared_ptr<const ExprStackTrace> expr_stack_trace);

  // Creates a link between an ip(instruction pointer) and an ExprNode.
  // Essentially a necessary link between ExprStackTrace and
  // BoundExprStackTrace.
  void RegisterIp(int64_t ip, const ExprNodePtr& node);

  DenseArray<Text> Build(int64_t num_operators) const;

 private:
  std::shared_ptr<const ExprStackTrace> stack_trace_;
  absl::flat_hash_map<int64_t, Fingerprint> ip_to_fingerprint_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_STACK_TRACE_H_
