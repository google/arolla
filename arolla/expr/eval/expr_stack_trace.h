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
#ifndef AROLLA_EXPR_EXPR_STACK_TRACE_H_
#define AROLLA_EXPR_EXPR_STACK_TRACE_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr_node.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"

namespace arolla::expr {

// The current structure of Eval API forces us to split stack traces into
// four parts:
//
// 1. ExprStackTrace: used to track Expr -> Expr transformations during
//    CompiledExpr creation.
// 2. BoundExprStackTraceFactory: stored in CompiledExpr, it is essentially an
//    immutable version of ExprStackTrace.
// 3. BoundExprStackTrace: used to track Expr -> instruction pointer
//    transformations during CompiledExpr::Bind.
// 4. AnnotateEvaluationError: stored in BoundExpr, it is essentially an
//    immutable version of BoundExprStackTrace.
//
// Each of these parts is declared below (in reverse order due to dependencies).

// Function called in runtime to annotate an error with evaluation details.
using AnnotateEvaluationError =
    std::function<absl::Status(absl::Status status, int64_t failed_ip)>;

// Interface for a stack trace tracking Expr -> instruction pointer
// transformation during CompiledExpr::Bind.
class BoundExprStackTrace {
 public:
  virtual ~BoundExprStackTrace() = default;

  // Creates a link between an ip (instruction pointer) and an ExprNode.
  virtual void RegisterIp(int64_t ip, const ExprNodePtr& node) = 0;

  // Constructs a function that annotates an error with evaluation details in
  // runtime.
  virtual AnnotateEvaluationError Finalize() && = 0;
};

// A factory of BoundExprStackTrace that is stored in CompiledExpr.
using BoundExprStackTraceFactory =
    std::function<std::unique_ptr<BoundExprStackTrace>()>;

// Interface for a stack trace tracking Expr -> Expr transformation during
// CompiledExpr creation.
class ExprStackTrace {
 public:
  virtual ~ExprStackTrace() = default;

  // Records a traceback from a target node to a source node including a
  // transformation type.
  virtual void AddTrace(const ExprNodePtr& transformed_node,
                        const ExprNodePtr& original_node) = 0;

  // Records the source location of a node.
  virtual void AddSourceLocation(const ExprNodePtr& node,
                                 SourceLocationView source_location) = 0;

  // Annotates the given status with the source locations of the given node and
  // all its origins.
  //
  // Unlike AnnotateEvaluationError created by
  // std::move(expr_stack_trace).Finalize().Finalize(), this function is
  // intended to be used during expr compilation, and reports only the source
  // locations collected _so far_. We assume that `status` already contains all
  // the source locations created during compilation of `failed_node`.
  virtual absl::Status AnnotateWithNodeSourceLocations(
      absl::Status status, const ExprNodePtr& failed_node) const = 0;

  // Finalizes construction of the stack trace, returning a factory that can be
  // used to create a BoundExprStackTrace.
  virtual BoundExprStackTraceFactory Finalize() && = 0;
};

// Lightweight Expr stack trace tracks only original operator names.
class LightweightExprStackTrace : public ExprStackTrace {
 public:
  void AddTrace(const ExprNodePtr& transformed_node,
                const ExprNodePtr& original_node) final;

  void AddSourceLocation(const ExprNodePtr& node,
                         SourceLocationView source_location) final {};

  absl::Status AnnotateWithNodeSourceLocations(
      absl::Status status, const ExprNodePtr& failed_node) const final {
    return status;
  }

  BoundExprStackTraceFactory Finalize() && final;

  // Returns the original operator name for a given node fingerprint, or empty
  // string if the node was not registered.
  std::string GetOriginalOperatorName(Fingerprint fp) const;

 private:
  absl::flat_hash_map<Fingerprint, std::string> original_node_op_name_;
};

// Detailed Expr stack trace that tracks the transformation histories of nodes
// storing all intermediate node transformations.
class DetailedExprStackTrace : public ExprStackTrace {
 public:
  DetailedExprStackTrace()
      : shared_data_(std::make_shared<InternalSharedData>()) {}

  // Creates a traceback from a target node to a source node including a
  // transformation type. Will store every intermediate state of a node's
  // compilation, except when the transformation type is untraced.
  void AddTrace(const ExprNodePtr& transformed_node,
                const ExprNodePtr& original_node) final;

  // Records the source location of a node. The function expects that the node
  // might have been modified during compilation, and so it tries to recover the
  // originally annotated node first.
  void AddSourceLocation(const ExprNodePtr& node,
                         SourceLocationView source_location) final;

  absl::Status AnnotateWithNodeSourceLocations(
      absl::Status status, const ExprNodePtr& failed_node) const final;

  BoundExprStackTraceFactory Finalize() && final;

  // Internal data shared between all the DetailedBoundExprStackTrace instances
  // created from this instance.
  struct InternalSharedData {
    absl::flat_hash_map<Fingerprint, Fingerprint> traceback;
    absl::flat_hash_map<Fingerprint, SourceLocationPayload> source_locations;
  };

 private:
  LightweightExprStackTrace lightweight_stack_trace_;
  // The collected data will be shared between all the
  // DetailedBoundExprStackTrace instances created from this instance.
  std::shared_ptr<InternalSharedData> shared_data_;
  // Set of the nodes that are last in the traceback, i.e. first added as a
  // original_node and not transformed_node. We use them to avoid cycles in
  // shared_data_.traceback.
  absl::flat_hash_set<Fingerprint> original_nodes_;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_EXPR_STACK_TRACE_H_
