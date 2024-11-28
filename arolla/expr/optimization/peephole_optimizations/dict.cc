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
#include "arolla/expr/optimization/peephole_optimizations/dict.h"

#include <memory>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

absl::StatusOr<std::unique_ptr<PeepholeOptimization>> BoolDictOptimization() {
  ExprNodePtr dict = Placeholder("dict");

  // Pattern
  ASSIGN_OR_RETURN(
      ExprNodePtr pattern,
      CallOpReference("array.at", {Placeholder("values"),
                                   CallOpReference("dict._get_row",
                                                   {dict, Placeholder("p")})}));
  // Replacement
  ASSIGN_OR_RETURN(
      ExprNodePtr true_value,
      CallOpReference("array.at", {Placeholder("values"),
                                   CallOpReference("dict._get_row",

                                                   {dict, Literal(true)})}));
  ASSIGN_OR_RETURN(
      ExprNodePtr false_value,
      CallOpReference("array.at", {Placeholder("values"),
                                   CallOpReference("dict._get_row",

                                                   {dict, Literal(false)})}));
  ASSIGN_OR_RETURN(ExprNodePtr missing_value,
                   CallOpReference("core.empty_like", {true_value}));
  ASSIGN_OR_RETURN(
      ExprNodePtr replacement,
      CallOpReference("bool.logical_if", {Placeholder("p"), true_value,
                                          false_value, missing_value}));

  auto is_bool_literal = [](const ExprNodePtr& node) {
    return node->qvalue().has_value() &&
           node->qtype() == GetKeyToRowDictQType<bool>();
  };
  auto is_not_literal = [](const ExprNodePtr& node) {
    return !node->qvalue().has_value();
  };
  return PeepholeOptimization::CreatePatternOptimization(
      pattern, replacement, {{"dict", is_bool_literal}, {"p", is_not_literal}});
}

absl::Status AddDictContainsOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ASSIGN_OR_RETURN(ExprNodePtr replacement,
                   CallOpReference("dict._contains",
                                   {Placeholder("dict"), Placeholder("x")}));
  for (const char* op_has : {"core.has._optional", "core.has._array"}) {
    {
      ASSIGN_OR_RETURN(
          ExprNodePtr pattern,
          CallOpReference(
              "core.presence_and",
              {CallOpReference(op_has, {Placeholder("x")}),
               CallOpReference("dict._contains",
                               {Placeholder("dict"), Placeholder("x")})}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           pattern, replacement));
    }
    {
      ASSIGN_OR_RETURN(
          ExprNodePtr pattern,
          CallOpReference(
              "core.presence_and",
              {CallOpReference("dict._contains",
                               {Placeholder("dict"), Placeholder("x")}),
               CallOpReference(op_has, {Placeholder("x")})}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           pattern, replacement));
    }
  }
  return absl::OkStatus();
}

}  // namespace

//
absl::StatusOr<PeepholeOptimizationPack> DictOptimizations() {
  PeepholeOptimizationPack optimizations;
  ASSIGN_OR_RETURN(optimizations.emplace_back(), BoolDictOptimization());
  RETURN_IF_ERROR(AddDictContainsOptimizations(optimizations));
  return optimizations;
}

}  // namespace arolla::expr
