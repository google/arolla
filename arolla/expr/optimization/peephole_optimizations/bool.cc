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
#include "arolla/expr/optimization/peephole_optimizations/bool.h"

#include <array>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

namespace {

auto Matches(const std::vector<ExprNodePtr>& patterns) {
  absl::flat_hash_set<Fingerprint> pattern_prints;
  pattern_prints.reserve(patterns.size());
  for (const auto& p : patterns) {
    pattern_prints.insert(p->fingerprint());
  }
  return [pattern_prints(std::move(pattern_prints))](const ExprNodePtr& node) {
    return pattern_prints.contains(node->fingerprint());
  };
}

std::vector<ExprNodePtr> BoolLiterals(bool value) {
  return {Literal(value), Literal(MakeOptionalValue(value))};
}

constexpr std::array kComparisonOppositeOps = {
    std::pair{"bool.equal", "bool.not_equal"},
    std::pair{"bool.not_equal", "bool.equal"},
    // bool.greater and bool.greater_equal are not backend operators,
    // so we don't transform from them.
    std::pair{"bool.less", "bool.greater_equal"},
    std::pair{"bool.less_equal", "bool.greater"}};

// Remove `bool.logical_not`:
// 1. double negation
// 2. around comparison operation by replacing operation with an opposite one.
absl::Status LogicalNotComparisonOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  {  // double negation
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("bool.logical_not",
                        {CallOpReference("bool.logical_not", {a})}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, a));
  }
  for (auto [cmp1, cmp2] : kComparisonOppositeOps) {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("bool.logical_not", {CallOpReference(cmp1, {a, b})}));
    ASSIGN_OR_RETURN(ExprNodePtr to, CallOpReference(cmp2, {a, b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  return absl::OkStatus();
}

constexpr std::array kComparisonOps = {"equal", "not_equal", "less",
                                       "less_equal"};
constexpr std::array kLogicalOps = {"and", "or"};

// Optimization to collapse
// `core.equal(True, a)` -> `core.equal(a, True)`
//
// `core.equal(bool.{comparison_op}(a, b), True)` ->
// `core.{comparison_op}(a, b)`
//
// `core.equal(core.to_optional._scalar(bool.{comparison_op}(a, b)), True)` ->
// `core.{comparison_op}(a, b)`
//
// `core.equal(bool.logical_{and,or}(a, b), True)` ->
// `core.presence_{and,or}(core.equal(a, True), core.equal(b, True))`
// when one or both of a and b are either literal or bool comparison operation.
//
// Such patterns are often happen during automatic conversion from 3-bool
// to 2-bool logic.
absl::Status CoreBoolComparisonOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  ExprNodePtr d = Placeholder("d");
  ExprNodePtr true_ = Placeholder("true");
  std::vector<ExprNodePtr> true_literals = BoolLiterals(true);
  auto is_true = Matches(true_literals);

  {  // true == a -> a == true in order to reduce number of optimizations
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.equal", {true_, a}));
    ASSIGN_OR_RETURN(ExprNodePtr to, CallOpReference("core.equal", {a, true_}));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(
            from, to, {{"true", is_true}, {"a", std::not_fn(is_true)}}));
  }
  for (absl::string_view comparison_op : kComparisonOps) {
    ASSIGN_OR_RETURN(
        ExprNodePtr bool_cmp,
        CallOpReference(absl::StrCat("bool.", comparison_op), {a, b}));
    ASSIGN_OR_RETURN(
        ExprNodePtr core_cmp,
        CallOpReference(absl::StrCat("core.", comparison_op), {a, b}));
    {
      ASSIGN_OR_RETURN(ExprNodePtr from,
                       CallOpReference("core.equal", {bool_cmp, true_}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, core_cmp, {{"true", is_true}}));
    }
    {
      ASSIGN_OR_RETURN(
          ExprNodePtr from,
          CallOpReference(
              "core.equal",
              {CallOpReference("core.to_optional._scalar", {bool_cmp}),
               true_}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, core_cmp, {{"true", is_true}}));
    }
  }

  absl::flat_hash_set<std::string> bool_comparison_ops;
  for (absl::string_view comparison_op : kComparisonOps) {
    bool_comparison_ops.insert(absl::StrCat("bool.", comparison_op));
  }
  auto eq_true_will_be_optimized_further =
      [bool_comparison_ops](const ExprNodePtr& node) {
        if (node->is_literal()) return true;
        if (!node->is_op()) return false;
        return IsRegisteredOperator(node->op()) &&
               bool_comparison_ops.contains(node->op()->display_name());
      };
  for (absl::string_view logical_op : kLogicalOps) {
    ASSIGN_OR_RETURN(
        ExprNodePtr bool_logic,
        CallOpReference(absl::StrCat("bool.logical_", logical_op), {a, b}));
    ASSIGN_OR_RETURN(
        ExprNodePtr core_logic,
        CallOpReference(absl::StrCat("core.presence_", logical_op),
                        {CallOpReference("core.equal", {a, true_}),
                         CallOpReference("core.equal", {b, true_})}));
    {
      ASSIGN_OR_RETURN(ExprNodePtr from,
                       CallOpReference("core.equal", {bool_logic, true_}));

      ASSIGN_OR_RETURN(
          optimizations.emplace_back(),
          PeepholeOptimization::CreatePatternOptimization(
              from, core_logic,
              {{"true", is_true}, {"a", eq_true_will_be_optimized_further}}));
      ASSIGN_OR_RETURN(
          optimizations.emplace_back(),
          PeepholeOptimization::CreatePatternOptimization(
              from, core_logic,
              {{"true", is_true}, {"b", eq_true_will_be_optimized_further}}));
    }
  }

  return absl::OkStatus();
}

// Optimization to remove unused branches in `bool.logical_if`.
absl::Status LogicalIfOptimizations(PeepholeOptimizationPack& optimizations) {
  ExprNodePtr condition = Placeholder("condition");
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  auto is_scalar_bool = [](const ExprNodePtr& expr) {
    return expr->qtype() == GetQType<bool>();
  };
  ExprNodePtr true_ = Placeholder("true");
  std::vector<ExprNodePtr> true_literals = BoolLiterals(true);
  auto is_true = Matches(true_literals);

  ExprNodePtr false_ = Placeholder("false");
  std::vector<ExprNodePtr> false_literals = BoolLiterals(false);
  auto is_false = Matches(false_literals);
  // Case when cond is never missing or missing interpreted as false.
  // bool.logical_if(core.to_optional._scalar(cond), a, b, c)
  // AND
  // bool.logical_if(cond | false, a, b, c)
  // ->
  // core.where(cond == true, a, b)
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from1,
        CallOpReference(
            "bool.logical_if",
            {CallOpReference("core.to_optional._scalar", {condition}), a, b,
             c}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference(
            "core.where",
            {CallOpReference("core.equal", {condition, Literal(true)}), a, b}));

    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from1, to, {{"condition", is_scalar_bool}}));

    ASSIGN_OR_RETURN(ExprNodePtr from2,
                     CallOpReference("bool.logical_if",
                                     {CallOpReference("core.presence_or",
                                                      {condition, false_}),
                                      a, b, c}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from2, to, {{"false", is_false}}));
  }
  // Case when false and missing cases are identical.
  // bool.logical_if(cond, a, b, b) ->
  // core.where(cond == true, a, b)
  // Here we rely on the CoreBoolComparisonOptimizations that will happen
  // downstream and likely remove the `cond == true` part.
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("bool.logical_if", {condition, a, b, b}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference(
            "core.where",
            {CallOpReference("core.equal", {condition, Literal(true)}), a, b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  // Case when missing is interpreted as true.
  // bool.logical_if(cond | true, a, b, c) ->
  // core.where(cond == false, b, a)
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("bool.logical_if", {CallOpReference("core.presence_or",
                                                            {condition, true_}),
                                            a, b, c}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference(
            "core.where",
            {CallOpReference("core.equal", {condition, Literal(false)}), b,
             a}));

    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"true", is_true}}));
  }
  // Case when the 2 arguments after cond are true and false.
  // bool.logical_if(cond, true, false, a) ->
  // core.presence_or(cond, a)
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("bool.logical_if", {condition, true_, false_, a}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_or", {condition, a}));

    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"true", is_true}, {"false", is_false}}));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<PeepholeOptimizationPack> BoolOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(LogicalNotComparisonOptimizations(optimizations));
  RETURN_IF_ERROR(CoreBoolComparisonOptimizations(optimizations));
  RETURN_IF_ERROR(LogicalIfOptimizations(optimizations));
  return optimizations;
}

}  // namespace arolla::expr
