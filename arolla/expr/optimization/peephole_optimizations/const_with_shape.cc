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
#include "arolla/expr/optimization/peephole_optimizations/const_with_shape.h"

#include <initializer_list>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {
namespace {

// NOTE: not using absl::string_view because it is broken in gcc:
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92181
struct OpRecord {
  const char* const from_op;
  const char* const to_op;
};

constexpr std::initializer_list<OpRecord> kUnaryPointwiseOps = {
    // go/keep-sorted start
    {"bool.logical_not", "bool.logical_not"},
    {"core.has._array", "core.has"},
    {"core.has._optional", "core.has"},
    {"core.presence_not._builtin", "core.presence_not"},
    {"core.to_bool", "core.to_bool"},
    {"core.to_float32", "core.to_float32"},
    {"core.to_float64", "core.to_float64"},
    {"core.to_int32", "core.to_int32"},
    {"core.to_int64", "core.to_int64"},
    {"core.to_optional._scalar", "core.to_optional"},
    {"core.to_uint64", "core.to_uint64"},
    {"math.abs", "math.abs"},
    {"math.ceil", "math.ceil"},
    {"math.exp", "math.exp"},
    {"math.expm1", "math.expm1"},
    {"math.floor", "math.floor"},
    {"math.is_finite", "math.is_finite"},
    {"math.is_inf", "math.is_inf"},
    {"math.is_nan", "math.is_nan"},
    {"math.log", "math.log"},
    {"math.log10", "math.log10"},
    {"math.log1p", "math.log1p"},
    {"math.log2", "math.log2"},
    {"math.logit", "math.logit"},
    {"math.neg", "math.neg"},
    {"math.pos", "math.pos"},
    {"math.round", "math.round"},
    {"math.sigmoid", "math.sigmoid"},
    {"math.sign", "math.sign"},
    // go/keep-sorted end
};

constexpr std::initializer_list<OpRecord> kBinaryPointwiseOps = {
    // go/keep-sorted start
    {"bool.equal", "bool.equal"},
    {"bool.less", "bool.less"},
    {"bool.less_equal", "bool.less_equal"},
    {"bool.logical_and", "bool.logical_and"},
    {"bool.logical_or", "bool.logical_or"},
    {"bool.not_equal", "bool.not_equal"},
    {"core.equal", "core.equal"},
    {"core.less", "core.less"},
    {"core.less_equal", "core.less_equal"},
    {"core.not_equal", "core.not_equal"},
    {"core.presence_and", "core.presence_and"},
    {"core.presence_or", "core.presence_or"},
    {"math._pow", "math.pow"},
    {"math.add", "math.add"},
    {"math.divide", "math.divide"},
    {"math.floordiv", "math.floordiv"},
    {"math.fmod", "math.fmod"},
    {"math.max", "math.max"},
    {"math.min", "math.min"},
    {"math.mod", "math.mod"},
    {"math.multiply", "math.multiply"},
    {"math.subtract", "math.subtract"},
    // go/keep-sorted end
};

absl::Status AddUnaryPointwiseOpOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr value = Placeholder("value");
  ExprNodePtr shape = Placeholder("shape");
  for (const auto& [from_op, to_op] : kUnaryPointwiseOps) {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(from_op,
                        {CallOpReference("core.const_with_shape._array_shape",
                                         {shape, value})}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.const_with_shape",
                                     {shape, CallOpReference(to_op, {value})}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  return absl::OkStatus();
}

bool IsBaseQType(const ExprNodePtr& node) {
  return IsScalarQType(DecayOptionalQType(node->qtype()));
}

absl::Status AddBinaryPointwiseOpOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr shape = Placeholder("shape");
  for (const auto& [from_op, to_op] : kBinaryPointwiseOps) {
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.const_with_shape",
                                     {shape, CallOpReference(to_op, {a, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr expanded_a,
        CallOpReference("core.const_with_shape._array_shape", {shape, a}));
    ASSIGN_OR_RETURN(
        ExprNodePtr expanded_b,
        CallOpReference("core.const_with_shape._array_shape", {shape, b}));

    {  // binary operation for two constants expanded to the same shape.
      ASSIGN_OR_RETURN(ExprNodePtr from,
                       CallOpReference(from_op, {expanded_a, expanded_b}));
      ASSIGN_OR_RETURN(
          optimizations.emplace_back(),
          PeepholeOptimization::CreatePatternOptimization(from, to));
    }
    // binary operation for two constants: one expanded and one not.
    {
      ASSIGN_OR_RETURN(ExprNodePtr from,
                       CallOpReference(from_op, {expanded_a, b}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, to, {{"b", IsBaseQType}}));
    }
    {
      ASSIGN_OR_RETURN(ExprNodePtr from,
                       CallOpReference(from_op, {a, expanded_b}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, to, {{"a", IsBaseQType}}));
    }
  }
  return absl::OkStatus();
}

absl::Status AddArrayShapeOfOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr shape = Placeholder("shape");
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(
            "core._array_shape_of",
            {CallOpReference(
                "core.has._array",
                {CallOpReference("core.const_with_shape._array_shape",
                                 {shape, a})})}));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(from, shape));
  }
  {
    // If `a` is UNIT, the previous optimizations could already remove
    // `core.has`, so we handle that case specially.
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("core._array_shape_of",
                        {CallOpReference("core.const_with_shape._array_shape",
                                         {shape, a})}));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(from, shape));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<PeepholeOptimizationPack> ConstWithShapeOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(AddArrayShapeOfOptimizations(optimizations));
  RETURN_IF_ERROR(AddUnaryPointwiseOpOptimizations(optimizations));
  RETURN_IF_ERROR(AddBinaryPointwiseOpOptimizations(optimizations));
  return optimizations;
}

}  // namespace arolla::expr
