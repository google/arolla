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
#include "arolla/expr/optimization/peephole_optimizations/presence.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/optimization/peephole_optimizer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr {

namespace presence_impl {

bool IsPresenceType(const ExprNodePtr& expr) {
  QTypePtr qtype = expr->qtype();
  return qtype != nullptr && qtype == GetPresenceQType(qtype).value_or(nullptr);
}

bool IsAlwaysPresentType(const ExprNodePtr& expr) {
  return IsScalarQType(expr->qtype());
}

bool IsAlwaysPresentOptionalValue(const ExprNodePtr& expr) {
  const auto& optional_qvalue = expr->qvalue();
  return optional_qvalue.has_value() &&
         IsOptionalQType(optional_qvalue->GetType()) &&
         UnsafeIsPresent(optional_qvalue->AsRef());
}

bool IsAlwaysPresent(const ExprNodePtr& expr) {
  return IsAlwaysPresentType(expr) || IsAlwaysPresentOptionalValue(expr);
}

bool IsAlwaysAbsentOptionalValue(const ExprNodePtr& expr) {
  const auto& optional_qvalue = expr->qvalue();
  return optional_qvalue.has_value() &&
         IsOptionalQType(optional_qvalue->GetType()) &&
         !UnsafeIsPresent(optional_qvalue->AsRef());
}

}  // namespace presence_impl
namespace {

using ::arolla::expr::presence_impl::IsAlwaysAbsentOptionalValue;
using ::arolla::expr::presence_impl::IsAlwaysPresent;
using ::arolla::expr::presence_impl::IsAlwaysPresentOptionalValue;
using ::arolla::expr::presence_impl::IsAlwaysPresentType;
using ::arolla::expr::presence_impl::IsPresenceType;

bool IsLiteral(const ExprNodePtr& node) { return node->is_literal(); }

bool IsOptionalLikeNode(const ExprNodePtr& node) {
  QTypePtr qtype = node->qtype();
  return qtype != nullptr && IsOptionalLikeQType(qtype);
}

bool IsBaseQType(const ExprNodePtr& node) {
  return IsScalarQType(DecayOptionalQType(node->qtype()));
}

// Optimization to remove `core.has`.
absl::Status HasRemovalOptimizations(PeepholeOptimizationPack& optimizations) {
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.has._optional", {Placeholder("a")}));
    ExprNodePtr to = Literal(kPresent);
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"a", IsAlwaysPresentOptionalValue}}));
  }
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.presence_not._builtin",
                                     {CallOpReference("core.has._optional",
                                                      {Placeholder("a")})}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_not", {Placeholder("a")}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.presence_not._builtin",
                                     {CallOpReference("core.has._array",
                                                      {Placeholder("a")})}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_not", {Placeholder("a")}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(
            "core.has._optional",
            {CallOpReference("core.to_optional._scalar", {Placeholder("a")})}));
    ExprNodePtr to = Literal(kPresent);
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"a", IsAlwaysPresentType}}));
  }
  return absl::OkStatus();
}

// Optimization to remove `core.presence_and`.
absl::Status PresenceAndRemovalOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.presence_and", {a, b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, a, {{"b", IsAlwaysPresentType}}));
  }
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("core.presence_not._builtin",
                        {CallOpReference("core.presence_and", {a, b})}));
    ASSIGN_OR_RETURN(ExprNodePtr to, CallOpReference("core.presence_not", {b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"a", IsAlwaysPresent}}));
  }
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.presence_and", {a, b}));
    ASSIGN_OR_RETURN(ExprNodePtr to, CallOpReference("core.to_optional", {a}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"b", IsAlwaysPresentOptionalValue}}));
  }
  {
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.presence_and", {a, b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, b, {{"a", [](const ExprNodePtr& expr) {
                                      return IsAlwaysPresent(expr) &&
                                             IsPresenceType(expr);
                                    }}}));
  }
  return absl::OkStatus();
}

// Optimization to remove `core.presence_or`.
absl::Status PresenceOrRemovalOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ASSIGN_OR_RETURN(ExprNodePtr from,
                   CallOpReference("core.presence_or", {a, b}));
  ASSIGN_OR_RETURN(optimizations.emplace_back(),
                   PeepholeOptimization::CreatePatternOptimization(
                       from, a, {{"a", IsAlwaysPresentType}}));
  return absl::OkStatus();
}

// Optimization to propagate `core.has` inside of the other operations.
absl::Status HasPropagationOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  auto is_literal_or_presence = [](const ExprNodePtr& expr) {
    return IsLiteral(expr) || IsPresenceType(expr);
  };
  for (const char* op_has : {"core.has._optional", "core.has._array"}) {
    for (const auto& op : {"core.presence_or", "core.presence_and"}) {
      ASSIGN_OR_RETURN(ExprNodePtr from,
                       CallOpReference(op_has, {CallOpReference(op, {a, b})}));
      ASSIGN_OR_RETURN(ExprNodePtr to,
                       CallOpReference(op, {CallOpReference("core.has", {a}),
                                            CallOpReference("core.has", {b})}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, to, {{"a", is_literal_or_presence}}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, to, {{"b", is_literal_or_presence}}));
    }
    {
      ASSIGN_OR_RETURN(
          ExprNodePtr from,
          CallOpReference(
              op_has, {CallOpReference("core._presence_and_or", {a, c, b})}));
      ASSIGN_OR_RETURN(ExprNodePtr to,
                       CallOpReference("core._presence_and_or",
                                       {CallOpReference("core.has", {a}), c,
                                        CallOpReference("core.has", {b})}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, to, {{"a", is_literal_or_presence}}));
      ASSIGN_OR_RETURN(optimizations.emplace_back(),
                       PeepholeOptimization::CreatePatternOptimization(
                           from, to, {{"b", is_literal_or_presence}}));
    }
  }
  return absl::OkStatus();
}

// to_optional(x_optional | default_literal) ->
// x_optional | to_optional(default_literal)
// optimization is useful, when `x | default` is immediately used as
// optional.
absl::Status ToOptionalPropagationOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("core.to_optional._scalar",
                        {CallOpReference("core.presence_or", {a, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference("core.presence_or",
                        {a, CallOpReference("core.to_optional", {b})}));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(
            from, to, {{"a", IsOptionalLikeNode}, {"b", IsLiteral}}));
  }
  {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("core.to_optional._scalar",
                        {CallOpReference("core._presence_and_or", {a, c, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference("core._presence_and_or",
                        {CallOpReference("core.to_optional", {a}), c,
                         CallOpReference("core.to_optional", {b})}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"a", IsLiteral}}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"b", IsLiteral}}));
  }
  return absl::OkStatus();
}

absl::Status PresenceAndOptionalOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr c = Placeholder("c");
  {  // to_optional(P.a) & P.c  -> P.a & P.c
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("core.presence_and",
                        {CallOpReference("core.to_optional._scalar", {a}), c}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_and", {a, c}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  return absl::OkStatus();
}

absl::Status PresenceAndOrCombinationOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  ExprNodePtr d = Placeholder("d");
  {  // (P.c & P.a) | (P.c & P.b)  -> P.c & (P.a | P.b)
    ASSIGN_OR_RETURN(
        ExprNodePtr from1,
        CallOpReference("core.presence_or",
                        {CallOpReference("core.presence_and", {c, a}),
                         CallOpReference("core.presence_and", {c, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr from2,
        CallOpReference("core._presence_and_or",
                        {c, a, CallOpReference("core.presence_and", {c, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference("core.presence_and",
                        {c, CallOpReference("core.presence_or", {a, b})}));
    for (const auto& from : {from1, from2}) {
      ASSIGN_OR_RETURN(
          optimizations.emplace_back(),
          PeepholeOptimization::CreatePatternOptimization(from, to));
    }
  }
  {  // (P.d | (P.c & P.a)) | (P.c & P.b)  -> P.d | (P.c & (P.a | P.b))
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(
            "core.presence_or",
            {CallOpReference("core.presence_or",
                             {
                                 d,
                                 CallOpReference("core.presence_and", {c, a}),
                             }),
             CallOpReference("core.presence_and", {c, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference(
            "core.presence_or",
            {d, CallOpReference(
                    "core.presence_and",
                    {c, CallOpReference("core.presence_or", {a, b})})}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  return absl::OkStatus();
}

absl::Status WhereOptimizations(PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  {  // (P.a & P.c) | (P.b & ~P.c)  -> where(P.c, P.a, P.b)
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(
            "core.presence_or",
            {CallOpReference("core.presence_and", {a, c}),
             CallOpReference(
                 "core.presence_and",
                 {b, CallOpReference("core.presence_not._builtin", {c})})}));
    ASSIGN_OR_RETURN(ExprNodePtr to, CallOpReference("core.to_optional", {
                     CallOpReference("core.where", {c, a, b})}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"c", IsOptionalLikeNode}}));
  }
  {  // _presence_and_or(P.a, P.c, P.b & ~P.c)  -> where(P.c, P.a, P.b)
     // only for optionals and primitive types.
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(
            "core._presence_and_or",
            {a, c,
             CallOpReference(
                 "core.presence_and",
                 {b, CallOpReference("core.presence_not._builtin", {c})})}));
    ASSIGN_OR_RETURN(ExprNodePtr to, CallOpReference("core.where", {c, a, b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(from, to));
  }
  {  // (P.a & P.c) | P.b  -> _presence_and_or(P.a, P.c, P.b)
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference("core.presence_or",
                        {CallOpReference("core.presence_and", {a, c}), b}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core._presence_and_or", {a, c, b}));
    ASSIGN_OR_RETURN(
        optimizations.emplace_back(),
        PeepholeOptimization::CreatePatternOptimization(
            from, to,
            {{"a", IsBaseQType}, {"b", IsBaseQType}, {"c", IsBaseQType}}));
  }
  {  // where(P.c, P.a, P.b) -> a if c is always present
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.where", {c, a, b}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, a,
                         {{"c", IsAlwaysPresent},
                          {"a", IsAlwaysPresentType},
                          {"b", IsAlwaysPresentType}}));
  }
  return absl::OkStatus();
}

absl::Status WhereToPresenceAndOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  {  // where(c, a, b) -> presence_and(a, c) if b is nullopt
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core.where", {c, a, b}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_and", {a, c}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"b", IsAlwaysAbsentOptionalValue}}));
  }
  return absl::OkStatus();
}

absl::Status PresenceAndOrOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  {  // _presence_and_or(P.a, P.b, P.c)  -> P.a | P.c (if b is always present)
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core._presence_and_or", {a, b, c}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_or", {a, c}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"b", IsAlwaysPresent}}));
  }
  {  // _presence_and_or(P.a, P.b, P.c)  -> P.b | P.c
     // (if a is always present and IsPresenceType)
    ASSIGN_OR_RETURN(ExprNodePtr from,
                     CallOpReference("core._presence_and_or", {a, b, c}));
    ASSIGN_OR_RETURN(ExprNodePtr to,
                     CallOpReference("core.presence_or", {b, c}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"a", [](const ExprNodePtr& expr) {
                                       return IsAlwaysPresent(expr) &&
                                              IsPresenceType(expr);
                                     }}}));
  }
  return absl::OkStatus();
}

// Propagate some operations inside where if one of the branches is literal.
absl::Status InsideWherePropagationOptimizations(
    PeepholeOptimizationPack& optimizations) {
  ExprNodePtr a = Placeholder("a");
  ExprNodePtr b = Placeholder("b");
  ExprNodePtr c = Placeholder("c");
  for (const auto& [op_from, op_to] :
       std::vector<std::pair<std::string, std::string>>{
           {"core.to_optional._scalar", "core.to_optional"},
           {"core.has._optional", "core.has"},
           {"core.has._array", "core.has"},
       }) {
    ASSIGN_OR_RETURN(
        ExprNodePtr from,
        CallOpReference(op_from, {CallOpReference("core.where", {c, a, b})}));
    ASSIGN_OR_RETURN(
        ExprNodePtr to,
        CallOpReference("core.where", {c, CallOpReference(op_to, {a}),
                                       CallOpReference(op_to, {b})}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"a", IsLiteral}}));
    ASSIGN_OR_RETURN(optimizations.emplace_back(),
                     PeepholeOptimization::CreatePatternOptimization(
                         from, to, {{"b", IsLiteral}}));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<PeepholeOptimizationPack> PresenceOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(HasRemovalOptimizations(optimizations));
  RETURN_IF_ERROR(PresenceAndRemovalOptimizations(optimizations));
  RETURN_IF_ERROR(PresenceOrRemovalOptimizations(optimizations));
  RETURN_IF_ERROR(HasPropagationOptimizations(optimizations));
  RETURN_IF_ERROR(ToOptionalPropagationOptimizations(optimizations));
  RETURN_IF_ERROR(PresenceAndOptionalOptimizations(optimizations));
  RETURN_IF_ERROR(PresenceAndOrCombinationOptimizations(optimizations));
  RETURN_IF_ERROR(WhereOptimizations(optimizations));
  RETURN_IF_ERROR(InsideWherePropagationOptimizations(optimizations));
  RETURN_IF_ERROR(PresenceAndOrOptimizations(optimizations));
  return optimizations;
}

absl::StatusOr<PeepholeOptimizationPack> CodegenPresenceOptimizations() {
  PeepholeOptimizationPack optimizations;
  RETURN_IF_ERROR(WhereToPresenceAndOptimizations(optimizations));
  return optimizations;
}

}  // namespace arolla::expr
