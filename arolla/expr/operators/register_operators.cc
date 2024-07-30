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
// Declares available operators.
//
// For example, operator 'add' is declared like this:
//
// auto binary_arithmetic = Chain( // Applies its arguments one after another.
//     // Takes two inputs.
//     Binary,
//     // If one of the inputs is array, broadcasts the other one to the same
//     // shape.
//     Broadcast,
//     // Both inputs are scalar or optional numbers, or arrays of those.
//     Numeric,
//     // Returns the common type for the inputs. See documentation for
//     // CastingRegistry::CommonType.
//     CommonType);
//
// For available methods to define type rules, see type_meta_eval_strategies.h.
//
// RETURN_IF_ERROR(RegisterBackendOperator(
//     // Registers the operator under name math.add.
//     "math.add",
//     // Signature of the operator. It is used for arguments verification and
//     // for the corresponding function signature in Python.
//     ExprOperatorSignature{{"x"}, {"y"}},
//     // Verifies operator input types and provides the output type for
//     // given input types.
//     binary_arithmetic));

#include "arolla/expr/operators/register_operators.h"

#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/operators/aggregation.h"
#include "arolla/expr/operators/factory_operators.h"
#include "arolla/expr/operators/registration.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/unit.h"

namespace arolla::expr_operators {

using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::RegisterOperator;
using ::arolla::expr_operators::type_meta::Chain;
using ::arolla::expr_operators::type_meta::Is;
using ::arolla::expr_operators::type_meta::Nth;
using ::arolla::expr_operators::type_meta::NthApply;
using ::arolla::expr_operators::type_meta::NthMatch;
using ::arolla::expr_operators::type_meta::Or;
using ::arolla::expr_operators::type_meta::PresenceOrType;
using ::arolla::expr_operators::type_meta::ScalarOrOptional;
using ::arolla::expr_operators::type_meta::ScalarTypeIs;
using ::arolla::expr_operators::type_meta::Ternary;
using ::arolla::expr_operators::type_meta::ToOptional;

AROLLA_DEFINE_EXPR_OPERATOR(
    CorePresenceAndOr,
    RegisterBackendOperator(
        "core._presence_and_or", ExprOperatorSignature{{"a"}, {"c"}, {"b"}},
        Chain(Ternary, ScalarOrOptional, NthMatch(1, Chain(ScalarTypeIs<Unit>)),
              Or(NthMatch(1, Is<Unit>), NthApply(0, ToOptional)), Nth({0, 2}),
              PresenceOrType)));

AROLLA_DEFINE_EXPR_OPERATOR(CoreEmptyLike, RegisterOperator("core.empty_like",
                                                            MakeEmptyLikeOp()));

AROLLA_DEFINE_EXPR_OPERATOR(ArrayTake,
                            RegisterOperator<TakeOperator>("array.take"));

}  // namespace arolla::expr_operators
