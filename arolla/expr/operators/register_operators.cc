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
#include "arolla/expr/operators/register_operators.h"

#include "arolla/expr/operators/aggregation.h"
#include "arolla/expr/operators/factory_operators.h"
#include "arolla/expr/operators/registration.h"
#include "arolla/expr/registered_expr_operator.h"

namespace arolla::expr_operators {

using ::arolla::expr::RegisterOperator;

AROLLA_DEFINE_EXPR_OPERATOR(CoreEmptyLike, RegisterOperator("core.empty_like",
                                                            MakeEmptyLikeOp()));

AROLLA_DEFINE_EXPR_OPERATOR(ArrayTake,
                            RegisterOperator<TakeOperator>("array.take"));

}  // namespace arolla::expr_operators
