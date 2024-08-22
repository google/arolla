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
// A minimal set of operators required by the arolla/expr/eval compiler.
#ifndef AROLLA_EXPR_OPERATORS_BOOTSTRAP_OPERATORS_H_
#define AROLLA_EXPR_OPERATORS_BOOTSTRAP_OPERATORS_H_

#include "arolla/expr/operators/registration.h"

namespace arolla::expr_operators {

// Operators required by the expr/eval compiler.
AROLLA_DECLARE_EXPR_OPERATOR(CoreCast);
AROLLA_DECLARE_EXPR_OPERATOR(CoreCastValues);
AROLLA_DECLARE_EXPR_OPERATOR(CoreMap);
AROLLA_DECLARE_EXPR_OPERATOR(CoreToWeakFloat);

// Operators for qtype constraints and inference expressions.
AROLLA_DECLARE_EXPR_OPERATOR(CoreApplyVarargs);
AROLLA_DECLARE_EXPR_OPERATOR(CoreEqual);
AROLLA_DECLARE_EXPR_OPERATOR(CoreConcatTuples);
AROLLA_DECLARE_EXPR_OPERATOR(CoreMakeTuple);
AROLLA_DECLARE_EXPR_OPERATOR(CoreMapTuple);
AROLLA_DECLARE_EXPR_OPERATOR(CoreReduceTuple);
AROLLA_DECLARE_EXPR_OPERATOR(CoreGetNth);
AROLLA_DECLARE_EXPR_OPERATOR(CoreZip);
AROLLA_DECLARE_EXPR_OPERATOR(CoreNotEqual);
AROLLA_DECLARE_EXPR_OPERATOR(CorePresenceAnd);
AROLLA_DECLARE_EXPR_OPERATOR(CorePresenceOr);

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_BOOTSTRAP_OPERATORS_H_
