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
#ifndef AROLLA_EXPR_OPERATORS_REGISTER_OPERATORS_H_
#define AROLLA_EXPR_OPERATORS_REGISTER_OPERATORS_H_

#include "absl/status/status.h"
#include "arolla/expr/operators/registration.h"

namespace arolla::expr_operators {

// Initialize "core" operators.
absl::Status InitCore();

// Initialize "array" operators.
absl::Status InitArray();

// Initialize "math" operators.
absl::Status InitMath();

// The functions below initialize individual operators and return Status of the
// registration.

AROLLA_DECLARE_EXPR_OPERATOR(CoreEmptyLike);
AROLLA_DECLARE_EXPR_OPERATOR(CorePresenceAndOr);
AROLLA_DECLARE_EXPR_OPERATOR(CoreShortCircuitWhere);

AROLLA_DECLARE_EXPR_OPERATOR(ArrayTake);

// go/keep-sorted start
AROLLA_DECLARE_EXPR_OPERATOR(MathSoftmax);
AROLLA_DECLARE_EXPR_OPERATOR(Math_Add4);
// go/keep-sorted end

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_REGISTER_OPERATORS_H_
