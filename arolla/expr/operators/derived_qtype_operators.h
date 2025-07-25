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
#ifndef AROLLA_EXPR_OPERATORS_DERIVED_QTYPE_OPERATORS_H_
#define AROLLA_EXPR_OPERATORS_DERIVED_QTYPE_OPERATORS_H_

#include "arolla/expr/expr_operator.h"

namespace arolla::expr_operators {

// derived_qtype.upcast(derived_qtype_literal, value)
expr::ExprOperatorPtr MakeDerivedQTypeUpcastOp();

// derived_qtype.downcast(derived_qtype_literal, value)
expr::ExprOperatorPtr MakeDerivedQTypeDowncastOp();

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_DERIVED_QTYPE_OPERATORS_H_
