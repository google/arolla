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
// Utility operators, which are useful for implementation or wrapping up in
// other operators.
// These operators are typically not registered in global registry and created
// directly.

#ifndef AROLLA_QEXPR_CORE_UTILITY_OPERATORS_H_
#define AROLLA_QEXPR_CORE_UTILITY_OPERATORS_H_

#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// CopyOperator copies value from the input slot to the output slots.
OperatorPtr MakeCopyOp(QTypePtr type);

}  // namespace arolla

#endif  // AROLLA_QEXPR_CORE_UTILITY_OPERATORS_H_
