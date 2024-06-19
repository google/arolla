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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_CACHED_EVAL_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_CACHED_EVAL_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// Compiles and evaluates the given expression with the given inputs. The
// compilation is cached and shared among similar functions.
absl::StatusOr<TypedValue> EvalExprWithCompilationCache(
    const expr::ExprNodePtr& expr, absl::Span<const std::string> input_names,
    absl::Span<const TypedRef> input_qvalues);

// Invokes the given operator on the given inputs. The compilation is cached and
// shared among similar functions.
absl::StatusOr<TypedValue> InvokeOpWithCompilationCache(
    expr::ExprOperatorPtr op, absl::Span<const TypedRef> input_qvalues);

// Clears the shared compilation cache.
void ClearCompilationCache();

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_CACHED_EVAL_H_
