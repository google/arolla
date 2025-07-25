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
// IMPORTANT: All the following functions assume that the current thread is
// ready to call the Python C API. You can find extra information in
// documentation for PyGILState_Ensure() and PyGILState_Release().

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_EVAL_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_EVAL_H_

#include <Python.h>

namespace arolla::python {

// Definitions of the functions:

// def invoke_op(...)
extern const PyMethodDef kDefPyInvokeOp;

// def eval_expr(...)
extern const PyMethodDef kDefPyEvalExpr;

// def aux_eval_op(...)
extern const PyMethodDef kDefPyAuxEvalOp;

// def clear_eval_compile_cache(...)
extern const PyMethodDef kDefPyClearEvalCompileCache;

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_ATTR_H_
