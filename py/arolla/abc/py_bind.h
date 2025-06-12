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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_BIND_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_BIND_H_

#include <Python.h>

namespace arolla::python {

// Definitions of the functions:

// def make_operator_node(...)
extern const PyMethodDef kDefPyMakeOperatorNode;

// def unsafe_make_operator_node(...)
extern const PyMethodDef kDefPyUnsafeMakeOperatorNode;

// def bind_op(...)
extern const PyMethodDef kDefPyBindOp;

// def aux_bind_arguments(...)
extern const PyMethodDef kDefPyAuxBindArguments;

// def aux_bind_op(...)
extern const PyMethodDef kDefPyAuxBindOp;

// def aux_get_python_signature(...)
extern const PyMethodDef kDefPyAuxGetPythonSignature;

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_BIND_H_
