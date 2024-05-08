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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_MISC_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_MISC_H_

#include <Python.h>

// This file serves as an umbrella for Python functions implemented using the
// Python C API that didn't fit into other files. The typical pathway for a
// function to end up in this file is for it to start as a function implemented
// using pybind11 and then be reimplemented at a lower level.

namespace arolla::python {

// Definitions of the functions:
// go/keep-sorted start block=yes newline_separated=yes
// def check_registered_operator_presence(...)
extern const PyMethodDef kDefPyCheckRegisteredOperatorPresence;

// def decay_registered_operator(...)
extern const PyMethodDef kDefPyDecayRegisteredOperator;

// def deep_transform(...)
extern const PyMethodDef kDefPyDeepTransform;

// def get_field_qtypes(...)
extern const PyMethodDef kDefPyGetFieldQTypes;

// def get_operator_doc(...)
extern const PyMethodDef kDefPyGetOperatorDoc;

// def get_operator_name(...)
extern const PyMethodDef kDefPyGetOperatorName;

// def get_operator_signature(...)
extern const PyMethodDef kDefPyGetOperatorSignature;

// def get_registry_revision_id(...)
extern const PyMethodDef kDefPyGetRegistryRevisionId;

// def is_annotation_operator(...)
extern const PyMethodDef kDefPyIsAnnotationOperator;

// def leaf(...)
extern const PyMethodDef kDefPyLeaf;

// def literal(...)
extern const PyMethodDef kDefPyLiteral;

// def placeholder(...)
extern const PyMethodDef kDefPyPlaceholder;

// def to_lower_node(...)
extern const PyMethodDef kDefPyToLowerNode;

// def to_lowest(...)
extern const PyMethodDef kDefPyToLowest;

// def transform(...)
extern const PyMethodDef kDefPyTransform;

// def unsafe_make_registered_operator(...)
extern const PyMethodDef kDefPyUnsafeMakeRegisteredOperator;

// def unspecified(...)
extern const PyMethodDef kDefPyUnspecified;

// go/keep-sorted end

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_MISC_H_
