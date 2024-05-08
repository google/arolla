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
#ifndef THIRD_PARTY_PY_AROLLA_TYPES_QTYPE_ARRAY_BOXING_H_
#define THIRD_PARTY_PY_AROLLA_TYPES_QTYPE_ARRAY_BOXING_H_

#include <Python.h>

namespace arolla::python {

// Initialize the array boxing subsystem.
[[nodiscard]] bool InitArrayBoxing();

// go/keep-sorted start newline_separated=yes
// def dense_array_boolean_from_values(...)
extern const PyMethodDef kDefPyDenseArrayBooleanFromValues;

// def dense_array_bytes_from_values(...)
extern const PyMethodDef kDefPyDenseArrayBytesFromValues;

// def dense_array_float32_from_values(...)
extern const PyMethodDef kDefPyDenseArrayFloat32FromValues;

// def dense_array_float64_from_values(...)
extern const PyMethodDef kDefPyDenseArrayFloat64FromValues;

// def dense_array_int32_from_values(...)
extern const PyMethodDef kDefPyDenseArrayInt32FromValues;

// def dense_array_int64_from_values(...)
extern const PyMethodDef kDefPyDenseArrayInt64FromValues;

// def dense_array_text_from_values(...)
extern const PyMethodDef kDefPyDenseArrayTextFromValues;

// def dense_array_uint64_from_values(...)
extern const PyMethodDef kDefPyDenseArrayUInt64FromValues;

// def dense_array_unit_from_values(...)
extern const PyMethodDef kDefPyDenseArrayUnitFromValues;

// def dense_array_weak_float_from_values(...)
extern const PyMethodDef kDefPyDenseArrayWeakFloatFromValues;

// def get_array_item(...)
extern const PyMethodDef kDefPyGetArrayItem;

// def get_array_py_value(...)
extern const PyMethodDef kDefPyGetArrayPyValue;
// go/keep-sorted end

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_TYPES_QTYPE_ARRAY_BOXING_H_
