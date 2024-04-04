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
#ifndef PY_AROLLA_TYPES_QTYPE_SCALAR_BOXING_H_
#define PY_AROLLA_TYPES_QTYPE_SCALAR_BOXING_H_

#include <Python.h>

#include <cstdint>
#include <optional>

#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla::python {

// Initialize the scalar boxing subsystem.
[[nodiscard]] bool InitScalarBoxing();

// Returns an exception type MissingOptionalError.
extern PyObject* PyExc_MissingOptionalError;

// Parses an optional scalar value. Returns `std::nullopt` on error; please use
// PyErr_Occurred() to distinguish between an error and a missing value.
// go/keep-sorted start
std::optional<Bytes> ParsePyBytes(PyObject* py_arg);
std::optional<Text> ParsePyText(PyObject* py_arg);
std::optional<Unit> ParsePyUnit(PyObject* py_arg);
std::optional<bool> ParsePyBoolean(PyObject* py_arg);
std::optional<double> ParsePyFloat(PyObject* py_arg);
std::optional<int32_t> ParsePyInt32(PyObject* py_arg);
std::optional<int64_t> ParsePyInt64(PyObject* py_arg);
std::optional<uint64_t> ParsePyUInt64(PyObject* py_arg);
// go/keep-sorted end

// go/keep-sorted start newline_separated=yes
// def py_boolean(...)
extern const PyMethodDef kDefPyValueBoolean;

// def py_bytes(...)
extern const PyMethodDef kDefPyValueBytes;

// def py_float(...)
extern const PyMethodDef kDefPyValueFloat;

// def py_index(...)
extern const PyMethodDef kDefPyValueIndex;

// def py_text(...)
extern const PyMethodDef kDefPyValueText;

// def py_unit(...)
extern const PyMethodDef kDefPyValueUnit;
// go/keep-sorted end

// Definitions of the functions:

// go/keep-sorted start newline_separated=yes
// def boolean(...)
extern const PyMethodDef kDefPyBoolean;

// def bytes(...)
extern const PyMethodDef kDefPyBytes;

// def float32(...)
extern const PyMethodDef kDefPyFloat32;

// def float64(...)
extern const PyMethodDef kDefPyFloat64;

// def int32(...)
extern const PyMethodDef kDefPyInt32;

// def int64(...)
extern const PyMethodDef kDefPyInt64;

// def text(...)
extern const PyMethodDef kDefPyText;

// def uint64(...)
extern const PyMethodDef kDefPyUInt64;

// def unit(...)
extern const PyMethodDef kDefPyUnit;

// def weak_float(...)
extern const PyMethodDef kDefPyWeakFloat;
// go/keep-sorted end

// go/keep-sorted start block=yes newline_separated=yes
// def optional_boolean(...)
extern const PyMethodDef kDefPyOptionalBoolean;

// def optional_bytes(...)
extern const PyMethodDef kDefPyOptionalBytes;

// def optional_float32(...)
extern const PyMethodDef kDefPyOptionalFloat32;

// def optional_float64(...)
extern const PyMethodDef kDefPyOptionalFloat64;

// def optional_int32(...)
extern const PyMethodDef kDefPyOptionalInt32;

// def optional_int64(...)
extern const PyMethodDef kDefPyOptionalInt64;

// def optional_text(...)
extern const PyMethodDef kDefPyOptionalText;

// def optional_uint64(...)
extern const PyMethodDef kDefPyOptionalUInt64;

// def optional_unit(...)
extern const PyMethodDef kDefPyOptionalUnit;

// def optional_weak_float(...)
extern const PyMethodDef kDefPyOptionalWeakFloat;
// go/keep-sorted end

}  // namespace arolla::python

#endif  // PY_AROLLA_TYPES_QTYPE_SCALAR_BOXING_H_
