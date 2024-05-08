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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_QVALUE_SPECIALIZATION_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_QVALUE_SPECIALIZATION_H_

#include <Python.h>

#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// Assigns PyQValue subclass for the given qtype.
//
// A subsequent call with the same qtype overrides the previous specialisation.
//
// Returns `true` on success, otherwise returns `false` and sets a python
// exception.
[[nodiscard]] bool RegisterPyQValueSpecializationByQType(
    QTypePtr qtype, PyObject* qvalue_subtype);

// Removes PyQValue subclass assignment for the given qtype.
//
// Returns `true` on success, otherwise returns `false` and sets a python
// exception.
[[nodiscard]] bool RemovePyQValueSpecializationByQType(QTypePtr qtype);

// Assigns PyQValue subclass for the given qvalue specialization key.
//
// A subsequent call with the same key overrides the previous specialisation.
//
// Returns `true` on success, otherwise returns `false` and sets a python
// exception.
[[nodiscard]] bool RegisterPyQValueSpecializationByKey(
    absl::string_view key, PyObject* qvalue_subtype);

// Removes PyQValue subclass assignment for the given specialization key.
//
// Returns `true` on success, otherwise returns `false and sets a python
// exception.
[[nodiscard]] bool RemovePyQValueSpecializationByKey(absl::string_view key);

// The dispatching algorithm for qvalue specializations:
//
// * If a value is a qtype:
//   (default) use PyQType type.
//
// * If a value is not a qtype:
//   (p0) lookup based on the qvalue_specialization_key of the value
//   (p1) lookup based on the value *qtype*
//   (p2) lookup based on the qvalue_specialization_key of the value qtype
//   (default) use PyQValue type.
//
// Motivation of the algorithm steps:
//
//   p0 -- enables fine grained dispatching for values of generic qtypes, like
//         ExprOperator
//   p1 -- helps with static qtypes, like the standard scalars/optionals/arrays
//   p2 -- works for dynamic qtype families, like ::arolla::TupleQType, when
//         there is no need in a qvalue_specialized_key at the value level

// Selects PyQValue specialization corresponding to the given typed_value, and
// instantiates it.
//
// Returns a PyQValue on success, otherwise returns nullptr and
// sets a python exception.
PyObject* WrapAsPyQValue(TypedValue&& typed_value);

inline PyObject* WrapAsPyQValue(const TypedValue& typed_value) {
  return WrapAsPyQValue(TypedValue(typed_value));
}

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_QVALUE_SPECIALIZATION_H_
