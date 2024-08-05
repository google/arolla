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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_OBJECT_QTYPE_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_OBJECT_QTYPE_H_

#include <optional>
#include <string>

#include "absl/status/statusor.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// Returns PY_OBJECT qtype.
//
// NOTE: This function does not require Python GIL to be locked.
QTypePtr GetPyObjectQType();

// Returns a python object wrapped as QValue.
absl::StatusOr<TypedValue> MakePyObjectQValue(PyObjectPtr object,
                                              std::optional<std::string> codec);

// Returns a borrowed pointer to the Python object stored in PyObjectQValue.
//
// NOTE: This function does not require Python GIL to be locked.
absl::StatusOr<std::reference_wrapper<const PyObjectGILSafePtr>>
GetPyObjectValue(TypedRef qvalue);

// Returns the codec stored in a PyObjectQValue instance.
//
// NOTE: This function does not require Python GIL to be locked.
absl::StatusOr<std::optional<std::string>> GetPyObjectCodec(TypedRef qvalue);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_OBJECT_QTYPE_H_
