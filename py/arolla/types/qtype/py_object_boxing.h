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
#ifndef PY_AROLLA_ABC_PY_OBJECT_BOXING_H_
#define PY_AROLLA_ABC_PY_OBJECT_BOXING_H_

#include <Python.h>

#include <functional>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

using PyObjectEncodingFn =
    std::function<absl::StatusOr<std::string>(PyObject*, absl::string_view)>;
using PyObjectDecodingFn = std::function<absl::StatusOr<PyObject*>(
    absl::string_view, absl::string_view)>;

// Returns a python object wrapped as PyObjectQValue.
//
// NOTE: If the argument is already a qvalue instances, the function raises
// ValueError.
absl::StatusOr<TypedValue> BoxPyObject(PyObject* object,
                                       std::optional<std::string> codec);

// Returns the python object stored in a PyObjectQValue.
absl::StatusOr<PyObject*> UnboxPyObject(const TypedValue& value);

// Decodes the provided data representing a PyObject into a PyObjectQValue.
absl::StatusOr<TypedValue> DecodePyObject(absl::string_view data,
                                          std::string codec);

// Returns a serialized python object stored in a PyObjectQValue instance.
absl::StatusOr<std::string> EncodePyObject(TypedRef value);

// Returns the codec stored in a PyObjectQValue instance.
absl::StatusOr<std::optional<std::string>> GetPyObjectCodec(TypedRef value);

// Returns the qtype of a wrapped python object.
QTypePtr GetPyObjectQType();

// Registers a function used to deserialize python objects. The function should
// take a serialized python object and a serialization codec (string) and return
// the deserialized object.
void RegisterPyObjectDecodingFn(PyObjectDecodingFn fn);

// Registers a function used to serialize python objects. The function should
// take a PyObject* and a serialization codec (string) and return a serialized
// representation of the provided object.
void RegisterPyObjectEncodingFn(PyObjectEncodingFn fn);

}  // namespace arolla::python

#endif  // PY_AROLLA_ABC_PY_OBJECT_BOXING_H_
