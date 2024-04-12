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
#ifndef PY_AROLLA_TYPES_S11N_PY_OBJECT_ENCODER_H_
#define PY_AROLLA_TYPES_S11N_PY_OBJECT_ENCODER_H_

#include <Python.h>

#include <functional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/typed_ref.h"

namespace arolla::python {

using PyObjectEncodingFn =
    std::function<absl::StatusOr<std::string>(PyObject*, absl::string_view)>;

// Registers a function used to serialize python objects. The function should
// take a PyObject* and a serialization codec (string) and return a serialized
// representation of the provided object.
void RegisterPyObjectEncodingFn(PyObjectEncodingFn fn);

// Returns a serialized python object stored in a PyObjectQValue instance.
absl::StatusOr<std::string> EncodePyObject(TypedRef value);

// Initialize PyObjectCodec encoder.
absl::Status InitPyObjectCodecEncoder();

}  // namespace arolla::python

#endif  // PY_AROLLA_TYPES_S11N_PY_OBJECT_ENCODER_H_
