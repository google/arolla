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
#ifndef THIRD_PARTY_PY_AROLLA_TYPES_S11N_PY_OBJECT_DECODER_H_
#define THIRD_PARTY_PY_AROLLA_TYPES_S11N_PY_OBJECT_DECODER_H_

#include <Python.h>

#include <functional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

using PyObjectDecodingFn = std::function<absl::StatusOr<PyObject*>(
    absl::string_view, absl::string_view)>;

// Registers a function used to deserialize python objects. The function should
// take a serialized python object and a serialization codec (string) and return
// the deserialized object.
void RegisterPyObjectDecodingFn(PyObjectDecodingFn fn);

// Decodes the provided data representing a PyObject into a PyObjectQValue.
absl::StatusOr<TypedValue> DecodePyObject(absl::string_view data,
                                          std::string codec);

// Initialize PyObjectCodec decoder.
absl::Status InitPyObjectCodecDecoder();

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_TYPES_S11N_PY_OBJECT_DECODER_H_
