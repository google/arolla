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

#ifndef THIRD_PARTY_PY_AROLLA_ABC_PY_SIGNATURE_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PY_SIGNATURE_H_

#include <Python.h>

#include <optional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {

// A cross-language representation of inspect.Signature.
//
// Note: This representation allows "positional-only", "keyword-only", and
// "variadic-keyword" parameters, which are not supported by
// ::arolla::expr::ExprOperatorSignature.
struct Signature {
  struct Parameter {
    std::string name;
    std::string kind;
    std::optional<TypedValue> default_value;
  };
  std::vector<Parameter> parameters;
  std::string aux_policy;
};

// Returns PySignature type (or nullptr and sets a python exception).
PyTypeObject* PySignatureType();

// Returns PySignatureParameter type (or nullptr and sets a python exception).
PyTypeObject* PySignatureParameterType();

// Returns PySignature object (or nullptr and sets a python exception).
PyObject* WrapAsPySignature(const Signature& signature);

// Returns PySignature object (or nullptr and sets a python exception).
PyObject* WrapAsPySignature(
    const arolla::expr::ExprOperatorSignature& signature);

// Returns true and sets the result output parameter if the function is
// successful (or returns false and sets a python exception).
bool UnwrapPySignature(absl::Nonnull<PyObject*> py_signature,
                       absl::Nonnull<Signature*> result);

// Returns true and sets the result output parameter if the function is
// successful (or returns false and sets a python exception).
bool UnwrapPySignature(
    absl::Nonnull<PyObject*> py_signature,
    absl::Nonnull<arolla::expr::ExprOperatorSignature*> result);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PY_SIGNATURE_H_
