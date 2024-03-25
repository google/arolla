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
#include "py/arolla/abc/py_signature.h"

#include <Python.h>

#include <cstddef>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprOperatorSignature;

constexpr absl::string_view kPositionalOrKeyword = "positional-or-keyword";
constexpr absl::string_view kVariadicPositional = "variadic-positional";

PyTypeObject PySignature_Type;

PyStructSequence_Field PySignature_fields[] = {
    {"parameters", "(tuple[Parameter, ...])A list of parameters."},
    {"aux_policy",
     "(str) An auxiliary policy (see ExprOperatorSignature for additional "
     "information)."},
    {nullptr}};

PyStructSequence_Desc PySignature_desc = {
    .name = "arolla.abc.Signature",
    .doc = "A cross-language representation of inspect.Signature.",
    .fields = PySignature_fields,
    .n_in_sequence = 2,
};

PyTypeObject PyParameter_Type;

PyStructSequence_Field PyParameter_fields[] = {
    {"name", "(str) Parameter name."},
    {"kind",
     "(str) Specifies how the arguments are bound to the parameter: "
     "'positional-only', 'positional-or-keyword', 'variadic-positional', "
     "'keyword-only', 'variadic-keyword'."},
    {"default", "(QValue|None) The default value for the parameter."},
    {nullptr},
};

PyStructSequence_Desc PyParameter_desc = {
    .name = "arolla.abc.SignatureParameter",
    .doc = "Type for arolla.abc.Signature parameters.",
    .fields = PyParameter_fields,
    .n_in_sequence = 3,
};

bool Init() {
  if (PySignature_Type.tp_name == nullptr) {
    if (PyStructSequence_InitType2(&PySignature_Type, &PySignature_desc) < 0) {
      PyErr_Format(PyExc_TypeError, "failed to initialize %s type",
                   PySignature_desc.name);
      return false;
    }
  }
  if (PyParameter_Type.tp_name == nullptr) {
    if (PyStructSequence_InitType2(&PyParameter_Type, &PyParameter_desc) < 0) {
      PyErr_Format(PyExc_TypeError, "failed to initialize %s type",
                   PyParameter_desc.name);
      return false;
    }
  }
  return true;
}

PyObject* WrapAsPyParameter(absl::string_view name, absl::string_view kind,
                            const std::optional<TypedValue>& default_value) {
  auto result = PyObjectPtr::Own(PyStructSequence_New(&PyParameter_Type));
  if (result == nullptr) {
    return nullptr;
  }
  if (auto* py_str = PyUnicode_FromStringAndSize(name.data(), name.size())) {
    PyStructSequence_SET_ITEM(result.get(), 0, py_str);
  } else {
    return nullptr;
  }
  if (auto* py_str = PyUnicode_FromStringAndSize(kind.data(), kind.size())) {
    PyStructSequence_SET_ITEM(result.get(), 1, py_str);
  } else {
    return nullptr;
  }
  if (default_value.has_value()) {
    if (auto* py_qvalue = WrapAsPyQValue(*default_value)) {
      PyStructSequence_SET_ITEM(result.get(), 2, py_qvalue);
    } else {
      return nullptr;
    }
  } else {
    PyStructSequence_SET_ITEM(result.get(), 2, Py_NewRef(Py_None));
  }
  return result.release();
}

}  // namespace

PyTypeObject* PySignatureType() {
  DCheckPyGIL();
  if (!Init()) {
    return nullptr;
  }
  Py_INCREF(&PySignature_Type);
  return &PySignature_Type;
}

PyTypeObject* PySignatureParameterType() {
  DCheckPyGIL();
  if (!Init()) {
    return nullptr;
  }
  Py_INCREF(&PyParameter_Type);
  return &PyParameter_Type;
}

PyObject* WrapAsPySignature(const Signature& signature) {
  DCheckPyGIL();
  if (!Init()) {
    return nullptr;
  }
  auto py_parameters =
      PyObjectPtr::Own(PyTuple_New(signature.parameters.size()));
  size_t i = 0;
  for (const auto& param : signature.parameters) {
    if (auto* py_param =
            WrapAsPyParameter(param.name, param.kind, param.default_value)) {
      PyTuple_SET_ITEM(py_parameters.get(), i++, py_param);
    } else {
      return nullptr;
    }
  }
  auto result = PyObjectPtr::Own(PyStructSequence_New(&PySignature_Type));
  if (result == nullptr) {
    return nullptr;
  }
  PyStructSequence_SET_ITEM(result.get(), 0, py_parameters.release());
  if (auto* py_str = PyUnicode_FromStringAndSize(signature.aux_policy.data(),
                                                 signature.aux_policy.size())) {
    PyStructSequence_SET_ITEM(result.get(), 1, py_str);
  } else {
    return nullptr;
  }
  return result.release();
}

PyObject* WrapAsPySignature(const ExprOperatorSignature& signature) {
  DCheckPyGIL();
  if (!Init()) {
    return nullptr;
  }
  auto py_parameters =
      PyObjectPtr::Own(PyTuple_New(signature.parameters.size()));
  size_t i = 0;
  for (const auto& param : signature.parameters) {
    absl::string_view kind;
    switch (param.kind) {
      case ExprOperatorSignature::Parameter::Kind::kPositionalOrKeyword:
        kind = kPositionalOrKeyword;
        break;
      case ExprOperatorSignature::Parameter::Kind::kVariadicPositional:
        kind = kVariadicPositional;
        break;
    }
    if (auto* py_param =
            WrapAsPyParameter(param.name, kind, param.default_value)) {
      PyTuple_SET_ITEM(py_parameters.get(), i++, py_param);
    } else {
      return nullptr;
    }
  }
  auto result = PyObjectPtr::Own(PyStructSequence_New(&PySignature_Type));
  if (result == nullptr) {
    return nullptr;
  }
  PyStructSequence_SET_ITEM(result.get(), 0, py_parameters.release());
  if (auto* py_str = PyUnicode_FromStringAndSize(signature.aux_policy.data(),
                                                 signature.aux_policy.size())) {
    PyStructSequence_SET_ITEM(result.get(), 1, py_str);
  } else {
    return nullptr;
  }
  return result.release();
}

namespace {

// Returns true and sets the result output parameter if the function is
// successful (or returns false and sets a python exception).
bool UnwrapPyParameter(
    absl::Nonnull<PyObject*> py_parameter, size_t i,
    absl::Nonnull<absl::string_view*> name,
    absl::Nonnull<absl::string_view*> kind,
    absl::Nonnull<std::optional<TypedValue>*> default_value) {
  DCheckPyGIL();
  if (!PyTuple_Check(py_parameter)) {
    PyErr_Format(PyExc_TypeError,
                 "expected a parameter, got signature.parameters[%zu]: %s", i,
                 Py_TYPE(py_parameter)->tp_name);
    return false;
  }
  if (PyTuple_GET_SIZE(py_parameter) != 3) {
    PyErr_Format(PyExc_ValueError,
                 "expected len(signature.parameters[%zu])=3, got %zu", i,
                 PyTuple_GET_SIZE(py_parameter));
    return false;
  }
  PyObject* const py_name = PyTuple_GET_ITEM(py_parameter, 0);
  Py_ssize_t name_size = 0;
  if (const char* name_data = PyUnicode_AsUTF8AndSize(py_name, &name_size)) {
    *name = absl::string_view(name_data, name_size);
  } else {
    PyErr_Clear();
    PyErr_Format(PyExc_TypeError,
                 "expected a string, got signature.parameters[%zu].name: %s", i,
                 Py_TYPE(py_name)->tp_name);
    return false;
  }

  PyObject* const py_kind = PyTuple_GET_ITEM(py_parameter, 1);
  Py_ssize_t kind_size = 0;
  if (const char* kind_data = PyUnicode_AsUTF8AndSize(py_kind, &kind_size)) {
    *kind = absl::string_view(kind_data, kind_size);
  } else {
    PyErr_Clear();
    PyErr_Format(PyExc_TypeError,
                 "expected a string, got signature.parameters[%zu].kind: %s", i,
                 Py_TYPE(py_kind)->tp_name);
    return false;
  }
  PyObject* const py_default = PyTuple_GET_ITEM(py_parameter, 2);
  if (py_default == Py_None) {
    default_value->reset();
  } else if (auto* qvalue = UnwrapPyQValue(py_default)) {
    default_value->emplace(*qvalue);
  } else {
    PyErr_Clear();
    PyErr_Format(
        PyExc_TypeError,
        "expected QValue|None, got signature.parameters[%zu].default: %s", i,
        Py_TYPE(py_default)->tp_name);
    return false;
  }
  return true;
}

}  // namespace

bool UnwrapPySignature(absl::Nonnull<PyObject*> py_signature,
                       absl::Nonnull<Signature*> result) {
  DCheckPyGIL();
  if (!Init()) {
    return false;
  }
  if (!PyTuple_Check(py_signature)) {
    PyErr_Format(PyExc_TypeError, "expected a signature, got %s",
                 Py_TYPE(py_signature)->tp_name);
    return false;
  }
  if (PyTuple_GET_SIZE(py_signature) != 2) {
    PyErr_Format(PyExc_ValueError, "expected len(signature)=2, got %d",
                 PyTuple_GET_SIZE(py_signature));
    return false;
  }
  PyObject* const py_parameters = PyTuple_GET_ITEM(py_signature, 0);
  if (!PyTuple_Check(py_parameters)) {
    PyErr_Format(
        PyExc_TypeError,
        "expected tuple[SignatureParameter, ...], got signature.parameters: %s",
        Py_TYPE(py_parameters)->tp_name);
    return false;
  }
  result->parameters.resize(PyTuple_GET_SIZE(py_parameters));
  size_t i = 0;
  for (auto& param : result->parameters) {
    absl::string_view name, kind;
    if (!UnwrapPyParameter(PyTuple_GET_ITEM(py_parameters, i), i, &name, &kind,
                           &param.default_value)) {
      return false;
    }
    param.name.assign(name.data(), name.size());
    param.kind.assign(kind.data(), kind.size());
    ++i;
  }
  PyObject* const py_aux_policy = PyTuple_GET_ITEM(py_signature, 1);
  Py_ssize_t aux_policy_size = 0;
  if (const char* aux_policy_data =
          PyUnicode_AsUTF8AndSize(py_aux_policy, &aux_policy_size)) {
    result->aux_policy.assign(aux_policy_data, aux_policy_size);
  } else {
    PyErr_Clear();
    PyErr_Format(PyExc_TypeError,
                 "expected a string, got signature.aux_policy: %s",
                 Py_TYPE(py_aux_policy)->tp_name);
    return false;
  }
  return result;
}

bool UnwrapPySignature(absl::Nonnull<PyObject*> py_signature,
                       absl::Nonnull<ExprOperatorSignature*> result) {
  DCheckPyGIL();
  if (!Init()) {
    return false;
  }
  if (!PyTuple_Check(py_signature)) {
    PyErr_Format(PyExc_TypeError, "expected a signature, got %s",
                 Py_TYPE(py_signature)->tp_name);
    return false;
  }
  if (PyTuple_GET_SIZE(py_signature) != 2) {
    PyErr_Format(PyExc_ValueError, "expected len(signature)=2, got %zu",
                 PyTuple_GET_SIZE(py_signature));
    return false;
  }
  PyObject* const py_parameters = PyTuple_GET_ITEM(py_signature, 0);
  if (!PyTuple_Check(py_parameters)) {
    PyErr_Format(
        PyExc_TypeError,
        "expected tuple[SignatureParameter, ...], got signature.parameters: %s",
        Py_TYPE(py_parameters)->tp_name);
    return false;
  }
  result->parameters.resize(PyTuple_GET_SIZE(py_parameters));
  size_t i = 0;
  for (auto& param : result->parameters) {
    absl::string_view name, kind;
    if (!UnwrapPyParameter(PyTuple_GET_ITEM(py_parameters, i), i, &name, &kind,
                           &param.default_value)) {
      return false;
    }
    param.name.assign(name.data(), name.size());
    if (kind == kPositionalOrKeyword) {
      param.kind = ExprOperatorSignature::Parameter::Kind::kPositionalOrKeyword;
    } else if (kind == kVariadicPositional) {
      param.kind = ExprOperatorSignature::Parameter::Kind::kVariadicPositional;
    } else {
      PyErr_Format(
          PyExc_ValueError,
          "expected '%s' or '%s', got signature.parameters[%zu].kind='%s'",
          std::string(kPositionalOrKeyword).c_str(),
          std::string(kVariadicPositional).c_str(), i,
          absl::Utf8SafeCHexEscape(kind).c_str());
      return false;
    }
    ++i;
  }
  PyObject* const py_aux_policy = PyTuple_GET_ITEM(py_signature, 1);
  Py_ssize_t aux_policy_size = 0;
  if (const char* aux_policy_data =
          PyUnicode_AsUTF8AndSize(py_aux_policy, &aux_policy_size)) {
    result->aux_policy.assign(aux_policy_data, aux_policy_size);
  } else {
    PyErr_Clear();
    PyErr_Format(PyExc_TypeError,
                 "expected a string, got signature.aux_policy: %s",
                 Py_TYPE(py_aux_policy)->tp_name);
    return false;
  }
  return result;
}

}  // namespace arolla::python
