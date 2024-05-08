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
#ifndef THIRD_PARTY_PY_AROLLA_ABC_PYBIND11_UTILS_H_
#define THIRD_PARTY_PY_AROLLA_ABC_PYBIND11_UTILS_H_

// IWYU pragma: always_keep // See pybind11/docs/type_caster_iwyu.rst

#include <Python.h>

#include "absl/log/check.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_fingerprint.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/pybind11.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla::python {

// A wrapper for PyModule_AddFunctions().
template <const PyMethodDef&... PY_METHOD_DEFs>
void pybind11_module_add_functions(pybind11::module_ m) {
  static PyMethodDef py_method_defs[] = {PY_METHOD_DEFs..., {nullptr}};
  if (PyModule_AddFunctions(m.ptr(), py_method_defs) < 0) {
    // NOTE: We generally avoid using C++ exceptions. However, we have to use
    // them in pybind11-specific code.
    throw pybind11::error_already_set();
  }
}

// A helper for absl::Status handling.
inline void pybind11_throw_if_error(const absl::Status& status) {
  if (!status.ok()) {
    ::arolla::python::SetPyErrFromStatus(status);
    // NOTE: We generally avoid using C++ exceptions. However, we have to use
    // them in pybind11-specific code.
    throw pybind11::error_already_set();
  }
}

// A helper for absl::Status handling.
template <typename T>
T&& pybind11_unstatus_or(absl::StatusOr<T>&& status_or) {
  pybind11_throw_if_error(status_or.status());
  return *std::move(status_or);
}

// A wrapper for pybind11::reinterpret_steal<T>() that treats `nullptr` as an
// indication of a Python error. It's convenient when we need to work directly
// with the Python C API.
template <typename T>
T pybind11_steal_or_throw(PyObject* py_obj) {
  if (py_obj == nullptr) {
    DCHECK(PyErr_Occurred());
    // NOTE: We generally avoid using C++ exceptions. However, we have to use
    // them in pybind11-specific code.
    throw pybind11::error_already_set();
  }
  DCHECK(!PyErr_Occurred());
  return pybind11::reinterpret_steal<T>(py_obj);
}

// A variant of pybind11_steal_or_throw<T>() that works with PyTypeObject.
template <typename T>
T pybind11_steal_or_throw(PyTypeObject* py_type) {
  return pybind11_steal_or_throw<T>(reinterpret_cast<PyObject*>(py_type));
}

}  // namespace arolla::python

namespace pybind11::detail {

template <>
class type_caster<::arolla::Fingerprint> {
 public:
  PYBIND11_TYPE_CASTER(::arolla::Fingerprint, const_name("Fingerprint"));

  bool load(handle src, bool) {
    if (::arolla::python::IsPyFingerprintInstance(src.ptr())) {
      value = ::arolla::python::UnsafeUnwrapPyFingerprint(src.ptr());
      return true;
    }
    return false;
  }

  static handle cast(::arolla::Fingerprint src, return_value_policy, handle) {
    return ::arolla::python::pybind11_steal_or_throw<object>(
               ::arolla::python::WrapAsPyFingerprint(std::move(src)))
        .release();
  }
};

template <>
class type_caster<::arolla::TypedValue> {
 public:
  PYBIND11_TYPE_CASTER(::arolla::TypedValue, const_name("QValue"));

  // NOTE: Since ::arolla::TypedValue lacks a default constructor, we need to
  // manually implement the default constructor for this type_caster<>.
  // In the unlikely event that this implementation becomes a bottleneck,
  // we can consider inlining the macro PYBIND11_TYPE_CASTER.
  type_caster() : value(::arolla::GetUnspecifiedQValue()) {}

  bool load(handle src, bool) {
    if (::arolla::python::IsPyQValueInstance(src.ptr())) {
      value = ::arolla::python::UnsafeUnwrapPyQValue(src.ptr());
      return true;
    }
    return false;
  }

  static handle cast(::arolla::TypedValue src, return_value_policy, handle) {
    return ::arolla::python::pybind11_steal_or_throw<object>(
               ::arolla::python::WrapAsPyQValue(std::move(src)))
        .release();
  }
};

template <>
class type_caster<::arolla::QType> {
 public:
  // NOTE: Since we actually need a type_caster<> for a pointer to const QType,
  // we cannot rely on the macro PYBIND11_TYPE_CASTER and have to deal with
  // the undocumented type_caster<> internals.

  static constexpr auto name = const_name("QType");

  template <typename T>
  using cast_op_type = ::arolla::QTypePtr;

  bool load(handle src, bool) {
    if (::arolla::python::IsPyQValueInstance(src.ptr())) {
      auto& typed_value = ::arolla::python::UnsafeUnwrapPyQValue(src.ptr());
      if (typed_value.GetType() == ::arolla::GetQType<::arolla::QTypePtr>()) {
        value_ = typed_value.UnsafeAs<::arolla::QTypePtr>();
        return true;
      }
    }
    return false;
  }

  operator ::arolla::QTypePtr() const {
    DCHECK_NE(value_, nullptr);
    return value_;
  }

  static handle cast(::arolla::QTypePtr src, return_value_policy, handle) {
    return ::arolla::python::pybind11_steal_or_throw<object>(
               ::arolla::python::WrapAsPyQValue(
                   ::arolla::TypedValue::FromValue(std::move(src))))
        .release();
  }

 protected:
  const arolla::QType* value_ = nullptr;
};

template <>
class type_caster<::arolla::expr::ExprOperatorPtr> {
 public:
  PYBIND11_TYPE_CASTER(::arolla::expr::ExprOperatorPtr, const_name("Operator"));

  bool load(handle src, bool) {
    if (::arolla::python::IsPyQValueInstance(src.ptr())) {
      auto& typed_value = ::arolla::python::UnsafeUnwrapPyQValue(src.ptr());
      if (typed_value.GetType() ==
          ::arolla::GetQType<::arolla::expr::ExprOperatorPtr>()) {
        value = typed_value.UnsafeAs<::arolla::expr::ExprOperatorPtr>();
        return true;
      }
    }
    return false;
  }

  static handle cast(::arolla::expr::ExprOperatorPtr src, return_value_policy,
                     handle) {
    return ::arolla::python::pybind11_steal_or_throw<object>(
               ::arolla::python::WrapAsPyQValue(
                   ::arolla::TypedValue::FromValue(std::move(src))))
        .release();
  }
};

template <>
class type_caster<::arolla::expr::ExprNodePtr> {
 public:
  PYBIND11_TYPE_CASTER(::arolla::expr::ExprNodePtr, const_name("Expr"));

  bool load(handle src, bool) {
    if (::arolla::python::IsPyExprInstance(src.ptr())) {
      value = ::arolla::python::UnwrapPyExpr(src.ptr());
      return true;
    }
    return false;
  }

  static handle cast(::arolla::expr::ExprNodePtr src, return_value_policy,
                     handle) {
    return ::arolla::python::pybind11_steal_or_throw<object>(
               ::arolla::python::WrapAsPyExpr(std::move(src)))
        .release();
  }
};

template <>
class type_caster<::arolla::expr::ExprOperatorSignature> {
 public:
  PYBIND11_TYPE_CASTER(::arolla::expr::ExprOperatorSignature,
                       const_name("OperatorSignature"));

  bool load(handle src, bool) {
    if (::arolla::python::UnwrapPySignature(src.ptr(), &value)) {
      return true;
    }
    PyErr_Clear();
    return false;
  }

  static handle cast(const ::arolla::expr::ExprOperatorSignature& src,
                     return_value_policy, handle) {
    return ::arolla::python::pybind11_steal_or_throw<object>(
               ::arolla::python::WrapAsPySignature(src))
        .release();
  }
};

}  // namespace pybind11::detail

#endif  // THIRD_PARTY_PY_AROLLA_ABC_PYBIND11_UTILS_H_
