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
#include "py/arolla/abc/py_qvalue_specialization.h"

#include <Python.h>

#include <string>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_expr_quote.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/indestructible.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprQuote;

class QValueSpecializationRegistry {
 public:
  static QValueSpecializationRegistry& instance() {
    static Indestructible<QValueSpecializationRegistry> result;
    return *result;
  }

  static PyTypeObject* CheckPyQValueSubtype(PyObject* qvalue_subtype) {
    if (!PyType_Check(qvalue_subtype)) {
      PyErr_Format(PyExc_TypeError, "expected subclass of QValue, got %R",
                   qvalue_subtype);
      return nullptr;
    }
    auto* const type = reinterpret_cast<PyTypeObject*>(qvalue_subtype);
    if (!IsPyQValueSubtype(type)) {
      PyErr_Format(PyExc_ValueError, "expected subclass of QValue, got %s",
                   type->tp_name);
      return nullptr;
    }
    Py_INCREF(type);
    return type;
  }

  bool RegisterSpecializationByQType(QTypePtr qtype, PyObject* qvalue_subtype) {
    DCHECK_NE(qtype, nullptr);
    if (qtype == GetQTypeQType()) {
      PyErr_SetString(PyExc_ValueError,
                      "QValue specialization for QTYPE cannot be changed");
      return false;
    }
    if (qtype == GetQType<ExprQuote>()) {
      PyErr_SetString(PyExc_ValueError,
                      "QValue specialization for EXPR_QUOTE cannot be changed");
      return false;
    }
    PyTypeObject* type = CheckPyQValueSubtype(qvalue_subtype);
    if (type == nullptr) {
      return false;
    }
    const auto& [it, success] = qtype_based_registry_.try_emplace(qtype, type);
    if (!success) {
      Py_DECREF(it->second);
      it->second = type;
    }
    return true;
  }

  bool RegisterSpecializationByKey(absl::string_view key,
                                   PyObject* qvalue_subtype) {
    if (key.empty()) {
      PyErr_SetString(PyExc_ValueError, "key is empty");
      return false;
    }
    PyTypeObject* type = CheckPyQValueSubtype(qvalue_subtype);
    if (type == nullptr) {
      return false;
    }
    const auto& [it, success] = key_based_registry_.try_emplace(key, type);
    if (!success) {
      Py_DECREF(it->second);
      it->second = type;
    }
    return true;
  }

  bool RemoveSpecializationByQType(QTypePtr qtype) {
    DCHECK_NE(qtype, nullptr);
    if (qtype == GetQTypeQType()) {
      PyErr_SetString(PyExc_ValueError,
                      "QValue specialization for QTYPE cannot be changed");
      return false;
    }
    if (qtype == GetQType<ExprQuote>()) {
      PyErr_SetString(PyExc_ValueError,
                      "QValue specialization for EXPR_QUOTE cannot be changed");
      return false;
    }
    qtype_based_registry_.erase(qtype);
    return true;
  }

  bool RemoveSpecializationByKey(absl::string_view key) {
    key_based_registry_.erase(key);
    return true;
  }

  PyObject* WrapAsPyQValue(TypedValue&& typed_value) const {
    PyTypeObject* type = nullptr;
    absl::Cleanup type_cleanup = [&] { Py_XDECREF(type); };
    if (type == nullptr) {
      if (typed_value.GetType() == GetQTypeQType()) {
        type = PyQTypeType();
        if (type == nullptr) {
          return nullptr;
        }
      } else if (typed_value.GetType() == GetQType<ExprQuote>()) {
        type = PyExprQuoteType();
        if (type == nullptr) {
          return nullptr;
        }
      }
    }
    if (type == nullptr) {
      if (auto key = typed_value.PyQValueSpecializationKey(); !key.empty()) {
        auto it = key_based_registry_.find(key);
        if (it != key_based_registry_.end()) {
          type = it->second;
          Py_INCREF(type);
        }
      }
    }
    if (type == nullptr) {
      auto it = qtype_based_registry_.find(typed_value.GetType());
      if (it != qtype_based_registry_.end()) {
        type = it->second;
        Py_INCREF(type);
      }
    }
    if (type == nullptr) {
      if (auto key = typed_value.GetType()->qtype_specialization_key();
          !key.empty()) {
        auto it = key_based_registry_.find(key);
        if (it != key_based_registry_.end()) {
          type = it->second;
          Py_INCREF(type);
        }
      }
    }
    if (type == nullptr) {
      type = PyQValueType();
      if (type == nullptr) {
        return nullptr;
      }
    }
    PyObject* result = MakePyQValue(type, std::move(typed_value));
    return result;
  }

 private:
  ~QValueSpecializationRegistry() = delete;

  absl::flat_hash_map<QTypePtr, PyTypeObject*> qtype_based_registry_;
  absl::flat_hash_map<std::string, PyTypeObject*> key_based_registry_;
};

}  // namespace

bool RegisterPyQValueSpecializationByQType(QTypePtr qtype,
                                           PyObject* qvalue_subtype) {
  DCheckPyGIL();
  return QValueSpecializationRegistry::instance().RegisterSpecializationByQType(
      qtype, qvalue_subtype);
}

bool RemovePyQValueSpecializationByQType(QTypePtr qtype) {
  DCheckPyGIL();
  return QValueSpecializationRegistry::instance().RemoveSpecializationByQType(
      qtype);
}

bool RegisterPyQValueSpecializationByKey(absl::string_view key,
                                         PyObject* qvalue_subtype) {
  DCheckPyGIL();
  return QValueSpecializationRegistry::instance().RegisterSpecializationByKey(
      key, qvalue_subtype);
}

bool RemovePyQValueSpecializationByKey(absl::string_view key) {
  DCheckPyGIL();
  return QValueSpecializationRegistry::instance().RemoveSpecializationByKey(
      key);
}

PyObject* WrapAsPyQValue(TypedValue&& typed_value) {
  DCheckPyGIL();
  return QValueSpecializationRegistry::instance().WrapAsPyQValue(
      std::move(typed_value));
}

}  // namespace arolla::python
