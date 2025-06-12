// Copyright 2025 Google LLC
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
#include "py/arolla/abc/py_compiled_expr.h"

#include <Python.h>

#include <cstddef>
#include <functional>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/io/wildcard_input_loader.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/string.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_expr_compilation_options.h"
#include "py/arolla/abc/py_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::PostOrder;

using InputNames = std::vector<std::string>;
using InputQTypes = absl::flat_hash_map<std::string, QTypePtr>;
using InputQValues = absl::flat_hash_map<absl::string_view, TypedRef>;
using Model = std::function<absl::StatusOr<TypedValue>(const InputQValues&)>;

// Forward declare.
extern PyTypeObject PyCompiledExpr_Type;

// CompiledExpr structure for Python.
struct PyCompiledExprObject final {
  struct Fields {
    InputNames input_names;
    InputQTypes input_qtypes;
    Model model;
    vectorcallfunc vectorcall;
  };
  PyObject_HEAD;
  Fields fields;
};

// Returns a reference to the `fields` field.
PyCompiledExprObject::Fields& PyCompiledExpr_fields(PyObject* self) {
  DCHECK_EQ(Py_TYPE(self), &PyCompiledExpr_Type);
  return reinterpret_cast<PyCompiledExprObject*>(self)->fields;
}

// (internal) Compiles an expression for the given input_qtypes.
absl::StatusOr<Model> Compile(const ExprNodePtr& expr,
                              const InputQTypes& input_qtypes,
                              const ExprCompilationOptions& options) {
  DCheckPyGIL();
  ReleasePyGIL guard;
  auto accessor = [](const InputQValues& input_qvalues,
                     const std::string& input_name,
                     WildcardInputLoaderCallback callback) -> absl::Status {
    if (auto it = input_qvalues.find(input_name); it != input_qvalues.end()) {
      return callback(it->second);
    }
    return absl::InvalidArgumentError(
        absl::StrCat("missing input: ", input_name));
  };
  return ExprCompiler<InputQValues, TypedValue>()
      .SetInputLoader(
          WildcardInputLoader<InputQValues>::BuildFromCallbackAccessorFn(
              accessor, input_qtypes))
      .SetAlwaysCloneThreadSafetyPolicy()
      .VerboseRuntimeErrors(options.verbose_runtime_errors)
      .Compile(expr);
}

// (internal) Detect common compilation errors.
std::optional<std::string> DetectCommonCompilationErrors(
    const ExprNodePtr& expr, const InputQTypes& input_qtypes) {
  DCheckPyGIL();
  ReleasePyGIL guard;
  std::set<std::string> missing_leaves;
  std::set<std::string> placeholders;
  PostOrder post_order(expr);
  for (const auto& node : post_order.nodes()) {
    if (node->is_leaf() && !input_qtypes.contains(node->leaf_key())) {
      missing_leaves.emplace(ToDebugString(node));
    } else if (node->is_placeholder()) {
      placeholders.emplace(ToDebugString(node));
    }
  }
  if (!placeholders.empty()) {
    return absl::StrCat("expression contains placeholders: ",
                        absl::StrJoin(placeholders, ", "));
  }
  if (!missing_leaves.empty()) {
    return absl::StrCat("missing input_qtypes for: ",
                        absl::StrJoin(missing_leaves, ", "));
  }
  return std::nullopt;
}

// (internal) Executes a compiled expression with the given inputs.
absl::StatusOr<TypedValue> Execute(const Model& model,
                                   const InputQValues& input_qvalues) {
  DCheckPyGIL();
  ReleasePyGIL release_py_gil_guard;
  return model(input_qvalues);
}

// CompiledExpr.execute(self, input_qvalues: dict[str, QValue]) method.
PyObject* PyCompiledExpr_execute(PyObject* self,
                                 PyObject* py_dict_input_qvalues) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto& self_fields = PyCompiledExpr_fields(self);
  // Parse `input_qvalues`.
  if (!PyDict_Check(py_dict_input_qvalues)) {
    return PyErr_Format(
        PyExc_TypeError,
        "%s.execute() expected a dict[str, QValue], got input_qvalues: %s",
        Py_TYPE(self)->tp_name, Py_TYPE(py_dict_input_qvalues)->tp_name);
  }
  const size_t input_qvalues_size = PyDict_Size(py_dict_input_qvalues);
  InputQValues input_qvalues;
  input_qvalues.reserve(input_qvalues_size);
  PyObject *py_str, *py_qvalue;
  Py_ssize_t pos = 0;
  while (PyDict_Next(py_dict_input_qvalues, &pos, &py_str, &py_qvalue)) {
    Py_ssize_t input_name_size = 0;
    if (!PyUnicode_Check(py_str)) {
      return PyErr_Format(
          PyExc_TypeError,
          "%s.execute() expected all input_qvalues.keys() to be "
          "strings, got %s",
          Py_TYPE(self)->tp_name, Py_TYPE(py_str)->tp_name);
    }
    const char* input_name_data =
        PyUnicode_AsUTF8AndSize(py_str, &input_name_size);
    if (input_name_data == nullptr) {
      return nullptr;
    }
    const absl::string_view input_name(input_name_data, input_name_size);
    auto it = self_fields.input_qtypes.find(input_name);
    if (it == self_fields.input_qtypes.end()) {
      return PyErr_Format(PyExc_TypeError,
                          "%s.execute() got an unexpected input %R",
                          Py_TYPE(self)->tp_name, py_str);
    }
    const auto* typed_value = UnwrapPyQValue(py_qvalue);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "%s.execute() expected all input_qvalues.values() to be "
          "QValues, got %s",
          Py_TYPE(self)->tp_name, Py_TYPE(py_qvalue)->tp_name);
    }
    if (typed_value->GetType() != it->second) {
      return PyErr_Format(PyExc_TypeError,
                          "%s.execute() expected %s, got input_qvalues[%R]: %s",
                          Py_TYPE(self)->tp_name,
                          std::string(it->second->name()).c_str(), py_str,
                          std::string(typed_value->GetType()->name()).c_str());
    }
    input_qvalues.try_emplace(input_name, typed_value->AsRef());
  }
  DCHECK(input_qvalues.size() <= self_fields.input_qtypes.size());
  if (input_qvalues.size() < self_fields.input_qtypes.size()) {
    std::ostringstream message;
    message << Py_TYPE(self)->tp_name << ".execute() missing required input: ";
    bool is_first = true;
    for (size_t i = 0; i < self_fields.input_names.size(); ++i) {
      const auto& input_name = self_fields.input_names[i];
      if (!input_qvalues.contains(input_name)) {
        message << NonFirstComma(is_first) << input_name << ": "
                << self_fields.input_qtypes.at(input_name)->name();
      }
    }
    PyErr_SetString(PyExc_TypeError, std::move(message).str().c_str());
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, Execute(self_fields.model, input_qvalues),
                   SetPyErrFromStatus(_));
  return WrapAsPyQValue(std::move(result));
}

// CompiledExpr.__call__(self, *args: QValue, **kwarg: QValue) method.
//
// The positional arguments `*args` follow the order of inputs in `input_qtypes`
// specified during construction. Some inputs can be passed positionally, while
// the rest are provided via `**kwargs`.
//
// Note: This function has lower overhead compared to `execute` because it
// avoids constructing a dictionary for the inputs by supporting positional
// arguments and implementing the vectorcall protocol.
//
PyObject* PyCompiledExpr_vectorcall(PyObject* self,
                                    PyObject* const* py_qvalue_args,
                                    size_t nargsf, PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  auto& self_fields = PyCompiledExpr_fields(self);
  const auto nargs = PyVectorcall_NARGS(nargsf);
  InputQValues input_qvalues;
  input_qvalues.reserve(self_fields.input_qtypes.size());

  // Parse positional arguments.
  if (nargs > self_fields.input_names.size()) {
    // Note: We add 1 to the count because the `self` parameter is also
    // considered a positional argument in Python.
    return PyErr_Format(PyExc_TypeError,
                        "%s.__call__() takes %zu positional arguments but %zu "
                        "were given",
                        Py_TYPE(self)->tp_name,
                        1 + self_fields.input_names.size(), 1 + nargs);
  }
  for (size_t i = 0; i < nargs; ++i) {
    auto* py_qvalue = py_qvalue_args[i];
    const auto& input_name = self_fields.input_names[i];
    const auto* input_qtype = self_fields.input_qtypes.at(input_name);
    const auto* typed_value = UnwrapPyQValue(py_qvalue);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(PyExc_TypeError,
                          "%s.__call__() expected a qvalue, got %s: %s",
                          Py_TYPE(self)->tp_name, input_name.c_str(),
                          Py_TYPE(py_qvalue)->tp_name);
    }
    if (typed_value->GetType() != input_qtype) {
      PyErr_SetString(
          PyExc_TypeError,
          absl::StrFormat("%s.__call__() expected %s, got %s: %s",
                          Py_TYPE(self)->tp_name, input_qtype->name(),
                          input_name, typed_value->GetType()->name())
              .c_str());
      return nullptr;
    }
    input_qvalues.emplace(input_name, typed_value->AsRef());
  }

  // Parse keyword-arguments.
  absl::Span<PyObject*> py_kwnames;
  PyTuple_AsSpan(py_tuple_kwnames, &py_kwnames);
  for (size_t i = 0; i < py_kwnames.size(); ++i) {
    auto* py_str = py_kwnames[i];
    auto* py_qvalue = py_qvalue_args[nargs + i];
    Py_ssize_t input_name_size = 0;
    const char* input_name_data =
        PyUnicode_AsUTF8AndSize(py_str, &input_name_size);
    if (input_name_data == nullptr) {
      return nullptr;
    }
    const absl::string_view input_name(input_name_data, input_name_size);
    auto it = self_fields.input_qtypes.find(input_name);
    if (it == self_fields.input_qtypes.end()) {
      return PyErr_Format(PyExc_TypeError,
                          "%s.__call__() got an unexpected keyword argument %R",
                          Py_TYPE(self)->tp_name, py_str);
    }
    const auto* typed_value = UnwrapPyQValue(py_qvalue);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError, "%s.__call__() expected a qvalue, got %S: %s",
          Py_TYPE(self)->tp_name, py_str, Py_TYPE(py_qvalue)->tp_name);
    }
    if (typed_value->GetType() != it->second) {
      PyErr_SetString(
          PyExc_TypeError,
          absl::StrFormat("%s.__call__() expected %s, got %s: %s",
                          Py_TYPE(self)->tp_name, it->second->name(),
                          input_name, typed_value->GetType()->name())
              .c_str());
      return nullptr;
    }
    if (!input_qvalues.emplace(input_name, typed_value->AsRef()).second) {
      return PyErr_Format(PyExc_TypeError,
                          "%s.__call__() got multiple values for argument %R",
                          Py_TYPE(self)->tp_name, py_str);
    }
  }

  // Detect missing inputs.
  DCHECK(input_qvalues.size() <= self_fields.input_qtypes.size());
  if (input_qvalues.size() < self_fields.input_qtypes.size()) {
    std::ostringstream message;
    message << Py_TYPE(self)->tp_name
            << ".__call__() missing required arguments: ";
    bool is_first = true;
    for (size_t i = nargs; i < self_fields.input_names.size(); ++i) {
      const auto& input_name = self_fields.input_names[i];
      if (!input_qvalues.contains(input_name)) {
        message << NonFirstComma(is_first) << input_name << ": "
                << self_fields.input_qtypes.at(input_name)->name();
      }
    }
    PyErr_SetString(PyExc_TypeError, std::move(message).str().c_str());
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, Execute(self_fields.model, input_qvalues),
                   SetPyErrFromStatus(_));
  return WrapAsPyQValue(std::move(result));
}

// CompiledExpr.__new__(expr: Expr, input_qtypes: dict[str, QType], *, options)
// method.
PyObject* PyCompiledExpr_new(PyTypeObject* py_type, PyObject* args,
                             PyObject* kwargs) {
  DCheckPyGIL();
  PyCancellationScope cancellation_scope;
  PyObject* py_expr = nullptr;
  PyObject* py_dict_input_qtypes = nullptr;
  PyObject* py_options = nullptr;
  static constexpr const char* keywords[] = {"expr", "input_qtypes", "options",
                                             nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                   "OO|$O:arolla.abc.CompiledExpr.__new__",
                                   const_cast<char**>(keywords), &py_expr,
                                   &py_dict_input_qtypes, &py_options)) {
    return nullptr;
  }

  // Parse `expr`.
  const auto expr = UnwrapPyExpr(py_expr);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError,
                        "%s.__new__() expected an expression, got expr: %s",
                        py_type->tp_name, Py_TYPE(py_expr)->tp_name);
  }

  // Parse `input_qtypes`.
  if (!PyDict_Check(py_dict_input_qtypes)) {
    return PyErr_Format(
        PyExc_TypeError,
        "%s.__new__() expected a dict[str, QType], got input_qtypes: %s",
        py_type->tp_name, Py_TYPE(py_dict_input_qtypes)->tp_name);
  }
  const size_t input_qtypes_size = PyDict_Size(py_dict_input_qtypes);
  std::vector<std::string> input_names;
  InputQTypes input_qtypes;
  input_names.reserve(input_qtypes_size);
  input_qtypes.reserve(input_qtypes_size);
  PyObject *py_str, *py_qtype;
  Py_ssize_t pos = 0;
  while (PyDict_Next(py_dict_input_qtypes, &pos, &py_str, &py_qtype)) {
    Py_ssize_t input_name_size = 0;
    if (!PyUnicode_Check(py_str)) {
      return PyErr_Format(
          PyExc_TypeError,
          "%s.__new__() expected all input_qtypes.keys() to be strings, got %s",
          py_type->tp_name, Py_TYPE(py_str)->tp_name);
    }
    const char* input_name_data =
        PyUnicode_AsUTF8AndSize(py_str, &input_name_size);
    if (input_name_data == nullptr) {
      return nullptr;
    }
    input_names.emplace_back(input_name_data, input_name_size);
    const auto* qtype = UnwrapPyQType(py_qtype);
    if (qtype == nullptr) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "%s.__new__() expected all input_qtypes.values() to be "
          "QTypes, got %s",
          py_type->tp_name, Py_TYPE(py_qtype)->tp_name);
    }
    input_qtypes[input_names.back()] = qtype;
  }

  ExprCompilationOptions options;
  if (py_options != nullptr) {
    if (!ParseExprCompilationOptions(py_options, options)) {
      return nullptr;
    }
  }

  // Compile the expression.
  absl::StatusOr<Model> model = Compile(expr, input_qtypes, options);
  if (!model.ok()) {
    if (auto message = DetectCommonCompilationErrors(expr, input_qtypes)) {
      PyErr_Format(PyExc_ValueError, "%s.__new__() %s", py_type->tp_name,
                   message->c_str());
    } else {
      SetPyErrFromStatus(std::move(model).status());
    }
    return nullptr;
  }

  // Build PyCompiledExprObject
  auto self = PyObjectPtr::Own(py_type->tp_alloc(py_type, 0));
  if (self == nullptr) {
    return nullptr;
  }
  auto& self_fields = PyCompiledExpr_fields(self.get());
  new (&self_fields) PyCompiledExprObject::Fields{
      .input_names = std::move(input_names),
      .input_qtypes = std::move(input_qtypes),
      .model = *std::move(model),
      .vectorcall = &PyCompiledExpr_vectorcall,
  };
  return self.release();
}

void PyCompiledExpr_dealloc(PyObject* self) {
  auto* self_compiled_expr = reinterpret_cast<PyCompiledExprObject*>(self);
  self_compiled_expr->fields.~Fields();
  Py_TYPE(self)->tp_free(self);
}

PyMethodDef kPyCompiledExpr_methods[] = {
    {
        "execute",
        PyCompiledExpr_execute,
        METH_O,
        "Executes the compiled expression with given inputs.",
    },
    {nullptr} /* sentinel */
};

PyTypeObject PyCompiledExpr_Type = {
    .ob_base = {PyObject_HEAD_INIT(nullptr)},
    .tp_name = "arolla.abc.CompiledExpr",
    .tp_basicsize = sizeof(PyCompiledExprObject),
    .tp_dealloc = PyCompiledExpr_dealloc,
    .tp_vectorcall_offset = offsetof(PyCompiledExprObject, fields.vectorcall),
    .tp_call = PyVectorcall_Call,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VECTORCALL |
                Py_TPFLAGS_IMMUTABLETYPE,
    .tp_doc = ("A compiled expression ready for execution.\n\n"
               "IMPORTANT: The primary purpose of this class is to be a "
               "low-level building\nblock. Particularly, it doesn't implement "
               "any caching facility. You should\npossibly prefer using "
               "arolla.abc.compile_expr()."),
    .tp_methods = kPyCompiledExpr_methods,
    .tp_new = PyCompiledExpr_new,
};

}  // namespace

PyTypeObject* PyCompiledExprType() {
  DCheckPyGIL();
  if (PyType_Ready(&PyCompiledExpr_Type) < 0) {
    return nullptr;
  }
  Py_INCREF(&PyCompiledExpr_Type);
  return &PyCompiledExpr_Type;
}

}  // namespace arolla::python
