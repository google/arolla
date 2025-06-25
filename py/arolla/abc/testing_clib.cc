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
// Python extension module exposing some endpoints for testing purposes.

#include <Python.h>

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::NameAnnotation;
using ::arolla::expr::RegisteredOperator;
using ::arolla::expr::VerboseRuntimeError;

PYBIND11_MODULE(testing_clib, m) {
  m.def("python_c_api_operator_signature_from_operator_signature",
        [](py::handle py_signature) {
          ExprOperatorSignature result;
          if (!UnwrapPySignature(py_signature.ptr(), &result)) {
            throw py::error_already_set();
          }
          return pybind11_steal_or_throw<py::object>(WrapAsPySignature(result));
        });

  m.def("python_c_api_signature_from_signature", [](py::handle py_signature) {
    Signature result;
    if (!UnwrapPySignature(py_signature.ptr(), &result)) {
      throw py::error_already_set();
    }
    return pybind11_steal_or_throw<py::object>(WrapAsPySignature(result));
  });

  m.def("pybind11_type_caster_load_fingerprint", [](Fingerprint) {});
  m.def("pybind11_type_caster_load_qvalue", [](TypedValue) {});
  m.def("pybind11_type_caster_load_qtype", [](QTypePtr) {});
  m.def("pybind11_type_caster_load_operator", [](ExprOperatorPtr) {});
  m.def("pybind11_type_caster_load_expr", [](ExprNodePtr) {});
  m.def("pybind11_type_caster_load_operator_signature",
        [](ExprOperatorSignature) {});

  m.def("pybind11_type_caster_cast_fingerprint",
        []() -> Fingerprint { return Fingerprint{}; });
  m.def("pybind11_type_caster_cast_qvalue",
        []() -> TypedValue { return GetUnspecifiedQValue(); });
  m.def("pybind11_type_caster_cast_qtype",
        []() -> QTypePtr { return GetNothingQType(); });
  m.def("pybind11_type_caster_cast_operator", []() -> ExprOperatorPtr {
    return std::make_shared<RegisteredOperator>("reg_op");
  });
  m.def("pybind11_type_caster_cast_expr",
        []() -> ExprNodePtr { return Leaf("key"); });
  m.def("pybind11_type_caster_cast_operator_signature",
        []() -> ExprOperatorSignature { return ExprOperatorSignature{}; });
  m.def("pybind11_type_caster_cast_load_operator_signature",
        [](ExprOperatorSignature x) { return x; });

  m.def("with_name_annotation",
        [](const ExprNodePtr& expr, absl::string_view name) {
          return pybind11_unstatus_or(
              CallOp(NameAnnotation::Make(), {expr, Literal(Text(name))}));
        });

  m.def("raise_verbose_runtime_error", []() {
    absl::Status cause = absl::InvalidArgumentError("error cause");
    absl::Status error = arolla::WithPayloadAndCause(
        absl::FailedPreconditionError("expr evaluation failed"),
        VerboseRuntimeError{.operator_name = "test.fail"}, std::move(cause));
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
  m.def("raise_invalid_verbose_runtime_error", []() {
    absl::Status error = arolla::WithPayload(
        absl::FailedPreconditionError("expr evaluation\nfailed"),
        VerboseRuntimeError{.operator_name = "test.fail"});
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
  m.def("raise_error_with_note", []() {
    absl::Status error = arolla::WithNote(
        absl::FailedPreconditionError("original error"), "Added note");
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
  m.def("raise_error_with_two_notes", []() {
    absl::Status error = arolla::WithNote(
        arolla::WithNote(absl::FailedPreconditionError("original error"),
                         "Added note"),
        "Another added note");
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
  m.def("raise_invalid_error_with_note", []() {
    absl::Status error =
        arolla::WithPayload(absl::FailedPreconditionError("original\nerror"),
                            NotePayload{.note = "Added note"});
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
  m.def("raise_error_with_source_location", []() {
    absl::Status error = arolla::WithSourceLocation(
        absl::FailedPreconditionError("original error"),
        SourceLocationPayload{.function_name = "foo",
                              .file_name = "bar.py",
                              .line = 123,
                              .column = 456,
                              .line_text = "x = y + 1"});
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
  m.def("raise_invalid_error_with_source_location", []() {
    absl::Status error =
        arolla::WithPayload(absl::FailedPreconditionError("original error"),
                            SourceLocationPayload{.function_name = "foo",
                                                  .file_name = "bar.py",
                                                  .line = 123,
                                                  .column = 456,
                                                  .line_text = "x = y + 1"});
    SetPyErrFromStatus(error);
    throw pybind11::error_already_set();
  });
}

}  // namespace
}  // namespace arolla::python
