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
// Python extension module with Arolla codegen primitives.
//

#include "arolla/codegen/expr/codegen_operator.h"
#include "arolla/expr/expr_node.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::codegen::Assignment;
using ::arolla::codegen::Function;
using ::arolla::codegen::GenerateOperatorCode;
using ::arolla::codegen::LValue;
using ::arolla::codegen::LValueKind;
using ::arolla::codegen::OperatorCodegenData;
using ::arolla::codegen::RValue;
using ::arolla::codegen::RValueKind;
using ::arolla::expr::ExprNodePtr;

PYBIND11_MODULE(clib, m) {
  py::options options;
  options.disable_function_signatures();

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "generate_operator_code",
      [](const ExprNodePtr& expr, bool inputs_are_cheap_to_read) {
        return pybind11_unstatus_or(
            GenerateOperatorCode(expr, inputs_are_cheap_to_read));
      },
      py::arg("expr"), py::arg("inputs_are_cheap_to_read"));

  py::class_<Assignment>(m, "Assignment")
      .def_property_readonly("lvalue",
                             static_cast<const LValue& (Assignment::*)() const>(
                                 &Assignment::lvalue))
      .def_property_readonly("rvalue",
                             static_cast<const RValue& (Assignment::*)() const>(
                                 &Assignment::rvalue))
      .def_property_readonly("is_inlinable", &Assignment::is_inlinable);

  py::class_<Function>(m, "Function")
      .def_readonly("assignment_ids", &Function::assignment_ids)
      .def_readonly("output_id", &Function::output_id)
      .def_readonly("is_result_status_or", &Function::is_result_status_or);

  py::class_<LValue>(m, "LValue")
      .def_readonly("type_name", &LValue::type_name)
      .def_readonly("is_entire_expr_status_or",
                    &LValue::is_entire_expr_status_or)
      .def_readonly("is_local_expr_status_or", &LValue::is_local_expr_status_or)
      .def_readonly("kind", &LValue::kind)
      .def("qtype_construction", [](const LValue& self) {
        return pybind11_unstatus_or(self.QTypeConstruction());
      });

  py::class_<OperatorCodegenData>(m, "OperatorCodegenData")
      .def_readonly("deps", &OperatorCodegenData::deps)
      .def_readonly("headers", &OperatorCodegenData::headers)
      .def_readonly("inputs", &OperatorCodegenData::inputs)
      .def_readonly("side_outputs", &OperatorCodegenData::side_outputs)
      .def_property_readonly("input_id_to_name",
                             &OperatorCodegenData::input_id_to_name)
      .def_readonly("assignments", &OperatorCodegenData::assignments)
      .def_readonly("functions", &OperatorCodegenData::functions)
      .def_readonly("lambdas", &OperatorCodegenData::lambdas)
      .def_property_readonly("function_entry_points",
                             &OperatorCodegenData::function_entry_points)
      .def_property_readonly("literal_ids", &OperatorCodegenData::literal_ids)
      .def_readonly("output_id", &OperatorCodegenData::output_id);

  py::class_<RValue>(m, "RValue")
      .def_readonly("kind", &RValue::kind)
      .def_readonly("operator_returns_status_or",
                    &RValue::operator_returns_status_or)
      .def_readonly("code", &RValue::code)
      .def_readonly("argument_ids", &RValue::argument_ids)
      .def_readonly("argument_as_function_offsets",
                    &RValue::argument_as_function_offsets)
      .def_readonly("comment", &RValue::comment);

  py::enum_<LValueKind>(m, "LValueKind")
      .value("LITERAL", LValueKind::kLiteral)
      .value("INPUT", LValueKind::kInput)
      .value("LOCAL", LValueKind::kLocal)
      .export_values();

  py::enum_<RValueKind>(m, "RValueKind")
      .value("INPUT", RValueKind::kInput)
      .value("VERBATIM", RValueKind::kVerbatim)
      .value("FUNCTION_CALL", RValueKind::kFunctionCall)
      .value("FUNCTION_WITH_CONTEXT_CALL", RValueKind::kFunctionWithContextCall)
      .value("NO_OP_FIRST", RValueKind::kFirst)
      .value("OUTPUT", RValueKind::kOutput)
      .export_values();

  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
