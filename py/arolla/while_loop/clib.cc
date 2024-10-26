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
#include <utility>

#include "absl/strings/str_format.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/pybind11.h"
#include "pybind11_abseil/absl_casters.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/operators/while_loop/while_loop.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/demangle.h"

namespace arolla::python {
namespace {

namespace py = ::pybind11;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr_operators::MakeWhileLoop;
using ::arolla::expr_operators::NamedExpressions;
using ::arolla::expr_operators::WhileLoopOperator;

const WhileLoopOperator* CastToWhileLoopOperator(const ExprOperatorPtr& op) {
  auto while_loop = dynamic_cast<const WhileLoopOperator*>(op.get());
  if (while_loop == nullptr) {
    throw py::type_error(absl::StrFormat(
        "type mismatch: expected '%s', got '%s'.",
        TypeName<WhileLoopOperator>(), TypeName(typeid(*op.get()))));
  }
  return while_loop;
}

PYBIND11_MODULE(clib, m) {
  m.def(
      "get_while_loop_body",
      [](const ExprOperatorPtr& op) -> TypedValue {
        return TypedValue::FromValue(CastToWhileLoopOperator(op)->body());
      },
      py::arg("op"),
      py::doc("(internal) Returns body operator of while loop operator."));
  m.def(
      "get_while_loop_condition",
      [](const ExprOperatorPtr& op) -> TypedValue {
        return TypedValue::FromValue(CastToWhileLoopOperator(op)->condition());
      },
      py::arg("op"),
      py::doc("(internal) Returns condition operator of while loop operator."));
  m.def(
      "make_while_loop",
      [](NamedExpressions initial_state, ExprNodePtr condition,
         NamedExpressions body) -> ExprNodePtr {
        return pybind11_unstatus_or(MakeWhileLoop(
            std::move(initial_state), std::move(condition), std::move(body)));
      },
      py::arg("initial_state"), py::arg("condition"), py::arg("body"),
      py::doc("(internal) See expr_operators::MakeWhileLoop doc."));
}

}  // namespace
}  // namespace arolla::python
