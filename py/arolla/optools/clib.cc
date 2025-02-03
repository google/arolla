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
#include <vector>

#include "absl/strings/string_view.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11_abseil/absl_casters.h"
#include "arolla/codegen/operator_package/operator_package.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::operator_package::DumpOperatorPackageProto;

PYBIND11_MODULE(clib, m) {
  py::options options;
  options.disable_function_signatures();

  m.def(
      "dumps_operator_package",
      [](const std::vector<absl::string_view> op_names) {
        auto result = pybind11_unstatus_or(DumpOperatorPackageProto(op_names));
        return py::bytes(result.SerializeAsString());
      },
      py::arg("op_names"), py::pos_only(),
      py::doc(
          "dumps_operator_package(op_names, /)\n"
          "--\n\n"
          "Returns an operator package containing the specified operators.\n\n"
          "If an operator from the op_names list is used to declare other\n"
          "operators on the list, it must be mentioned before its first\n"
          "use, meaning operators should be listed in topological order.\n"
          "Operators used in implementations but not listed are considered\n"
          "prerequisites."));
}

}  // namespace
}  // namespace arolla::python
