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
// Python extension module with Arolla serialization primitives.
//

#include <utility>
#include <vector>

#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization::Decode;
using ::arolla::serialization::Encode;
using ::arolla::serialization_base::ContainerProto;

PYBIND11_MODULE(clib, m) {

  py::options options;
  options.disable_function_signatures();

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "dumps_many",
      [](const std::vector<TypedValue>& values,
         const std::vector<ExprNodePtr>& exprs) {
        auto result = pybind11_unstatus_or(Encode(values, exprs));
        return py::bytes(result.SerializeAsString());
      },
      py::arg("values"), py::arg("exprs"),
      py::doc("dumps_many(values, exprs)\n"
              "--\n\n"
              "Encodes the given values and expressions."));

  m.def(
      "loads_many",
      [](const py::bytes& data) {
        ContainerProto container_proto;
        if (!container_proto.ParseFromString(data)) {
          throw py::value_error("could not parse ContainerProto");
        }
        auto result = pybind11_unstatus_or(Decode(std::move(container_proto)));
        return std::pair(std::move(result.values), std::move(result.exprs));
      },
      py::arg("data"), py::pos_only(),
      py::doc("loads_many(data, /)\n"
              "--\n\n"
              "Decodes values and expressions from the given bytes."));

  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
