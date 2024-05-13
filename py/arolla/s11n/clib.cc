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

#include "absl/container/flat_hash_map.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization/utils.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization::Decode;
using ::arolla::serialization::DecodeExprSet;
using ::arolla::serialization::Encode;
using ::arolla::serialization::EncodeExprSet;
using ::arolla::serialization_base::ContainerProto;

PYBIND11_MODULE(clib, m) {
  py::options options;
  options.disable_function_signatures();

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "dumps_expr_set",
      [](const absl::flat_hash_map<std::string, ExprNodePtr>& expr_set) {
        auto result = pybind11_unstatus_or(EncodeExprSet(expr_set));
        return py::bytes(result.SerializeAsString());
      },
      py::arg("data"), py::pos_only(),
      py::doc(
          "dumps_expr_set(expr_set, /)\n"
          "--\n\n"
          "Encodes the given set of named expressions into a bytes object.\n\n"
          "Note: The order of the dict keys does not guarantee to be "
          "preserved."));

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
              "Encodes the given values and expressions into a bytes object."));

  m.def(
      "loads_expr_set",
      [](const py::bytes& data) {
        ContainerProto container_proto;
        if (!container_proto.ParseFromString(data)) {
          throw py::value_error("could not parse ContainerProto");
        }
        return pybind11_unstatus_or(DecodeExprSet(container_proto));
      },
      py::arg("data"), py::pos_only(),
      py::doc("loads_expr_set(data, /)\n"
              "--\n\n"
              "Decodes a set of named expressions from the given data.\n\n"
              "Note: The order of the keys in the resulting dict is "
              "non-deterministic."));

  m.def(
      "loads_many",
      [](const py::bytes& data) {
        ContainerProto container_proto;
        if (!container_proto.ParseFromString(data)) {
          throw py::value_error("could not parse ContainerProto");
        }
        auto result = pybind11_unstatus_or(Decode(container_proto));
        return std::pair(std::move(result.values), std::move(result.exprs));
      },
      py::arg("data"), py::pos_only(),
      py::doc("loads_many(data, /)\n"
              "--\n\n"
              "Decodes values and expressions from the given data."));
  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
