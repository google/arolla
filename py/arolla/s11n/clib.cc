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

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
#include "pybind11_protobuf/native_proto_caster.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization/riegeli.h"
#include "arolla/serialization/utils.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization::Decode;
using ::arolla::serialization::DecodeExprSet;
using ::arolla::serialization::DecodeFromRiegeliData;
using ::arolla::serialization::DecodeResult;
using ::arolla::serialization::Encode;
using ::arolla::serialization::EncodeAsRiegeliData;
using ::arolla::serialization::EncodeExprSet;
using ::arolla::serialization_base::ContainerProto;

absl::StatusOr<std::string> SerializeProtoAsString(
    absl::StatusOr<ContainerProto>&& proto) {
  if (!proto.ok()) {
    return std::move(proto).status();
  }
  std::string result;
  if (!proto->SerializeToString(&result)) {
    return absl::InternalError("failed to serialize ContainerProto");
  }
  return result;
}

PYBIND11_MODULE(clib, m) {
  py::options options;
  options.disable_function_signatures();

  pybind11_protobuf::ImportNativeProtoCasters();

  // Note: We typically release the GIL during serialization, as it's
  // a time-consuming operation that doesn't rely on the Python interpreter.
  // This allows other Python threads to execute useful tasks in parallel.

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "dump_proto_expr_set",
      [](const absl::flat_hash_map<std::string, ExprNodePtr>& expr_set) {
        absl::StatusOr<ContainerProto> result;
        {
          py::gil_scoped_release guard;
          result = EncodeExprSet(expr_set);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("container_proto"), py::pos_only(),
      py::doc("dump_proto_expr_set(expr_set, /)\n"
              "--\n\n"
              "Encodes a set of named expressions into a proto container.\n\n"
              "Note: The order of the dict keys does not guarantee to be "
              "preserved."));

  m.def(
      "dump_proto_many",
      [](const std::vector<TypedValue>& values,
         const std::vector<ExprNodePtr>& exprs) {
        absl::StatusOr<ContainerProto> result;
        {
          py::gil_scoped_release guard;
          result = Encode(values, exprs);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("values"), py::arg("exprs"),
      py::doc(
          "dump_proto_many(values, exprs)\n"
          "--\n\n"
          "Encodes the given values and expressions into a proto container."));

  m.def(
      "dumps_expr_set",
      [](const absl::flat_hash_map<std::string, ExprNodePtr>& expr_set) {
        absl::StatusOr<std::string> result;
        {
          py::gil_scoped_release guard;
          result = SerializeProtoAsString(EncodeExprSet(expr_set));
        }
        return py::bytes(pybind11_unstatus_or(std::move(result)));
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
        absl::StatusOr<std::string> result;
        {
          py::gil_scoped_release guard;
          result = SerializeProtoAsString(Encode(values, exprs));
        }
        return py::bytes(pybind11_unstatus_or(std::move(result)));
      },
      py::arg("values"), py::arg("exprs"),
      py::doc("dumps_many(values, exprs)\n"
              "--\n\n"
              "Encodes the given values and expressions into a bytes object."));

  m.def(
      "load_proto_expr_set",
      [](const ContainerProto& container_proto) {
        decltype(DecodeExprSet(container_proto)) result;
        {
          py::gil_scoped_release guard;
          result = DecodeExprSet(container_proto);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("container_proto"), py::pos_only(),
      py::doc("load_proto_expr_set(container_proto, /)\n"
              "--\n\n"
              "Decodes a set of named expressions from the given proto "
              "container.\n\n"
              "Note: The order of the keys in the resulting dict is "
              "non-deterministic."));

  m.def(
      "load_proto_many",
      [](const ContainerProto& container_proto) {
        absl::StatusOr<DecodeResult> result;
        {
          py::gil_scoped_release guard;
          result = Decode(container_proto);
        }
        pybind11_throw_if_error(result.status());
        return std::pair(std::move(result->values), std::move(result->exprs));
      },
      py::arg("container_proto"), py::pos_only(),
      py::doc(
          "load_proto_many(container_proto, /)\n"
          "--\n\n"
          "Decodes values and expressions from the given proto container."));

  m.def(
      "loads_expr_set",
      [](py::bytes data) {
        const auto data_view = py::cast<absl::string_view>(data);
        decltype(DecodeExprSet(ContainerProto())) result;
        {
          py::gil_scoped_release guard;
          ContainerProto container_proto;
          if (!container_proto.ParseFromString(data_view)) {
            throw py::value_error("could not parse ContainerProto");
          }
          result = DecodeExprSet(container_proto);
        }
        return pybind11_unstatus_or(std::move(result));
      },
      py::arg("data"), py::pos_only(),
      py::doc("loads_expr_set(data, /)\n"
              "--\n\n"
              "Decodes a set of named expressions from the given data.\n\n"
              "Note: The order of the keys in the resulting dict is "
              "non-deterministic."));

  m.def(
      "loads_many",
      [](py::bytes data) {
        const auto data_view = py::cast<absl::string_view>(data);
        absl::StatusOr<DecodeResult> result;
        {
          py::gil_scoped_release guard;
          ContainerProto container_proto;
          if (!container_proto.ParseFromString(data_view)) {
            throw py::value_error("could not parse ContainerProto");
          }
          result = Decode(container_proto);
        }
        pybind11_throw_if_error(result.status());
        return std::pair(std::move(result->values), std::move(result->exprs));
      },
      py::arg("data"), py::pos_only(),
      py::doc("loads_many(data, /)\n"
              "--\n\n"
              "Decodes values and expressions from the given data."));

  m.def(
      "riegeli_dumps_many",
      [](const std::vector<TypedValue>& values,
         const std::vector<ExprNodePtr>& exprs, py::str riegeli_options) {
        const auto riegeli_options_view =
            py::cast<absl::string_view>(riegeli_options);
        absl::StatusOr<std::string> result;
        {
          py::gil_scoped_release guard;
          result = EncodeAsRiegeliData(values, exprs, riegeli_options_view);
        }
        pybind11_throw_if_error(result.status());
        return py::bytes(*std::move(result));
      },
      py::arg("values"), py::arg("exprs"), py::kw_only(),
      py::arg("riegeli_options") = "",
      py::doc(
          "riegeli_dumps_many(values, exprs, *, riegeli_options='')\n"
          "--\n\n"
          "Encodes multiple values and expressions into riegeli container "
          "data.\n\n"
          "Args:\n"
          "  values: A list of values for serialization.\n"
          "  expr: A list of expressions for serialization.\n"
          "  riegeli_options: A string with riegeli/records writer options. "
          "See\n"
          "    "
          "https://github.com/google/riegeli/blob/master/doc/"
          "record_writer_options.md\n"
          "    for details. If not provided, default options will be used.\n\n"
          "Returns:\n"
          "  A bytes object containing the serialized data in riegeli "
          "format."));

  m.def(
      "riegeli_loads_many",
      [](py::bytes data) {
        const auto data_view = py::cast<absl::string_view>(data);
        absl::StatusOr<DecodeResult> result;
        {
          py::gil_scoped_release guard;
          result = DecodeFromRiegeliData(data_view);
        }
        pybind11_throw_if_error(result.status());
        return std::pair(std::move(result->values), std::move(result->exprs));
      },
      py::arg("data"), py::pos_only(),
      py::doc("riegeli_loads_many(data, /)\n"
              "--\n\n"
              "Decodes values and expressions from riegeli container data."));
  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
