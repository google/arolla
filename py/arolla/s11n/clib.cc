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
// Python extension module with Arolla serialization primitives.

#include <string>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization/riegeli.h"
#include "arolla/serialization/utils.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
#include "pybind11_protobuf/native_proto_caster.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization::Decode;
using ::arolla::serialization::DecodeExprSet;
using ::arolla::serialization::DecodeFromRiegeliData;
using ::arolla::serialization::DecodeResult;
using ::arolla::serialization::DecodingOptions;
using ::arolla::serialization::Encode;
using ::arolla::serialization::EncodeAsRiegeliData;
using ::arolla::serialization::EncodeExprSet;
using ::arolla::serialization_base::ContainerProto;
using ::arolla::serialization_base::ValueDecoder;
using ::arolla::serialization_codecs::CodecBasedValueDecoderProvider;
using ::arolla::serialization_codecs::GetRegisteredValueDecoderCodecNames;

using AllowedCodecNames = absl::flat_hash_set<std::string>;

DecodingOptions MakeDecodingOptions(const AllowedCodecNames& allowed_codec_names
                                        ABSL_ATTRIBUTE_LIFETIME_BOUND) {
  DecodingOptions result;
  result.value_decoder_provider =
      [value_decoder_provider = CodecBasedValueDecoderProvider(),
       &allowed_codec_names](
          absl::string_view codec_name) -> absl::StatusOr<ValueDecoder> {
    if (allowed_codec_names.contains(codec_name)) {
      return value_decoder_provider(codec_name);
    }
    return absl::FailedPreconditionError(absl::StrFormat(
        "codec '%s' is not allowed", absl::Utf8SafeCHexEscape(codec_name)));
  };
  return result;
}

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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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

  m.def("experimental_list_registered_decoders",
        GetRegisteredValueDecoderCodecNames,
        py::call_guard<py::gil_scoped_release>(),
        py::doc("experimental_list_registered_decoders()\n"
                "--\n\n"
                "Returns the names of all registered value decoders.\n\n"
                "NOTE: This function is not part of the \"stable\" API and is"
                " subject to change\nor removal without notice."));

  m.def(
      "experimental_riegeli_loads_many",
      [](py::bytes data, AllowedCodecNames allowed_decoders) {
        PyCancellationScope cancellation_scope_guard;
        const auto data_view = py::cast<absl::string_view>(data);
        absl::StatusOr<DecodeResult> result;
        {
          py::gil_scoped_release guard;
          result = DecodeFromRiegeliData(data_view,
                                         MakeDecodingOptions(allowed_decoders));
        }
        pybind11_throw_if_error(result.status());
        return std::pair(std::move(result->values), std::move(result->exprs));
      },
      py::arg("data"), py::pos_only(), py::kw_only(),
      py::arg("allowed_decoders"),
      py::doc(
          "experimental_riegeli_loads_many(data, /, *,"
          " allowed_codec_names=None)\n"
          "--\n\n"
          "(experimental) Decodes values and expressions from Riegeli"
          " container data.\n\n"
          "This is an experimental variant of riegeli_loads_many() that"
          " allows\nrestricting the set of codecs used for decoding. This helps"
          " mitigate\nsecurity risks associated with certain codecs, such as"
          " PICKLE, which are\nvulnerable to arbitrary Python code"
          " execution.\n\n"
          "NOTE: This function is not part of the \"stable\" API and is"
          " subject to change\nor removal without notice.\n\n"
          "Args:\n"
          "  data: A bytes object containing serialized data in Riegeli"
          " format.\n"
          "  allowed_decoders: A set of codec names permitted for"
          " decoding.\n\n"
          "Returns:\n"
          "  A pair of lists: the first element is a list of values, the"
          " second is a list\n  of expressions."));

  m.def(
      "load_proto_expr_set",
      [](const ContainerProto& container_proto) {
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
        PyCancellationScope cancellation_scope_guard;
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
