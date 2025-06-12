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
#include <Python.h>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/init_arolla.h"
#include "py/arolla/abc/py_object_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/s11n/py_object_codec/codec.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

constexpr absl::string_view kCodecName =
    "arolla.python.PyObjectV1Proto.extension";

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::RegisterValueDecoder;
using ::arolla::serialization_codecs::RegisterValueEncoderByQType;

absl::StatusOr<PyObjectPtr> GetPyToolsModule() {
  static PyObject* py_module_name =
      PyUnicode_InternFromString("arolla.s11n.py_object_codec.tools");
  auto py_module = PyObjectPtr::Own(PyImport_GetModule(py_module_name));
  if (py_module == nullptr) {
    PyErr_Clear();
    return absl::InternalError(
        "the module `arolla.s11n.py_object_codec.tools` is not loaded");
  }
  return py_module;
}

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kCodecName));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodePyObjectQType(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(PyObjectV1Proto::extension)
      ->set_py_object_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodePyObjectQValue(TypedRef value,
                                                Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec, GetPyObjectCodec(value));
  if (!codec.has_value()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("missing serialization codec for %s", value.Repr()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* py_obj_proto = value_proto.MutableExtension(PyObjectV1Proto::extension);
  py_obj_proto->mutable_py_object_value()->set_codec(*codec);
  {  // Use Python C API.
    AcquirePyGIL guard;
    static PyObject* py_method_name =
        PyUnicode_InternFromString("encode_py_object");
    ASSIGN_OR_RETURN(auto py_module, GetPyToolsModule());
    auto py_qvalue = PyObjectPtr::Own(WrapAsPyQValue(TypedValue(value)));
    if (py_qvalue == nullptr) {
      return StatusCausedByPyErr(absl::StatusCode::kInternal,
                                 "unabled to construct qvalue");
    }
    PyObject* py_args[2] = {py_module.get(), py_qvalue.get()};
    auto py_bytes = PyObjectPtr::Own(PyObject_VectorcallMethod(
        py_method_name, py_args, 2 | PY_VECTORCALL_ARGUMENTS_OFFSET, nullptr));
    if (py_bytes == nullptr) {
      return StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "arolla.s11n.py_object_codec.tools.encode_py_object() failed; "
              "py_object_codec='%s'",
              absl::Utf8SafeCHexEscape(*codec)));
    }
    if (!PyBytes_CheckExact(py_bytes.get())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected serialized object to be bytes, got %s; "
          "py_object_codec='%s'",
          Py_TYPE(py_bytes.get())->tp_name, absl::Utf8SafeCHexEscape(*codec)));
    }
    py_obj_proto->mutable_py_object_value()->set_data(
        PyBytes_AS_STRING(py_bytes.get()), PyBytes_GET_SIZE(py_bytes.get()));
    return value_proto;
  }
}

absl::StatusOr<ValueProto> EncodePyObject(TypedRef value, Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    const auto& qtype_value = value.UnsafeAs<QTypePtr>();
    if (qtype_value == GetPyObjectQType()) {
      return EncodePyObjectQType(encoder);
    }
  } else if (value.GetType() == GetPyObjectQType()) {
    return EncodePyObjectQValue(value, encoder);
  }
  return absl::UnimplementedError(
      absl::StrFormat("%s does not support serialization of %s: %s", kCodecName,
                      value.GetType()->name(), value.Repr()));
}

absl::StatusOr<TypedValue> DecodePyObjectQValue(
    const PyObjectV1Proto::PyObjectProto& py_object_proto) {
  if (!py_object_proto.has_data()) {
    return absl::InvalidArgumentError(
        "missing py_object.py_object_value.data; value=PY_OBJECT");
  }
  absl::string_view data = py_object_proto.data();
  if (!py_object_proto.has_codec()) {
    return absl::InvalidArgumentError(
        "missing py_object.py_object_value.codec; value=PY_OBJECT");
  }
  absl::string_view codec = py_object_proto.codec();
  {  // Use Python C API.
    AcquirePyGIL guard;
    static PyObject* py_method_name =
        PyUnicode_InternFromString("decode_py_object");
    ASSIGN_OR_RETURN(auto py_module, GetPyToolsModule());
    auto py_data =
        PyObjectPtr::Own(PyBytes_FromStringAndSize(data.data(), data.size()));
    if (py_data == nullptr) {
      return StatusCausedByPyErr(absl::StatusCode::kInternal,
                                 "unabled to construct python bytes object "
                                 "with `data`; value=PY_OBJECT");
    }
    auto py_codec =
        PyObjectPtr::Own(PyBytes_FromStringAndSize(codec.data(), codec.size()));
    if (py_codec == nullptr) {
      return StatusCausedByPyErr(absl::StatusCode::kInternal,
                                 "unabled to construct python bytes object "
                                 "with `codec`; value=PY_OBJECT");
    }
    PyObject* py_args[3] = {py_module.get(), py_data.get(), py_codec.get()};
    auto py_qvalue = PyObjectPtr::Own(PyObject_VectorcallMethod(
        py_method_name, py_args, 3 | PY_VECTORCALL_ARGUMENTS_OFFSET, nullptr));
    if (py_qvalue == nullptr) {
      return StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("arolla.abc.py_object_codec.tools.decode_py_object() "
                          "failed; py_object_codec='%s'; value=PY_OBJECT",
                          absl::Utf8SafeCHexEscape(codec)));
    }
    if (!IsPyQValueInstance(py_qvalue.get())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected deserialized object to be arolla.abc.PyObject, got %s; "
          "py_object_codec='%s'; value=PY_OBJECT",
          Py_TYPE(py_qvalue.get())->tp_name, absl::Utf8SafeCHexEscape(codec)));
    }
    const auto& qvalue = UnsafeUnwrapPyQValue(py_qvalue.get());
    if (qvalue.GetType() != GetPyObjectQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected deserialized object to be PY_OBJECT, got %s; "
          "py_object_codec='%s'; value=PY_OBJECT",
          qvalue.GetType()->name(), absl::Utf8SafeCHexEscape(codec)));
    }
    ASSIGN_OR_RETURN(auto qvalue_codec, GetPyObjectCodec(qvalue.AsRef()));
    if (qvalue_codec != codec) {
      if (qvalue_codec.has_value()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected deserialized object to have py_object_codec='%s', "
            "got '%s'; value=PY_OBJECT",
            absl::Utf8SafeCHexEscape(codec),
            absl::Utf8SafeCHexEscape(*qvalue_codec)));
      } else {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected deserialized object to have py_object_codec='%s', "
            "got no codec; value=PY_OBJECT",
            absl::Utf8SafeCHexEscape(codec)));
      }
    }
    return qvalue;
  }
}

absl::StatusOr<ValueDecoderResult> DecodePyObject(
    const ValueProto& value_proto,
    absl::Span<const TypedValue> /*input_values*/,
    absl::Span<const ExprNodePtr> /*input_exprs*/) {
  if (!value_proto.HasExtension(PyObjectV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& py_object_proto =
      value_proto.GetExtension(PyObjectV1Proto::extension);
  switch (py_object_proto.value_case()) {
    case PyObjectV1Proto::kPyObjectQtype:
      return TypedValue::FromValue(GetPyObjectQType());
    case PyObjectV1Proto::kPyObjectValue:
      return DecodePyObjectQValue(py_object_proto.py_object_value());
    case PyObjectV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(py_object_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueDecoder(kCodecName, DecodePyObject));
          return RegisterValueEncoderByQType(GetPyObjectQType(),
                                             EncodePyObject);
        })

}  // namespace
}  // namespace arolla::python
