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
// A python extension module: arolla.types.s11n.clib
//

#include <functional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/types/s11n/py_object_decoder.h"
#include "py/arolla/types/s11n/py_object_encoder.h"
#include "pybind11/functional.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  pybind11_throw_if_error(InitPyObjectCodecDecoder());
  pybind11_throw_if_error(InitPyObjectCodecEncoder());

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "get_py_object_data",
      [](const TypedValue& value) {
        return py::bytes(pybind11_unstatus_or(EncodePyObject(value.AsRef())));
      },
      py::arg("value"), py::pos_only(),
      py::doc("get_py_object_data(value, /)\n"
              "--\n\n"
              "(internal) Returns the serialized data of the object stored in "
              "a PY_OBJECT instance."));

  m.def(
      "py_object_from_data",
      [](absl::string_view data, std::string codec) {
        return pybind11_unstatus_or(DecodePyObject(data, std::move(codec)));
      },
      py::arg("data"), py::arg("codec"), py::pos_only(),
      py::doc("py_object_from_data(data, codec, /)\n"
              "--\n\n"
              "(internal) Returns a PY_OBJECT instance decoded from the "
              "serialized data."));

  m.def(
      "register_py_object_decoding_fn",
      [](std::function<py::object(py::bytes data, py::bytes codec)>
             decoding_fn) {
        if (decoding_fn) {
          RegisterPyObjectDecodingFn(
              [decoding_fn = std::move(decoding_fn)](
                  absl::string_view data,
                  absl::string_view codec) -> absl::StatusOr<PyObject*> {
                pybind11::gil_scoped_acquire guard;
                try {
                  return decoding_fn(py::bytes(data), py::bytes(codec))
                      .release()
                      .ptr();
                } catch (pybind11::error_already_set& ex) {
                  ex.restore();
                } catch (pybind11::builtin_exception& ex) {
                  ex.set_error();
                } catch (...) {
                  return absl::InternalError("unknown error");
                }
                return StatusWithRawPyErr(
                    absl::StatusCode::kFailedPrecondition,
                    absl::StrFormat("PY_OBJECT decoder has failed, codec='%s'",
                                    absl::Utf8SafeCHexEscape(codec)));
              });
        } else {
          RegisterPyObjectDecodingFn(nullptr);
        }
      },
      py::arg("decoding_fn"), py::pos_only(),
      py::doc(
          "register_py_object_decoding_fn(decoding_fn, /)\n"
          "--\n\n"
          "(internal) Registers a function used to decode python objects.\n\n"
          "Note: Use `None` to reset the `decoding_fn` state."));

  m.def(
      "register_py_object_encoding_fn",
      [](std::function<std::string(py::handle object, py::bytes codec)>
             encoding_fn) {
        if (encoding_fn) {
          RegisterPyObjectEncodingFn(
              [encoding_fn = std::move(encoding_fn)](
                  PyObject* py_obj,
                  absl::string_view codec) -> absl::StatusOr<std::string> {
                pybind11::gil_scoped_acquire guard;
                try {
                  return encoding_fn(py::handle(py_obj), py::bytes(codec));
                } catch (pybind11::error_already_set& ex) {
                  ex.restore();
                } catch (pybind11::builtin_exception& ex) {
                  ex.set_error();
                } catch (...) {
                  return absl::InternalError("unknown error");
                }
                return StatusWithRawPyErr(
                    absl::StatusCode::kFailedPrecondition,
                    absl::StrFormat("PY_OBJECT encoder has failed, codec='%s'",
                                    absl::Utf8SafeCHexEscape(codec)));
              });
        } else {
          RegisterPyObjectEncodingFn(nullptr);
        }
      },
      py::arg("encoding_fn"), py::pos_only(),
      py::doc(
          "register_py_object_encoding_fn(encoding_fn, /)\n"
          "--\n\n"
          "(internal) Registers a function used to encode python objects.\n\n"
          "Note: Use `None` to reset the `encoding_fn` state."));
  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
