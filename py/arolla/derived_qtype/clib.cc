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
// Python extension module with C++ primitives for arolla.derived_qtype.*.
//
#include <functional>
#include <optional>
#include <string>
#include <utility>

#include "absl/strings/str_format.h"
#include "arolla/derived_qtype/labeled_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/repr.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/functional.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11/warnings.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  // NOTE: While `py::function` is non-GIL-safe, `py::cast<PyOpReprFn>` is.
  using PyReprFn =
      std::function<std::optional<ReprToken>(const TypedValue& qvalue)>;

  // NOTE: We disable prepending of function signatures to docstrings because
  // pybind11 generates signatures that are incompatible with
  // `__text_signature__`.
  py::options options;
  options.disable_function_signatures();

  // go/keep-sorted start block=yes newline_separated=yes
  m.def(
      "register_labeled_qtype_repr_fn",
      [](std::string label, PyReprFn repr_fn, bool override) {
        auto cc_repr_fn = [repr_fn = std::move(repr_fn)](
                              TypedRef value) -> std::optional<ReprToken> {
          auto qvalue = TypedValue(value);
          AcquirePyGIL guard;
          try {
            return repr_fn(qvalue);
          } catch (py::error_already_set& ex) {
            py::warnings::warn(
                absl::StrFormat(
                    "failed to evaluate the repr_fn on a value with "
                    "qtype=%s and fingerprint=%s:\n%s",
                    qvalue.GetType()->name(),
                    qvalue.GetFingerprint().AsString(), ex.what())
                    .c_str(),
                PyExc_RuntimeWarning, 1);
            return std::nullopt;
          }
        };
        pybind11_throw_if_error(RegisterLabeledQTypeReprFn(
            std::move(label), std::move(cc_repr_fn), override));
      },
      py::arg("label"), py::arg("repr_fn"), py::pos_only(), py::kw_only(),
      py::arg("override") = false,
      py::doc(
          "register_labeled_qtype_repr_fn(label, repr_fn, /, *, "
          "override=False)\n"
          "--\n\n"
          "Registers a `repr_fn` for labeled qtypes with the given label.\n\n"
          "The `repr_fn` should have the signature\n\n"
          "  repr_fn(qvalue) -> repr_token|None\n\n"
          "and it will be called during `repr(labeled_qvalue)` with:\n"
          " * `qvalue`: a QValue with the provided labeled_qtype.\n"
          "Args:\n"
          "  label: a label to register the repr_fn for.\n"
          "  repr_fn: function producing a repr (or None to fallback to\n"
          "    default repr). Any exception will be caught and treated as if\n"
          "    None was returned.\n"
          "  override: if True, override any existing repr_fn for the label."));
  // go/keep-sorted end
}

}  // namespace
}  // namespace arolla::python
