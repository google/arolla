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
#include <algorithm>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;

// This function brute-forces qtype signatures for the given `op`erator. It
// uses all `possible_qtypes` combinations for exactly the first `arity`
// parameters, and rely on the default values for the rest.
std::vector<std::vector<QTypePtr>> InternalDetectQTypeSignatures(
    const ExprOperatorPtr& op, const std::vector<QTypePtr>& possible_qtypes,
    size_t arity, int64_t combination_range_offset,
    int64_t combination_range_size) {
  std::vector<ExprAttributes> input_attrs;

  // Initializes `init_attrs`; the first `arity` positions will be tried with
  // all `possible_qtypes` combinations. The rest need to be covered with the
  // parameters' default values.
  //
  // This helper returns True, if the initialization was successful.
  const auto init_input_attrs = [&] {
    if (arity > 0 && possible_qtypes.empty()) {
      return false;  // No `possible_qtypes` provided for brute-forcing.
    }
    const auto signature = pybind11_unstatus_or(op->GetSignature());
    const auto& params = signature.parameters;
    if (HasVariadicParameter(signature)) {
      input_attrs.resize(std::max(arity, params.size() - 1));
    } else if (arity <= params.size()) {
      input_attrs.resize(params.size());
    } else {
      return false;  // The operator cannot handle this many parameters.
    }
    // For the positions beyond the specified `arity`, use the default values.
    for (size_t i = arity; i < input_attrs.size(); ++i) {
      if (!params[i].default_value.has_value()) {
        // One of the parameters has no default value, so we cannot initiate
        // the brute-force process.
        return false;
      }
      input_attrs[i] = ExprAttributes{*params[i].default_value};
    }
    return true;
  };

  // Fills the first `arity` positions in `input_attrs` with a combination of
  // `possible_qtypes` corresponding to `combination_index`. Returns false if
  // the `combination_index` is out of range.
  //
  // `combination_index` can be viewed as a large number in a numeral system
  // with BASE = `possible_qtypes.size()` and `arity` digits (some leading
  // digits may be zeros). Each digit encodes an index of a `possible_qtype` for
  // the corresponding position in `input_attrs`.
  const auto update_input_attrs = [&](int64_t combination_index) {
    for (size_t i = 0; i < arity; ++i) {
      input_attrs[i] = ExprAttributes(
          possible_qtypes[combination_index % possible_qtypes.size()]);
      combination_index /= possible_qtypes.size();
    }
    return (combination_index == 0);
  };

  // Infers the type of the operator's output. Returns `nullptr` if the operator
  // doesn't support the given input combination.
  const auto infer_output_qtype = [&]() -> const QType* {
    const absl::StatusOr<ExprAttributes> output_attr =
        op->InferAttributes(input_attrs);
    if (!output_attr.ok()) {
      return nullptr;
    }
    if (!output_attr->IsEmpty()) {
      return output_attr->qtype();
    }
    std::ostringstream message;
    message << "operator returned no output qtype: op="
            << op->GenReprToken().str << ", arg_qtypes=(";
    for (size_t i = 0; i < arity; ++i) {
      if (i > 0) {
        message << ", ";
      }
      message << input_attrs[i].qtype()->name();
    }
    message << ")";
    throw std::runtime_error(std::move(message).str());
  };

  std::vector<std::vector<QTypePtr>> result;

  // Appends the current input types with the given `output_qtype` to the
  // result.
  const auto append_qtype_signature = [&](QTypePtr output_qtype) {
    std::vector<QTypePtr> qtype_signature(arity + 1);
    for (size_t i = 0; i < arity; ++i) {
      qtype_signature[i] = input_attrs[i].qtype();
    }
    qtype_signature[arity] = output_qtype;
    result.push_back(std::move(qtype_signature));
  };

  // The main algorithm.
  //
  if (!init_input_attrs()) {
    return result;
  }
  for (int64_t i = 0; i < combination_range_size &&
                      update_input_attrs(combination_range_offset + i);
       ++i) {
    if (auto* output_qtype = infer_output_qtype()) {
      append_qtype_signature(output_qtype);
    }
  }
  return result;
}

PYBIND11_MODULE(clib, m) {
  m.def(
      "internal_detect_qtype_signatures", &InternalDetectQTypeSignatures,
      py::call_guard<py::gil_scoped_release>(),  //
      py::arg("op"), py::arg("possible_qtypes"), py::arg("arity"),
      py::arg("combination_range_offset"), py::arg("combination_range_size"),
      py::doc(
          "(internal) Brute-forces qtype signatures for the given operator.\n\n"
          "It uses all `possible_qtypes` combinations for the first `arity`\n"
          "parameters and relies on the default values for the rest."));
}

}  // namespace
}  // namespace arolla::python
