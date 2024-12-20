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
#ifndef THIRD_PARTY_PY_AROLLA_ABC_EVAL_OPTIONS_H_
#define THIRD_PARTY_PY_AROLLA_ABC_EVAL_OPTIONS_H_

#include <Python.h>

#include "absl//status/statusor.h"

namespace arolla::python {

// Settings to propagate to Expr compilation for dynamic evaluation.
struct ExprCompilationOptions {
  // Verbosity of errors returned by model evaluation.
  bool verbose_runtime_errors = true;

  template <typename H>
  friend H AbslHashValue(H h, const ExprCompilationOptions& options) {
    return H::combine(std::move(h), options.verbose_runtime_errors);
  }
  bool operator==(const ExprCompilationOptions& other) const = default;
};

absl::StatusOr<ExprCompilationOptions> ParseExprCompilationOptions(
    PyObject* /*nullable*/ py_dict_options);

}  // namespace arolla::python

#endif  // THIRD_PARTY_PY_AROLLA_ABC_EVAL_OPTIONS_H_