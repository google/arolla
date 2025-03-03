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
#ifndef AROLLA_EXPR_VERBOSE_RUNTIME_ERROR_H_
#define AROLLA_EXPR_VERBOSE_RUNTIME_ERROR_H_

#include <string>

namespace arolla::expr {

// An error payload intended for use with errors occurring during expression
// evaluation when `verbose_runtime_errors` is set. Designed to communicate
// additional metadata about evaluation in a structured way.
//
// When converted to Python, it adds attributes like `operator_name` to
// the "cause" error.
struct VerboseRuntimeError {
  // Name of the "topmost" (before expression lowering) operator that caused the
  // error.
  std::string operator_name;
};

}  // namespace arolla::expr

#endif  // AROLLA_EXPR_VERBOSE_RUNTIME_ERROR_H_
