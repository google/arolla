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
#ifndef AROLLA_CODEGEN_EXPR_OPTIMIZATIONS_H_
#define AROLLA_CODEGEN_EXPR_OPTIMIZATIONS_H_

#include <string>

#include "absl/flags/declare.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/optimization/optimizer.h"

ABSL_DECLARE_FLAG(std::string, arolla_codegen_optimizer_name);

namespace arolla::codegen {

// Registers optimization.
// Extension point for GetOptimizer.
// Must be called at library initialization time and added as alwayslink
// dependency to the tool generating code.
// After registration optimization can be selected by providing
// --arolla_codegen_optimizer_name flag.
absl::Status RegisterOptimization(absl::string_view optimization_name,
                                  expr::Optimizer optimizer);

// Returns optimizer with the given name.
// If name is "", default optimizer is returned.
absl::StatusOr<expr::Optimizer> GetOptimizer(absl::string_view name);

}  // namespace arolla::codegen

#endif  // AROLLA_CODEGEN_EXPR_OPTIMIZATIONS_H_
