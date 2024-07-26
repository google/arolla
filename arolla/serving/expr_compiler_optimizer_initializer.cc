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
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serving_impl {

class ExprCompilerDefaultOptimizerInitializer {
 public:
  static absl::Status Init() {
    ASSIGN_OR_RETURN(
        *::arolla::serving_impl::ExprCompilerDefaultOptimizer::optimizer_,
        ::arolla::expr::DefaultOptimizer());
    return absl::OkStatus();
  }
};

// The compiler optimizer is optional for serving; when present, it is
// initialized before AROLLA_DEFINE_EMBEDDED_MODEL_FN.
//
// If the optimizer is dynamically loaded at runtime, it will affect newly
// loaded models but will have no effect on already compiled models.
//
AROLLA_INITIALIZER(.reverse_deps = {"@phony/serving_compiler_optimizer"},
                   .init_fn = &ExprCompilerDefaultOptimizerInitializer::Init)

}  // namespace arolla::serving_impl
