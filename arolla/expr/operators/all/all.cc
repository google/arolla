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
#include "absl/status/status.h"
#include "arolla/expr/operators/register_operators.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

AROLLA_INITIALIZER(
        .name = "arolla_operators/standard",
        .deps = {"arolla_operators/standard:bootstrap",
                 "arolla_operators/standard:operator_package"},
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterCoreEmptyLike());
          RETURN_IF_ERROR(RegisterArrayTake());
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::expr_operators
