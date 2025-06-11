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
#include "arolla/util/init_arolla.h"
namespace arolla::expr {
namespace {

AROLLA_INITIALIZER(
        .name = "arolla_operators/derived_qtype:bootstrap",
        .reverse_deps =
            {
                arolla::initializer_dep::kOperators,
                arolla::initializer_dep::kQExprOperators,
            },
        .init_fn = []() -> absl::Status {
          return absl::OkStatus();
        })

}  // namespace
}  // namespace arolla::expr
