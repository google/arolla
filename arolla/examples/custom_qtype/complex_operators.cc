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
#include "arolla/examples/custom_qtype/complex.h"
#include "arolla/optools/optools.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace my_namespace {
namespace {

using ::arolla::optools::RegisterFunctionAsOperator;

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterFunctionAsOperator(
              arolla::GetQType<MyComplex>, "my_complex.get_qtype"));
          RETURN_IF_ERROR(RegisterFunctionAsOperator(
              [](double re, double im) {
                return MyComplex{.re = re, .im = im};
              },
              "my_complex.make"));
          RETURN_IF_ERROR(RegisterFunctionAsOperator(
              [](const MyComplex& c) { return c.re; }, "my_complex.get_re"));
          RETURN_IF_ERROR(RegisterFunctionAsOperator(
              [](const MyComplex& c) { return c.im; }, "my_complex.get_im"));

          return absl::OkStatus();
        })

}  // namespace
}  // namespace my_namespace
