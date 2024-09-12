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
#include <cstdint>

#include "absl/base/no_destructor.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "arolla/qtype/base_types.h"  // IWYU pragma: keep
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

struct GF127QType final : public BasicDerivedQType {
  GF127QType()
      : BasicDerivedQType(ConstructorArgs{
            .name = "GF127",
            .base_qtype = GetQType<int32_t>(),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const final {
    auto result = GetBaseQType()->UnsafeReprToken(source);
    result.str += "gf";
    return result;
  }

  static QTypePtr get() {
    static const absl::NoDestructor<GF127QType> result;
    return result.get();
  }
};

PYBIND11_MODULE(gf127, m) {
  m.add_object("GF127", py::cast(GF127QType::get()));
}

}  // namespace
}  // namespace arolla::python
