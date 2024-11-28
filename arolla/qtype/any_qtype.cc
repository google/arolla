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
#include "arolla/qtype/any_qtype.h"

#include <typeinfo>

#include "absl//status/status.h"
#include "absl//strings/str_format.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/demangle.h"

namespace arolla {

absl::Status Any::InvalidCast(const std::type_info& t) const {
  if (value_.has_value()) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "can not cast Any(%s) to %s", TypeName(value_.type()), TypeName(t)));
  } else {
    return absl::FailedPreconditionError("can not cast an empty ::arolla::Any");
  }
}

AROLLA_DEFINE_SIMPLE_QTYPE(ANY, Any);

}  // namespace arolla
