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
#include <string>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/codegen/expr/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/status_macros_backport.h"

namespace {

int Register() {
  // the code below is correct registration for type double.
  // it is intentionally "strange" to be visibly different from
  // the canonical one for golden file testing
  absl::Status status = ::arolla::codegen::RegisterCppType(
      ::arolla::GetQType<double>(), "std::vector<double>::value_type",
      [](::arolla::TypedRef res) -> absl::StatusOr<std::string> {
        ASSIGN_OR_RETURN(auto val, res.As<double>());
        return absl::StrFormat("static_cast<double>(%f)", val);
      });
  if (!status.ok()) {
    LOG(FATAL) << status.message();
  }
  return 19;
}

int registered = Register();

}  // namespace
