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
#include "arolla/util/demangle.h"

#include <cstdlib>
#include <string>
#include <typeinfo>

#include "arolla/util/bytes.h"

#if defined(__GXX_RTTI)
#define AROLLA_HAS_CXA_DEMANGLE
#endif

#ifdef AROLLA_HAS_CXA_DEMANGLE
#include <cxxabi.h>  // IWYU pragma: keep
#endif

namespace arolla {

// This is almost a copy of Demangle function from ABSL implementation:
// //third_party/absl/debugging/internal/demangle.cc
std::string TypeName(const std::type_info& ti) {
  // arolla::Bytes is std::string, so we override representation to have
  // arolla specific name.
  if (ti == typeid(arolla::Bytes)) {
    return "arolla::Bytes";
  }
  int status = 0;
  char* demangled = nullptr;
#ifdef AROLLA_HAS_CXA_DEMANGLE
  demangled = abi::__cxa_demangle(ti.name(), nullptr, nullptr, &status);
#endif
  if (status == 0 && demangled != nullptr) {  // Demangling succeeded.
    std::string out = demangled;
    free(demangled);
    return out;
  } else {
    return ti.name();
  }
}

}  // namespace arolla
