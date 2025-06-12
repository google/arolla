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
#ifndef AROLLA_UTIL_DEMANGLE_H_
#define AROLLA_UTIL_DEMANGLE_H_

#include <string>
#include <typeinfo>

namespace arolla {

// Returns a name of a type described by std::type_info.
// It tries to use C++ ABI demangler, if available. Returns a mangled name
// otherwise.
std::string TypeName(const std::type_info& ti);

// Returns a name of the template parameter type.
// It tries to use C++ ABI demangler, if available. Returns a mangled name
// otherwise.
template <class T>
std::string TypeName() {
  return TypeName(typeid(T));
}

}  // namespace arolla

#undef AROLLA_HAS_CXA_DEMANGLE

#endif  // AROLLA_UTIL_DEMANGLE_H_
