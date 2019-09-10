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
#include "arolla/examples/custom_qtype/complex.h"

#include "absl/strings/str_format.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

void FingerprintHasherTraits<my_namespace::MyComplex>::operator()(
    FingerprintHasher* hasher, const my_namespace::MyComplex& value) const {
  // No need to include type-specific salt to the fingerprint: it will be done
  // automatically by arolla::TypedValue.
  hasher->Combine(value.im, value.re);
}

ReprToken ReprTraits<my_namespace::MyComplex>::operator()(
    const my_namespace::MyComplex& value) const {
  return ReprToken{absl::StrFormat("%v + %vi", value.re, value.im)};
}

AROLLA_DEFINE_SIMPLE_QTYPE(MY_COMPLEX, my_namespace::MyComplex);

}  // namespace arolla
