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
#ifndef THIRD_PARTY_PY_AROLLA_EXAMPLES_MY_COMPLEX_MY_COMPLEX_TYPE_H_
#define THIRD_PARTY_PY_AROLLA_EXAMPLES_MY_COMPLEX_MY_COMPLEX_TYPE_H_

#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace my_complex {

// Custom struct to represent a complex number. We will be defining a QType for
// it in this example.
struct MyComplex {
  double re;
  double im;

  // AROLLA_DECLARE_REPR below one can declare ArollaFingerprint and
  // ArollaRepr functions here.

  // Note: instead of AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS below one can
  // define the following method:
  //   void ArollaFingerprint(arolla::FingerprintHasher* hasher);

  // Note: instead of AROLLA_DECLARE_REPR below one can define the following
  // method:
  //   arolla::ReprToken ArollaRepr();
};

}  // namespace my_complex

namespace arolla {

// Declares FingerprintHasherTraits<my_complex::MyComplex>.
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(my_complex::MyComplex);

// Declares ReprTraits<my_complex::MyComplex>.
AROLLA_DECLARE_REPR(my_complex::MyComplex);

// Declares arolla::QTypeTraits<my_complex::MyComplex> to make
// arolla::GetQType<my_complex::MyComplex>() work.
AROLLA_DECLARE_QTYPE(my_complex::MyComplex);

}  // namespace arolla

#endif  // THIRD_PARTY_PY_AROLLA_EXAMPLES_MY_COMPLEX_MY_COMPLEX_TYPE_H_
