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
#ifndef AROLLA_EXAMPLES_CUSTOM_QTYPE_COMPLEX_H_
#define AROLLA_EXAMPLES_CUSTOM_QTYPE_COMPLEX_H_

#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace my_namespace {

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

}  // namespace my_namespace

namespace arolla {

// Declares FingerprintHasherTraits<my_namespace::MyComplex>.
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(my_namespace::MyComplex);

// Declares ReprTraits<my_namespace::MyComplex>.
AROLLA_DECLARE_REPR(my_namespace::MyComplex);

// Declares arolla::QTypeTraits<my_namespace::MyComplex> to make
// arolla::GetQType<my_namespace::MyComplex>() work.
//
// The resulting QType will have "MY_COMPLEX" name.
AROLLA_DECLARE_SIMPLE_QTYPE(MY_COMPLEX, my_namespace::MyComplex);

}  // namespace arolla

#endif  // AROLLA_EXAMPLES_CUSTOM_QTYPE_COMPLEX_H_
