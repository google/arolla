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
#ifndef AROLLA_CODEGEN_IO_TESTING_TEST_PROTO_QTYPE_H_
#define AROLLA_CODEGEN_IO_TESTING_TEST_PROTO_QTYPE_H_

#include "arolla/dense_array/qtype/types.h"
#include "arolla/proto/testing/test.pb.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"

namespace testing_namespace {

using InnerRawPtr = const Inner*;

}

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(testing_namespace::InnerRawPtr);

AROLLA_DECLARE_SIMPLE_QTYPE(INNER_PROTO_RAW_PTR,
                            testing_namespace::InnerRawPtr);
AROLLA_DECLARE_OPTIONAL_QTYPE(INNER_PROTO_RAW_PTR,
                              testing_namespace::InnerRawPtr);
AROLLA_DECLARE_DENSE_ARRAY_QTYPE(INNER_PROTO_RAW_PTR,
                                 testing_namespace::InnerRawPtr);

}  // namespace arolla

#endif  // AROLLA_CODEGEN_IO_TESTING_TEST_PROTO_QTYPE_H_
