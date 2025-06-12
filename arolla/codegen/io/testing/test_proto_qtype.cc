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
#include "arolla/codegen/io/testing/test_proto_qtype.h"

#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

void FingerprintHasherTraits<testing_namespace::InnerRawPtr>::operator()(
    FingerprintHasher* hasher,
    const testing_namespace::InnerRawPtr& value) const {
  hasher->Combine(value);
}

AROLLA_DEFINE_SIMPLE_QTYPE(INNER_PROTO_RAW_PTR, testing_namespace::InnerRawPtr);
AROLLA_DEFINE_OPTIONAL_QTYPE(INNER_PROTO_RAW_PTR,
                             testing_namespace::InnerRawPtr);
AROLLA_DEFINE_DENSE_ARRAY_QTYPE(INNER_PROTO_RAW_PTR,
                                testing_namespace::InnerRawPtr);

AROLLA_DEFINE_SIMPLE_QTYPE(ROOT_PROTO_RAW_PTR_HOLDER,
                           testing_namespace::RootRawPtrHolder);

}  // namespace arolla
