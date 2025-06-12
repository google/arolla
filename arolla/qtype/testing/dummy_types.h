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
#ifndef AROLLA_QTYPE_TESTING_DUMMY_TYPES_H_
#define AROLLA_QTYPE_TESTING_DUMMY_TYPES_H_

#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

struct DummyWithPrecedence {
  ReprToken::Precedence precedence;
};

AROLLA_DECLARE_REPR(DummyWithPrecedence);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DummyWithPrecedence);
AROLLA_DECLARE_SIMPLE_QTYPE(DUMMY_WITH_PRECEDENCE, DummyWithPrecedence);

}  // namespace arolla

#endif  // AROLLA_QTYPE_TESTING_DUMMY_TYPES_H_
