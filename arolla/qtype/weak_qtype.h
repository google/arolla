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
#ifndef AROLLA_QTYPE_WEAK_QTYPE_H_
#define AROLLA_QTYPE_WEAK_QTYPE_H_

#include "arolla/qtype/qtype.h"

namespace arolla {

// Weak types are implicitly castable to any precision type of the same type
// class (e.g. floating), but are stored as the maximum precision type.
//
// Weak type usage prevents unwanted type promotion within operations between
// typed values (such as Arrays) and values with no explicitly specified type
// (such as Python scalar literals).
QTypePtr GetWeakFloatQType();
QTypePtr GetOptionalWeakFloatQType();

}  // namespace arolla

#endif  // AROLLA_QTYPE_WEAK_QTYPE_H_
