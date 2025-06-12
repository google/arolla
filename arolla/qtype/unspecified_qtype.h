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
#ifndef AROLLA_QTYPE_UNSPECIFIED_QTYPE_H_
#define AROLLA_QTYPE_UNSPECIFIED_QTYPE_H_

#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {

// Returns `UNSPECIFIED` qtype.
QTypePtr GetUnspecifiedQType();

// Returns `unspecified` value.
//
// The main purpose of `unspecified` is to serve as a default value
// for a parameter in situations where the actual default value must be
// determined based on other parameters.
const TypedValue& GetUnspecifiedQValue();

}  // namespace arolla

#endif  // AROLLA_QTYPE_UNSPECIFIED_QTYPE_H_
