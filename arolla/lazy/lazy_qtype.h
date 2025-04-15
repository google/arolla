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
#ifndef AROLLA_LAZY_LAZY_QTYPE_H_
#define AROLLA_LAZY_LAZY_QTYPE_H_

#include "absl/base/nullability.h"
#include "arolla/lazy/lazy.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"

namespace arolla {

// Returns true for "lazy" qtypes.
bool IsLazyQType(const QType* /*absl_nullable*/ qtype);

// Returns a "lazy" qtype with the given value_qtype.
QTypePtr GetLazyQType(QTypePtr value_qtype);

template <typename T>
QTypePtr GetLazyQType() {
  return GetLazyQType(GetQType<T>());
}

// Returns a lazy qvalue.
TypedValue MakeLazyQValue(LazyPtr lazy);

}  // namespace arolla

#endif  // AROLLA_SEQUENCE_SEQUENCE_QTYPE_H_
