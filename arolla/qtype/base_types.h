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
#ifndef AROLLA_QTYPE_BASE_TYPES_H_
#define AROLLA_QTYPE_BASE_TYPES_H_

// IWYU pragma: always_keep, the file defines QTypeTraits<T> specializations.

#include <cstdint>
#include <string>

#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/weak_qtype.h"  // IWYU pragma: export
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {

// List of scalar C++ types.
using ScalarTypes = meta::type_list<Unit, bool, int32_t, int64_t, uint64_t,
                                    float, double, Bytes, Text>;

template <typename T>
constexpr bool is_scalar_type_v = meta::contains_v<ScalarTypes, T>;

// A macro loop for base types.
//
// The `visitor` should accept two macro arguments (NAME, CPP_TYPE).
#define AROLLA_FOREACH_BASE_TYPE(visitor) \
  visitor(INT32, int32_t);                \
  visitor(INT64, int64_t);                \
  visitor(UINT64, uint64_t);              \
  visitor(FLOAT32, float);                \
  visitor(FLOAT64, double);               \
  visitor(BOOLEAN, bool);                 \
  visitor(BYTES, ::arolla::Bytes);        \
  visitor(TEXT, ::arolla::Text)

AROLLA_FOREACH_BASE_TYPE(AROLLA_DECLARE_SIMPLE_QTYPE);
AROLLA_FOREACH_BASE_TYPE(AROLLA_DECLARE_OPTIONAL_QTYPE);

AROLLA_DECLARE_SIMPLE_QTYPE(UNIT, Unit);
AROLLA_DECLARE_OPTIONAL_QTYPE(UNIT, Unit);

// Returns true if the given QType is scalar.
bool IsScalarQType(const QType* /*nullable*/ qtype);

// Returns true if the given QType is integral scalar.
bool IsIntegralScalarQType(const QType* /*nullable*/ qtype);

// Returns true if the given QType is floating point scalar.
bool IsFloatingPointScalarQType(const QType* /*nullable*/ qtype);

// Returns true if the given QType is either integral or floating point scalar.
bool IsNumericScalarQType(const QType* /*nullable*/ qtype);

}  // namespace arolla

#endif  // AROLLA_QTYPE_BASE_TYPES_H_
