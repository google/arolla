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
#include "arolla/qtype/base_types.h"

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <string>

#include "absl/types/span.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace arolla {

AROLLA_FOREACH_BASE_TYPE(AROLLA_DEFINE_SIMPLE_QTYPE);
AROLLA_FOREACH_BASE_TYPE(AROLLA_DEFINE_OPTIONAL_QTYPE);
AROLLA_DEFINE_SIMPLE_QTYPE(UNIT, Unit);
AROLLA_DEFINE_OPTIONAL_QTYPE(UNIT, Unit);

namespace {

bool ContainsQType(absl::Span<const QTypePtr> qtypes,
                   const QType* /*nullable*/ qtype) {
  return std::find(qtypes.begin(), qtypes.end(), qtype) != qtypes.end();
}

auto GetScalarQTypes() {
  static const auto result = {
      GetQType<Unit>(),    GetQType<bool>(),     GetQType<int32_t>(),
      GetQType<int64_t>(), GetQType<uint64_t>(), GetQType<float>(),
      GetQType<double>(),  GetWeakFloatQType(),  GetQType<Bytes>(),
      GetQType<Text>(),
  };
  return result;
}

auto GetIntegralScalarQTypes() {
  static const auto result = {
      GetQType<int32_t>(),
      GetQType<int64_t>(),
  };
  return result;
}

auto GetFloatingPointScalarQTypes() {
  static const auto result = {
      GetQType<float>(),
      GetQType<double>(),
      GetWeakFloatQType(),
  };
  return result;
}

}  // namespace

bool IsScalarQType(const QType* /*nullable*/ qtype) {
  return qtype != nullptr && qtype->value_qtype() == nullptr &&
         ContainsQType(GetScalarQTypes(), qtype);
}

bool IsIntegralScalarQType(const QType* /*nullable*/ qtype) {
  return ContainsQType(GetIntegralScalarQTypes(), qtype);
}

bool IsFloatingPointScalarQType(const QType* /*nullable*/ qtype) {
  return ContainsQType(GetFloatingPointScalarQTypes(), qtype);
}

bool IsNumericScalarQType(const QType* /*nullable*/ qtype) {
  return IsIntegralScalarQType(qtype) || IsFloatingPointScalarQType(qtype);
}

}  // namespace arolla
