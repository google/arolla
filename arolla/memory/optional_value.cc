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
#include "arolla/memory/optional_value.h"

#include <cstdint>

#include "absl/strings/str_cat.h"
#include "arolla/util/bytes.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"

namespace arolla {

ReprToken ReprTraits<OptionalValue<bool>>::operator()(
    const OptionalValue<bool>& value) const {
  return ReprToken{
      value.present ? absl::StrCat("optional_boolean{", Repr(value.value), "}")
                    : "optional_boolean{NA}"};
}

ReprToken ReprTraits<OptionalValue<int32_t>>::operator()(
    const OptionalValue<int32_t>& value) const {
  return ReprToken{value.present
                       ? absl::StrCat("optional_int32{", Repr(value.value), "}")
                       : "optional_int32{NA}"};
}

ReprToken ReprTraits<OptionalValue<int64_t>>::operator()(
    const OptionalValue<int64_t>& value) const {
  return ReprToken{value.present ? absl::StrCat("optional_", Repr(value.value))
                                 : "optional_int64{NA}"};
}

ReprToken ReprTraits<OptionalValue<uint64_t>>::operator()(
    const OptionalValue<uint64_t>& value) const {
  return ReprToken{value.present ? absl::StrCat("optional_", Repr(value.value))
                                 : "optional_uint64{NA}"};
}

ReprToken ReprTraits<OptionalValue<float>>::operator()(
    const OptionalValue<float>& value) const {
  return ReprToken{
      value.present ? absl::StrCat("optional_float32{", Repr(value.value), "}")
                    : "optional_float32{NA}"};
}

ReprToken ReprTraits<OptionalValue<double>>::operator()(
    const OptionalValue<double>& value) const {
  return ReprToken{value.present ? absl::StrCat("optional_", Repr(value.value))
                                 : "optional_float64{NA}"};
}

ReprToken ReprTraits<OptionalValue<Bytes>>::operator()(
    const OptionalValue<Bytes>& value) const {
  return ReprToken{value.present
                       ? absl::StrCat("optional_bytes{", Repr(value.value), "}")
                       : "optional_bytes{NA}"};
}

ReprToken ReprTraits<OptionalValue<Text>>::operator()(
    const OptionalValue<Text>& value) const {
  return ReprToken{value.present
                       ? absl::StrCat("optional_text{", Repr(value.value), "}")
                       : "optional_text{NA}"};
}

ReprToken ReprTraits<OptionalUnit>::operator()(
    const OptionalUnit& value) const {
  return ReprToken{value.present ? "present" : "missing"};
}

}  // namespace arolla
