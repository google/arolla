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
#include "arolla/util/repr.h"

#include <cstdint>
#include <string>

#include "absl/strings/str_cat.h"
#include "arolla/util/fingerprint.h"
#include "double-conversion/double-to-string.h"
#include "double-conversion/utils.h"

namespace arolla {

ReprToken ReprTraits<bool>::operator()(const bool& value) const {
  return ReprToken{value ? "true" : "false"};
}

ReprToken ReprTraits<int32_t>::operator()(const int32_t& value) const {
  ReprToken result{absl::StrCat(value)};
  if (result.str[0] == '-') {
    result.precedence = ReprToken::kSafeForArithmetic;
  } else {
    result.precedence = ReprToken::kSafeForNegation;
  }
  return result;
}

ReprToken ReprTraits<int64_t>::operator()(const int64_t& value) const {
  return ReprToken{absl::StrCat("int64{", value, "}")};
}

ReprToken ReprTraits<uint64_t>::operator()(const uint64_t& value) const {
  return ReprToken{absl::StrCat("uint64{", value, "}")};
}

using double_conversion::DoubleToStringConverter;

ReprToken ReprTraits<float>::operator()(const float& value) const {
  static const DoubleToStringConverter converter(
      DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT, "inf", "nan", 'e',
      -6, 21, 6, 0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  converter.ToShortestSingle(value, &builder);
  ReprToken result{builder.Finalize()};
  if (result.str[0] == '-') {
    result.precedence = ReprToken::kSafeForArithmetic;
  } else {
    result.precedence = ReprToken::kSafeForNegation;
  }
  return result;
}

ReprToken ReprTraits<double>::operator()(const double& value) const {
  static const DoubleToStringConverter converter(
      DoubleToStringConverter::NO_FLAGS, "inf", "nan", 'e', -6, 21, 6, 0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  builder.AddString("float64{");
  converter.ToShortest(value, &builder);
  builder.AddString("}");
  return ReprToken{builder.Finalize()};
}

ReprToken GenReprTokenWeakFloat(double value) {
  static const DoubleToStringConverter converter(
      DoubleToStringConverter::NO_FLAGS, "inf", "nan", 'e', -6, 21, 6, 0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  builder.AddString("weak_float{");
  converter.ToShortest(value, &builder);
  builder.AddString("}");
  return ReprToken{builder.Finalize()};
}

void FingerprintHasherTraits<ReprToken::Precedence>::operator()(
    FingerprintHasher* hasher, const ReprToken::Precedence& value) const {
  hasher->Combine(value.left, value.right);
}

void FingerprintHasherTraits<ReprToken>::operator()(
    FingerprintHasher* hasher, const ReprToken& value) const {
  hasher->Combine(value.str, value.precedence);
}

}  // namespace arolla
