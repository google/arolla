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
#include "arolla/qexpr/qexpr_operator_signature.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {
namespace {

struct AltFloatQType final : BasicDerivedQType {
  AltFloatQType()
      : BasicDerivedQType(ConstructorArgs{
            .name = "ALT_FLOAT",
            .base_qtype = GetQType<float>(),
            .value_qtype = GetQType<float>(),
            .qtype_specialization_key = "::arolla::AltFloatQType",
        }) {}

  static QTypePtr get() {
    static const absl::NoDestructor<AltFloatQType> result;
    return result.get();
  }
};

TEST(QExprOperatorSignatureTest, IsDerivedFrom) {
  EXPECT_FALSE(IsDerivedFrom(
      {GetQType<float>(), GetQType<float>()}, GetQType<float>(),
      *QExprOperatorSignature::Get({GetQType<float>()}, GetQType<float>())));
  EXPECT_FALSE(IsDerivedFrom(
      {GetQType<float>()}, GetQType<float>(),
      *QExprOperatorSignature::Get({GetQType<int64_t>()}, GetQType<float>())));
  EXPECT_FALSE(IsDerivedFrom(
      {GetQType<float>()}, GetQType<float>(),
      *QExprOperatorSignature::Get({GetQType<float>()}, GetQType<int64_t>())));

  EXPECT_TRUE(IsDerivedFrom(
      {GetQType<float>()}, GetQType<float>(),
      *QExprOperatorSignature::Get({GetQType<float>()}, GetQType<float>())));
  EXPECT_FALSE(
      IsDerivedFrom({AltFloatQType::get()}, AltFloatQType::get(),
                    *QExprOperatorSignature::Get({AltFloatQType::get()},
                                                 AltFloatQType::get())));
}

}  // namespace
}  // namespace arolla
