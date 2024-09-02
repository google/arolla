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
#include "arolla/qtype/unspecified_qtype.h"

#include "absl/base/no_destructor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {
namespace {

// A private C++ type for UnspecifiedQType::type_info().
struct Unspecified {};

class UnspecifiedQType final : public QType {
 public:
  UnspecifiedQType()
      : QType(ConstructorArgs{.name = "UNSPECIFIED",
                              .type_info = typeid(Unspecified),
                              .type_layout = MakeTypeLayout<Unspecified>()}) {}

  ReprToken UnsafeReprToken(const void* source) const override {
    return ReprToken{"unspecified"};
  }

  void UnsafeCopy(const void* /*source*/,
                  void* /*destination*/) const override {}

  void UnsafeCombineToFingerprintHasher(
      const void* /*source*/, FingerprintHasher* hasher) const override {
    hasher->Combine(absl::string_view("::arolla::UnspecifiedQValue"));
  }
};

}  // namespace

QTypePtr GetUnspecifiedQType() {
  static const absl::NoDestructor<UnspecifiedQType> result;
  return result.get();
}

const TypedValue& GetUnspecifiedQValue() {
  static const absl::NoDestructor<TypedValue> result(
      TypedValue::UnsafeFromTypeDefaultConstructed(GetUnspecifiedQType()));
  return *result;
}

}  // namespace arolla
