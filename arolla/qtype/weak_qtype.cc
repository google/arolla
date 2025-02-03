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
#include "arolla/qtype/weak_qtype.h"

#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"  // IWYU pragma: keep
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {
namespace {

class WeakFloatQType final : public BasicDerivedQType {
 public:
  explicit WeakFloatQType()
      : BasicDerivedQType(ConstructorArgs{
            .name = "WEAK_FLOAT",
            .base_qtype = GetQType<double>(),
        }) {
    CHECK_OK(VerifyDerivedQType(this));
  }

  static QTypePtr get() {
    static const absl::NoDestructor<WeakFloatQType> result;
    return result.get();
  }

  ReprToken UnsafeReprToken(const void* source) const override {
    return GenReprTokenWeakFloat(*static_cast<const double*>(source));
  }
};

class OptionalWeakFloatQType final : public QType,
                                     public DerivedQTypeInterface {
 public:
  OptionalWeakFloatQType() : QType(MakeConstructorArgs()) {
    CHECK_OK(VerifyDerivedQType(this));
  }

  static QTypePtr get() {
    static const absl::NoDestructor<OptionalWeakFloatQType> result;
    return result.get();
  }

  QTypePtr GetBaseQType() const final { return GetOptionalQType<double>(); }

  ReprToken UnsafeReprToken(const void* source) const final {
    const auto& value = *static_cast<const OptionalValue<double>*>(source);
    if (value.present) {
      return ReprToken{
          absl::StrCat("optional_", GenReprTokenWeakFloat(value.value).str)};
    }
    return ReprToken{"optional_weak_float{NA}"};
  }

  void UnsafeCopy(const void* source, void* destination) const final {
    if (source != destination) {
      *static_cast<OptionalValue<double>*>(destination) =
          *static_cast<const OptionalValue<double>*>(source);
    }
  }

  void UnsafeCombineToFingerprintHasher(const void* source,
                                        FingerprintHasher* hasher) const final {
    hasher->Combine(*static_cast<const OptionalValue<double>*>(source));
  }

 private:
  static ConstructorArgs MakeConstructorArgs() {
    // Copy type layout and fields from OptionalValue and replace QType
    // of the value with the corresponding weak QType.
    auto base_qtype = GetOptionalQType<double>();
    std::vector<TypedSlot> fields = base_qtype->type_fields();
    DCHECK_EQ(fields.size(), 2);
    fields[1] = TypedSlot::UnsafeFromOffset(WeakFloatQType::get(),
                                            fields[1].byte_offset());
    return ConstructorArgs{
        .name = "OPTIONAL_WEAK_FLOAT",
        .type_info = base_qtype->type_info(),
        .type_layout = base_qtype->type_layout(),
        .type_fields = std::move(fields),
        .value_qtype = WeakFloatQType::get(),
    };
  }
};

}  // namespace

QTypePtr GetWeakFloatQType() { return WeakFloatQType::get(); }

QTypePtr GetOptionalWeakFloatQType() { return OptionalWeakFloatQType::get(); }

namespace {

static const int optional_weak_float_registered =
    (RegisterOptionalQType(GetOptionalWeakFloatQType()), 1);

}  // namespace
}  // namespace arolla
