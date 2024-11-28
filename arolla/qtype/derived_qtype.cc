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
#include "arolla/qtype/derived_qtype.h"

#include <cstddef>
#include <utility>

#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//strings/str_cat.h"
#include "absl//strings/str_format.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

BasicDerivedQType::BasicDerivedQType(ConstructorArgs args)
    : QType(QType::ConstructorArgs{
          .name = std::move(args.name),
          .type_info = args.base_qtype->type_info(),
          .type_layout = args.base_qtype->type_layout(),
          .type_fields = args.base_qtype->type_fields(),
          .value_qtype = args.value_qtype,
          .qtype_specialization_key = std::move(args.qtype_specialization_key),
      }),
      base_qtype_(args.base_qtype) {
  CHECK_OK(VerifyDerivedQType(this));
}

ReprToken BasicDerivedQType::UnsafeReprToken(const void* source) const {
  return ReprToken{
      absl::StrCat(name(), "{", base_qtype_->UnsafeReprToken(source).str, "}")};
}

void BasicDerivedQType::UnsafeCopy(const void* source,
                                   void* destination) const {
  base_qtype_->UnsafeCopy(source, destination);
}

void BasicDerivedQType::UnsafeCombineToFingerprintHasher(
    const void* source, FingerprintHasher* hasher) const {
  base_qtype_->UnsafeCombineToFingerprintHasher(source, hasher);
}

const QType* /*nullable*/ DecayDerivedQType(const QType* /*nullable*/ qtype) {
  if (auto* derived_qtype_interface =
          dynamic_cast<const DerivedQTypeInterface*>(qtype)) {
    return derived_qtype_interface->GetBaseQType();
  }
  return qtype;
}

absl::Status VerifyDerivedQType(QTypePtr qtype) {
  const auto* derived_qtype_interface =
      dynamic_cast<const DerivedQTypeInterface*>(qtype);
  if (derived_qtype_interface == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s is not a derived qtype", qtype->name()));
  }
  const auto* base_qtype = derived_qtype_interface->GetBaseQType();
  if (base_qtype == nullptr) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid derived_qtype=%s: base_qtype=nullptr", qtype->name()));
  }
  if (dynamic_cast<const DerivedQTypeInterface*>(base_qtype) != nullptr) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "base_qtype=%s cannot be a derived qtype", base_qtype->name()));
  }
  const bool type_info_ok = (qtype->type_info() == base_qtype->type_info());
  if (!type_info_ok) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid derived_qtype=%s: base_qtype=%s: incompatible type_info",
        qtype->name(), base_qtype->name()));
  }
  // Note: We check only byte size and alignment. Unfortunately there is not way
  // to do an exhaustive check.
  const bool type_layout_ok =
      (qtype->type_layout().AllocSize() ==
           base_qtype->type_layout().AllocSize() &&
       qtype->type_layout().AllocAlignment().value ==
           base_qtype->type_layout().AllocAlignment().value);
  if (!type_layout_ok) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid derived_qtype=%s: base_qtype=%s: incompatible type_layout",
        qtype->name(), base_qtype->name()));
  }
  bool type_fields_ok =
      (qtype->type_fields().empty() ||
       qtype->type_fields().size() == base_qtype->type_fields().size());
  for (size_t i = 0; type_fields_ok && i < qtype->type_fields().size() &&
                     i < base_qtype->type_fields().size();
       ++i) {
    const auto& derived_field = qtype->type_fields()[i];
    const auto& base_field = base_qtype->type_fields()[i];
    type_fields_ok = type_fields_ok &&
                     (derived_field.byte_offset() == base_field.byte_offset() &&
                      DecayDerivedQType(derived_field.GetType()) ==
                          DecayDerivedQType(base_field.GetType()));
  }
  if (!type_layout_ok) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid derived_qtype=%s: base_qtype=%s: incompatible type_fields",
        qtype->name(), base_qtype->name()));
  }
  // Note: The value_type has open semantics by design, nevertheless we believe
  // that the following constraint is reasonable. If you think that it's too
  // restrictive, please contact the arolla team, we are interested to learn
  // about your use case.
  const bool value_qtype_ok =
      (qtype->value_qtype() == nullptr ||
       base_qtype->value_qtype() == nullptr ||
       DecayDerivedQType(qtype->value_qtype()) ==
           DecayDerivedQType(base_qtype->value_qtype()));
  if (!value_qtype_ok) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "invalid derived_qtype=%s: base_qtype=%s: incompatible value_qtype",
        qtype->name(), base_qtype->name()));
  }
  return absl::OkStatus();
}

TypedRef DecayDerivedQValue(TypedRef qvalue) {
  return TypedRef::UnsafeFromRawPointer(DecayDerivedQType(qvalue.GetType()),
                                        qvalue.GetRawPointer());
}

TypedValue DecayDerivedQValue(const TypedValue& qvalue) {
  // TODO: Consider storing the qtype separately from the data, in
  // that case we can avoid copying the data here.
  return TypedValue(DecayDerivedQValue(qvalue.AsRef()));
}

TypedRef UnsafeDowncastDerivedQValue(QTypePtr derived_qtype, TypedRef qvalue) {
  DCHECK_NE(derived_qtype, nullptr);
  auto* base_qtype = DecayDerivedQType(derived_qtype);
  DCHECK_EQ(qvalue.GetType(), base_qtype);
  return TypedRef::UnsafeFromRawPointer(derived_qtype, qvalue.GetRawPointer());
}

TypedValue UnsafeDowncastDerivedQValue(QTypePtr derived_qtype,
                                       const TypedValue& qvalue) {
  // TODO: Consider storing the qtype separately from the data, in
  // that case we can avoid copying the data here.
  return TypedValue(UnsafeDowncastDerivedQValue(derived_qtype, qvalue.AsRef()));
}

}  // namespace arolla
