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
#include "arolla/qtype/qtype.h"

#include <cstdint>
#include <ostream>
#include <string>
#include <typeinfo>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/demangle.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace arolla {

QType::QType(ConstructorArgs args)
    : name_(std::move(args.name)),
      type_info_(args.type_info),
      type_layout_(std::move(args.type_layout)),
      type_fields_(std::move(args.type_fields)),
      value_qtype_(args.value_qtype),
      qtype_specialization_key_(std::move(args.qtype_specialization_key)) {}

QType::QType(std::string name, const std::type_info& type_info,
             FrameLayout type_layout)
    : QType(ConstructorArgs{
          .name = std::move(name),
          .type_info = type_info,
          .type_layout = std::move(type_layout),
      }) {}

QType::~QType() = default;

void FingerprintHasherTraits<QTypePtr>::operator()(FingerprintHasher* hasher,
                                                   QTypePtr qtype) const {
  hasher->Combine(reinterpret_cast<uintptr_t>(static_cast<const void*>(qtype)));
}

ReprToken QType::UnsafeReprToken(const void* source) const {
  return ReprToken{absl::StrFormat("<value of %s at %p>", name(), source)};
}

absl::string_view QType::UnsafePyQValueSpecializationKey(
    const void* /*source*/) const {
  return "";
}

std::ostream& operator<<(std::ostream& stream, const QType& type) {
  return stream << "QType{" << type.name() << "}";
}

std::ostream& operator<<(std::ostream& stream, QTypePtr type) {
  return stream << "QTypePtr{" << type->name() << "}";
}

ReprToken ReprTraits<QTypePtr>::operator()(const QTypePtr& value) const {
  return ReprToken{std::string(value->name())};
}

namespace {

class QTypeQType final : public QType {
 public:
  QTypeQType()
      : QType(ConstructorArgs{
            .name = "QTYPE",
            .type_info = typeid(QTypePtr),
            .type_layout = MakeTypeLayout<QTypePtr>(),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const final {
    return ReprTraits<QTypePtr>()(*static_cast<const QTypePtr*>(source));
  }

  void UnsafeCopy(const void* source, void* destination) const final {
    *static_cast<QTypePtr*>(destination) =
        *static_cast<const QTypePtr*>(source);
  }

  void UnsafeCombineToFingerprintHasher(const void* source,
                                        FingerprintHasher* hasher) const final {
    hasher->Combine(*static_cast<const QTypePtr*>(source));
  }

  absl::string_view UnsafePyQValueSpecializationKey(
      const void* source) const final {
    return (**static_cast<const QTypePtr*>(source)).qtype_specialization_key();
  }
};

}  // namespace

QTypePtr GetQTypeQType() {
  static const absl::NoDestructor<QTypeQType> result;
  return result.get();
}

namespace {

// NOTE: Nothing is private by design; there must be no values of this type.
struct Nothing {};

class NothingQType final : public QType {
 public:
  NothingQType()
      : QType(ConstructorArgs{.name = "NOTHING",
                              .type_info = typeid(Nothing),
                              .type_layout = {}}) {}

  ReprToken UnsafeReprToken(const void* source) const override {
    return ReprToken{"nothing"};
  }

  void UnsafeCopy(const void* /*source*/,
                  void* /*destination*/) const override {}

  void UnsafeCombineToFingerprintHasher(
      const void* source, FingerprintHasher* hasher) const override {}
};

}  // namespace

QTypePtr GetNothingQType() {
  static const absl::NoDestructor<NothingQType> result;
  return result.get();
}

namespace {

// Formatter to be used with absl/strings library.
struct QTypeFormatter {
  void operator()(std::string* out, const QType* type /*nullable*/) const {
    // NOTE: Old compilers may not support string->append(string_view).
    if (type != nullptr) {
      const absl::string_view name = type->name();
      out->append(name.data(), name.size());
    } else {
      out->append("NULL");
    }
  }
};

}  // namespace

// Join type names separated by ",".
std::string JoinTypeNames(absl::Span<const QType* const /*nullable*/> types) {
  return absl::StrJoin(types, ",", QTypeFormatter());
}

std::string FormatTypeVector(absl::Span<const QTypePtr> types) {
  return absl::StrCat("(", JoinTypeNames(types), ")");
}

absl::Status VerifyQTypeTypeInfo(QTypePtr expected_qtype,
                                 const std::type_info& actual_type_info) {
  if (actual_type_info != expected_qtype->type_info()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("type mismatch: expected C++ type `%s` (%s), got `%s`",
                        TypeName(expected_qtype->type_info()),
                        expected_qtype->name(), TypeName(actual_type_info)));
  }
  return absl::OkStatus();
}

}  // namespace arolla
