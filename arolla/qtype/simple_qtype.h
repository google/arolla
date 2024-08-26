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
#ifndef AROLLA_QTYPE_SIMPLE_TYPE_H_
#define AROLLA_QTYPE_SIMPLE_TYPE_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/no_destructor.h"  // IWYU pragma: keep
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/struct_field.h"

namespace arolla {

// A helper class for defining simple QTypes.
//
// This helper is suitable for QTypes that:
//
//  * Are backed by a C++ type.
//  * The C++ type has a default constructor.
//  * The C++ type has a copy constructor.
//
// To use this class for a class T one must define:
//
//  * arolla::FingerprintHasherTraits<T> or T::ArollaFingerint method to
//      define QType::UnsafeCombineToFingerprintHasher method (see the
//      documentation in qtype.h).
//
// One may also define:
//
//  * arolla::ReprTraits<T> to redefine the default QType::UnsafeReprToken
//      behavior (see the documentation in qtype.h).
//  * arolla::StructFieldTraits or T::ArollaStructFields to define
//      QType::type_fields and NamedFieldQTypeInterface implementation (see
//      StructFieldTraits documentation).
//
class SimpleQType : public QType, public NamedFieldQTypeInterface {
 public:
  // Constructs a simple container qtype with given type_name and value_qtype.
  template <typename CppType>
  ABSL_ATTRIBUTE_ALWAYS_INLINE SimpleQType(
      meta::type<CppType>, std::string type_name,
      const QType* value_qtype = nullptr,
      std::string qtype_specialization_key = "")
      : QType(ConstructorArgs{
            .name = std::move(type_name),
            .type_info = typeid(CppType),
            .type_layout = MakeTypeLayout<CppType>(),
            .type_fields = GenTypeFields<CppType>(),
            .value_qtype = value_qtype,
            .qtype_specialization_key = std::move(qtype_specialization_key),
        }),
        field_names_(GenFieldNames<CppType>()) {
    CHECK_OK(InitNameMap());
    if constexpr (std::is_invocable_v<ReprTraits<CppType>, CppType>) {
      unsafe_repr_token_fn_ = [](const void* source) {
        return ReprTraits<CppType>()(*static_cast<const CppType*>(source));
      };
    }
    unsafe_copy_fn_ = [](const void* source, void* destination) {
      *static_cast<CppType*>(destination) =
          *static_cast<const CppType*>(source);
    };
    unsafe_combine_to_fingerprint_hasher_fn_ = [](const void* source,
                                                  FingerprintHasher* hasher) {
      hasher->Combine(*static_cast<const CppType*>(source));
    };
  }

 protected:
  ReprToken UnsafeReprToken(const void* source) const override {
    if (unsafe_repr_token_fn_) {
      return unsafe_repr_token_fn_(source);
    } else {
      return QType::UnsafeReprToken(source);
    }
  }

 private:
  absl::Status InitNameMap();

  absl::Span<const std::string> GetFieldNames() const final;

  std::optional<int64_t> GetFieldIndexByName(
      absl::string_view field_name) const final;

  void UnsafeCopy(const void* source, void* destination) const final {
    if (source != destination) {
      return unsafe_copy_fn_(source, destination);
    }
  }

  void UnsafeCombineToFingerprintHasher(const void* source,
                                        FingerprintHasher* hasher) const final {
    return unsafe_combine_to_fingerprint_hasher_fn_(source, hasher);
  }

  template <typename CppType>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static std::vector<std::string> GenFieldNames() {
    std::vector<std::string> result;
    result.reserve(StructFieldCount<CppType>());
    meta::foreach_tuple_element(GetStructFields<CppType>(),
                                [&](const auto& struct_field) {
                                  result.emplace_back(struct_field.field_name);
                                });
    return result;
  }

  template <typename CppType>
  ABSL_ATTRIBUTE_ALWAYS_INLINE static std::vector<TypedSlot> GenTypeFields() {
    std::vector<TypedSlot> result;
    result.reserve(StructFieldCount<CppType>());
    meta::foreach_tuple_element(
        GetStructFields<CppType>(), [&](const auto& struct_field) {
          result.push_back(TypedSlot::FromSlot(
              FrameLayout::Slot<
                  typename std::decay_t<decltype(struct_field)>::field_type>::
                  UnsafeSlotFromOffset(struct_field.field_offset)));
        });
    return result;
  }

 private:
  using UnsafeReprTokenFn = ReprToken (*)(const void* source);
  using UnsafeCopyFn = void (*)(const void* source, void* destination);
  using UnsafeCombineToFingerprintHasherFn =
      void (*)(const void* source, FingerprintHasher* hasher);
  using Name2IdMap = absl::flat_hash_map<std::string, size_t>;

  Name2IdMap name2index_;
  std::vector<std::string> field_names_;

  // Since UnsafeReprFn() may be overridden in derived classes,
  // unsafe_repr_token_fn_ can not be considered a source of truth.
  UnsafeReprTokenFn unsafe_repr_token_fn_ = nullptr;
  UnsafeCopyFn unsafe_copy_fn_ = nullptr;
  UnsafeCombineToFingerprintHasherFn unsafe_combine_to_fingerprint_hasher_fn_ =
      nullptr;
};

// Template for declaring QTypeTraits for simple types.
// Must be used within `arolla` namespace.
//
// AROLLA_DECLARE_SIMPLE_QTYPE --
//   Should be used in a header. Declares QTypeTraits<T> (aka GetQType<T>())
//   specialization.
//
// AROLLA_DEFINE_SIMPLE_QTYPE()
//   Must be used in the corresponding cc file. Defines QTypeTraits<T>::type()
//   method implementation based on SimpleQType. See SimpleQType class doc for
//   the additional requirements / options for the class T.
//
// Example:
//   namespace arolla {
//   AROLLA_DECLARE_SIMPLE_QTYPE(INT32, int32_t);
//   AROLLA_DEFINE_SIMPLE_QTYPE(INT32, int32_t);
//   }  // namespace arolla
//
//   auto int32_qtype = GetQType<int32_t>();
//
#define AROLLA_DECLARE_SIMPLE_QTYPE(NAME, /*CPP_TYPE*/...) \
  AROLLA_DECLARE_QTYPE(__VA_ARGS__);

#define AROLLA_DEFINE_SIMPLE_QTYPE(NAME, /*CPP_TYPE*/...) \
  QTypePtr QTypeTraits<__VA_ARGS__>::type() {             \
    static const absl::NoDestructor<SimpleQType> result(  \
        meta::type<__VA_ARGS__>(), #NAME);                \
    return result.get();                                  \
  }

}  // namespace arolla

#endif  // AROLLA_QTYPE_SIMPLE_TYPE_H_
