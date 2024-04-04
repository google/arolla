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
#ifndef AROLLA_ARRAY_QTYPE_TYPES_H_
#define AROLLA_ARRAY_QTYPE_TYPES_H_

// Define QTypeTraits related to Array, allowing it to be used
// as an argument to and as a result of QExpressions.

// IWYU pragma: always_keep, the file defines QTypeTraits<T> specializations.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/array/array.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/copier.h"  // IWYU pragma: export
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/any_qtype.h"
#include "arolla/qtype/array_like/array_like_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

AROLLA_DECLARE_QTYPE(ArrayEdge);
AROLLA_DECLARE_QTYPE(ArrayGroupScalarEdge);
AROLLA_DECLARE_QTYPE(ArrayShape);

class ArrayQTypeBase : public ArrayLikeQType {
 public:
  const ArrayLikeShapeQType* shape_qtype() const final;

  const EdgeQType* edge_qtype() const final;
  const EdgeQType* group_scalar_edge_qtype() const final;

  // Gets Array from "source", slices it (see Array::Slice), and saves to
  // "destination". Both source and destination must be allocated and
  // initialized with type corresponding to the QType.
  virtual void UnsafeSlice(int64_t start_id, int64_t row_count,
                           const void* source, void* destination) const = 0;

 protected:
  void RegisterValueQType();
  using ArrayLikeQType::ArrayLikeQType;
};

inline bool IsArrayQType(QTypePtr type) {
  return nullptr != dynamic_cast<const ArrayQTypeBase*>(type);
}

// Returns QType of Array with elements of type value_qtype. Returns an
// error if no Array of this type is registered.
absl::StatusOr<const ArrayQTypeBase*> GetArrayQTypeByValueQType(
    QTypePtr value_qtype);

template <typename T>
class ArrayQType : public ArrayQTypeBase {
 public:
  std::unique_ptr<BatchToFramesCopier> CreateBatchToFramesCopier() const final {
    return std::make_unique<ArrayToFramesCopier<T>>();
  }

  std::unique_ptr<BatchFromFramesCopier> CreateBatchFromFramesCopier(
      RawBufferFactory* buffer_factory) const final {
    return std::make_unique<ArrayFromFramesCopier<T>>(buffer_factory);
  }

  void UnsafeSlice(int64_t start_id, int64_t row_count, const void* source,
                   void* destination) const final {
    const Array<T>& src = *reinterpret_cast<const Array<T>*>(source);
    Array<T>& dst = *reinterpret_cast<Array<T>*>(destination);
    dst = src.Slice(start_id, row_count);
  }

  absl::StatusOr<size_t> ArraySize(TypedRef value) const final {
    ASSIGN_OR_RETURN(const Array<T>& v, value.As<Array<T>>());
    return v.size();
  }

 private:
  using ArrayQTypeBase::ArrayQTypeBase;
  friend struct QTypeTraits<Array<T>>;
};

// Template for declaring QTypeTraits for Array types.
// Must be used within `arolla` namespace.
//
// AROLLA_DECLARE_ARRAY_QTYPE --
//   Should be used in a header. Declares QTypeTraits specialization
//   with type() method that returns a pointer to the corresponding
//   QType singleton. Also declare extern template class for ArrayQType.
//
// AROLLA_DEFINE_ARRAY_QTYPE --
//   Must be used in the corresponding cc file. Defines QTypeTraits::type()
//   method implementation for Array. Also implicitly instantiate template
//    class for ArrayQType.
//
// Examples:
//   namespace arolla {  // in *.h
//   AROLLA_DECLARE_ARRAY_QTYPE(INT32, int32_t);
//   }
//   namespace arolla {  // in *.cc
//   AROLLA_DEFINE_ARRAY_QTYPE(INT32, int32_t);
//   }  // namespace arolla
//
//   auto int32_qtype = GetArrayQType<int32_t>();

#define AROLLA_DECLARE_ARRAY_QTYPE(NAME, /*VALUE_TYPE*/...) \
  extern template class ArrayQType<__VA_ARGS__>;            \
  AROLLA_DECLARE_QTYPE(Array<__VA_ARGS__>)

#define AROLLA_DEFINE_ARRAY_QTYPE(NAME, /*VALUE_TYPE*/...)    \
  template class ArrayQType<__VA_ARGS__>;                     \
  QTypePtr QTypeTraits<Array<__VA_ARGS__>>::type() {          \
    static const QTypePtr result = [] {     \
      auto* result = new ArrayQType<__VA_ARGS__>(             \
          meta::type<Array<__VA_ARGS__>>(), ("ARRAY_" #NAME), \
          GetQType<__VA_ARGS__>());                           \
      result->RegisterValueQType();                           \
      return result;                                          \
    }();                                                      \
    return result;                                            \
  }

// Declare QTypeTraits<Array<T>> for primitive types.
AROLLA_FOREACH_BASE_TYPE(AROLLA_DECLARE_ARRAY_QTYPE);
AROLLA_DECLARE_ARRAY_QTYPE(UNIT, Unit);
AROLLA_DECLARE_ARRAY_QTYPE(ANY, Any);

// Array Repr with customizable value_repr_fn and qtype_name.
template <typename T, typename ValueReprFn>
ReprToken ArrayReprToken(const Array<T>& values, ValueReprFn value_repr_fn,
                         absl::string_view qtype_name) {
  constexpr int64_t max_repr_size = 10;
  const bool omit_values = values.size() > max_repr_size;
  const int64_t repr_size = omit_values ? max_repr_size : values.size();
  std::vector<std::string> repr_values;
  repr_values.reserve(omit_values ? max_repr_size + 1 : repr_size);
  bool all_missing = true;
  for (int64_t i = 0; i < repr_size; ++i) {
    OptionalValue<view_type_t<T>> v = values[i];
    if (v.present) {
      repr_values.push_back(value_repr_fn(T(v.value)));
      all_missing = false;
    } else {
      repr_values.push_back("NA");
    }
  }
  if (omit_values) {
    repr_values.push_back("...");
  }
  std::string type_token =
      all_missing ? absl::StrCat(", value_qtype=", qtype_name) : "";
  std::string size_token =
      omit_values ? absl::StrFormat(", size=%d", values.size()) : "";
  return ReprToken{absl::StrFormat("array([%s]%s%s)",
                                   absl::StrJoin(repr_values, ", "), size_token,
                                   type_token)};
}

// Define Array string representation.
template <class T>
struct ReprTraits<Array<T>,
                  std::enable_if_t<std::is_invocable_v<ReprTraits<T>, T>>> {
  ReprToken operator()(const Array<T>& values) const {
    return ArrayReprToken(values, Repr<T>, GetQType<T>()->name());
  }
};
template <>
struct ReprTraits<Array<Unit>> {
  ReprToken operator()(const Array<Unit>& values) const {
    // Use the repr for OptionalUnit to show "present" instead of "unit".
    return ArrayReprToken(values, Repr<OptionalUnit>, GetQType<Unit>()->name());
  }
};

template <typename T>
QTypePtr GetArrayQType() {
  return GetQType<Array<T>>();
}

// Returns ARRAY_WEAK_FLOAT qtype.
QTypePtr GetArrayWeakFloatQType();

AROLLA_DECLARE_REPR(ArrayEdge);
AROLLA_DECLARE_REPR(ArrayGroupScalarEdge);

}  // namespace arolla

#endif  // AROLLA_ARRAY_QTYPE_TYPES_H_
