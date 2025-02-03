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
#ifndef AROLLA_DENSE_ARRAY_QTYPE_TYPES_H_
#define AROLLA_DENSE_ARRAY_QTYPE_TYPES_H_

// Define QTypeTraits related to DenseArray, allowing it to be used
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
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/copier.h"  // IWYU pragma: export
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
#include "arolla/util/status_macros_backport.h"

namespace arolla {

AROLLA_DECLARE_QTYPE(DenseArrayEdge);
AROLLA_DECLARE_QTYPE(DenseArrayGroupScalarEdge);
AROLLA_DECLARE_QTYPE(DenseArrayShape);

class DenseArrayQTypeBase : public ArrayLikeQType {
 public:
  const ArrayLikeShapeQType* shape_qtype() const final;

  const EdgeQType* edge_qtype() const final;
  const EdgeQType* group_scalar_edge_qtype() const final;

 protected:
  void RegisterValueQType();
  using ArrayLikeQType::ArrayLikeQType;
};

inline bool IsDenseArrayQType(QTypePtr type) {
  return nullptr != dynamic_cast<const DenseArrayQTypeBase*>(type);
}

// Returns QType of DenseArray with elements of type value_qtype. Returns an
// error if no DenseArray of this type is registered.
absl::StatusOr<const DenseArrayQTypeBase*> GetDenseArrayQTypeByValueQType(
    QTypePtr value_qtype);

template <typename T>
class DenseArrayQType : public DenseArrayQTypeBase {
 public:
  std::unique_ptr<BatchToFramesCopier> CreateBatchToFramesCopier() const final {
    return std::make_unique<DenseArray2FramesCopier<T>>();
  }

  std::unique_ptr<BatchFromFramesCopier> CreateBatchFromFramesCopier(
      RawBufferFactory* buffer_factory) const final {
    return std::make_unique<Frames2DenseArrayCopier<T>>(buffer_factory);
  }

  absl::StatusOr<size_t> ArraySize(TypedRef value) const final {
    ASSIGN_OR_RETURN(const DenseArray<T>& v, value.As<DenseArray<T>>());
    return v.size();
  }

 private:
  using DenseArrayQTypeBase::DenseArrayQTypeBase;
  friend struct QTypeTraits<DenseArray<T>>;
};

// Template for declaring QTypeTraits for DenseArray types.
// Must be used within `arolla` namespace.
//
// AROLLA_DECLARE_DENSE_ARRAY_QTYPE --
//   Should be used in a header. Declares QTypeTraits specialization
//   with type() method that returns a pointer to the corresponding
//   QType singleton. Also declare extern template class for DenseArrayQType.
//
// AROLLA_DEFINE_DENSE_ARRAY_QTYPE --
//   Must be used in the corresponding cc file. Defines QTypeTraits::type()
//   method implementation for DenseArray. Also implicitly instantiate template
//    class for DenseArrayQType.
//
// Examples:
//   namespace arolla {  // in *.h
//   AROLLA_DECLARE_DENSE_ARRAY_QTYPE(INT32, int32_t);
//   }
//   namespace arolla {  // in *.cc
//   AROLLA_DEFINE_DENSE_ARRAY_QTYPE(INT32, int32_t);
//   }  // namespace arolla
//
//   auto int32_qtype = GetDenseArrayQType<int32_t>();

#define AROLLA_DECLARE_DENSE_ARRAY_QTYPE(NAME, /*VALUE_TYPE*/...) \
  extern template class DenseArrayQType<__VA_ARGS__>;             \
  AROLLA_DECLARE_QTYPE(DenseArray<__VA_ARGS__>)

#define AROLLA_DEFINE_DENSE_ARRAY_QTYPE(NAME, /*VALUE_TYPE*/...)         \
  template class DenseArrayQType<__VA_ARGS__>;                           \
  QTypePtr QTypeTraits<DenseArray<__VA_ARGS__>>::type() {                \
    static const QTypePtr result([] {                                    \
      auto* result = new DenseArrayQType<__VA_ARGS__>(                   \
          meta::type<DenseArray<__VA_ARGS__>>(), ("DENSE_ARRAY_" #NAME), \
          GetQType<__VA_ARGS__>());                                      \
      result->RegisterValueQType();                                      \
      return result;                                                     \
    }());                                                                \
    return result;                                                       \
  }

// Declare QTypeTraits<DenseArray<T>> for primitive types.
AROLLA_FOREACH_BASE_TYPE(AROLLA_DECLARE_DENSE_ARRAY_QTYPE);
AROLLA_DECLARE_DENSE_ARRAY_QTYPE(UNIT, Unit);
AROLLA_DECLARE_DENSE_ARRAY_QTYPE(ANY, Any);

template <typename T>
QTypePtr GetDenseArrayQType() {
  return GetQType<DenseArray<T>>();
}

// Returns DENSE_ARRAY_WEAK_FLOAT qtype.
QTypePtr GetDenseArrayWeakFloatQType();

// DenseArray Repr with customizable value_repr_fn and qtype_name.
template <typename T, typename ValueReprFn>
ReprToken DenseArrayReprToken(const DenseArray<T>& values,
                              ValueReprFn value_repr_fn,
                              absl::string_view qtype_name) {
  constexpr int64_t max_repr_size = 10;
  const bool omit_values = values.size() > max_repr_size;
  const int64_t repr_size = omit_values ? max_repr_size : values.size();
  std::vector<std::string> repr_values;
  repr_values.reserve(omit_values ? max_repr_size + 1 : repr_size);
  bool all_missing = true;
  for (int64_t i = 0; i < repr_size; ++i) {
    if (values.present(i)) {
      repr_values.push_back(value_repr_fn(T(values.values[i])));
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
  return ReprToken{absl::StrFormat("dense_array([%s]%s%s)",
                                   absl::StrJoin(repr_values, ", "), size_token,
                                   type_token)};
}

// Define DenseArray string representation.
template <class T>
struct ReprTraits<DenseArray<T>,
                  std::enable_if_t<std::is_invocable_v<ReprTraits<T>, T>>> {
  ReprToken operator()(const DenseArray<T>& values) const {
    return DenseArrayReprToken(values, Repr<T>, GetQType<T>()->name());
  }
};
template <>
struct ReprTraits<DenseArray<Unit>> {
  ReprToken operator()(const DenseArray<Unit>& values) const {
    // Use the repr for OptionalUnit to print "present" instead of "unit".
    return DenseArrayReprToken(values, Repr<OptionalUnit>,
                               GetQType<Unit>()->name());
  }
};

AROLLA_DECLARE_REPR(DenseArrayEdge);
AROLLA_DECLARE_REPR(DenseArrayGroupScalarEdge);
AROLLA_DECLARE_REPR(DenseArrayShape);

}  // namespace arolla

#endif  // AROLLA_DENSE_ARRAY_QTYPE_TYPES_H_
