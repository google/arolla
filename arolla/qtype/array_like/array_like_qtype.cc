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
#include "arolla/qtype/array_like/array_like_qtype.h"

#include <cstddef>
#include <memory>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/class_info.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

// QType for scalar an edge that connects two scalars.
class ScalarToScalarEdgeQType final : public EdgeQType {
 public:
  QTypePtr child_shape_qtype() const final {
    return GetQType<OptionalScalarShape>();
  }
  QTypePtr parent_shape_qtype() const final {
    return GetQType<OptionalScalarShape>();
  }

  ScalarToScalarEdgeQType()
      : EdgeQType(meta::type<ScalarToScalarEdge>(), "SCALAR_TO_SCALAR_EDGE") {}
};

}  // namespace

absl::StatusOr<const EdgeQType*> ToEdgeQType(QTypePtr type) {
  if (auto* edge_qtype = FastDowncast<EdgeQType>(type)) {
    return edge_qtype;
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "expected an edge, got %s", type ? type->name() : "nullptr"));
}

void FingerprintHasherTraits<ScalarToScalarEdge>::operator()(
    FingerprintHasher* hasher, const ScalarToScalarEdge& value) const {
  hasher->Combine(absl::string_view("arolla::ScalarToScalarEdge"));
}

QTypePtr QTypeTraits<ScalarToScalarEdge>::type() {
  static const absl::NoDestructor<ScalarToScalarEdgeQType> result;
  return result.get();
}

absl::StatusOr<const ArrayLikeQType*> ToArrayLikeQType(QTypePtr type) {
  if (auto* array_like_qtype = FastDowncast<ArrayLikeQType>(type)) {
    return array_like_qtype;
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "expected an array, got %s", type ? type->name() : "nullptr"));
}

absl::StatusOr<size_t> GetArraySize(TypedRef array) {
  ASSIGN_OR_RETURN(auto type, ToArrayLikeQType(array.GetType()));
  return type->ArraySize(array);
}

absl::StatusOr<std::unique_ptr<BatchToFramesCopier>> CreateBatchToFramesCopier(
    QTypePtr type) {
  ASSIGN_OR_RETURN(const ArrayLikeQType* array_type, ToArrayLikeQType(type));
  return array_type->CreateBatchToFramesCopier();
}

absl::StatusOr<std::unique_ptr<BatchFromFramesCopier>>
CreateBatchFromFramesCopier(QTypePtr type, RawBufferFactory* buffer_factory) {
  ASSIGN_OR_RETURN(const ArrayLikeQType* array_type, ToArrayLikeQType(type));
  return array_type->CreateBatchFromFramesCopier(buffer_factory);
}

}  // namespace arolla
