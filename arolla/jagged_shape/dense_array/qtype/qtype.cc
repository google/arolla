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
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"

#include "absl/base/no_destructor.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/qtype/qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

// Not movable or copyable.
class JaggedDenseArrayShapeQType final : public JaggedShapeQType {
 public:
  static const JaggedDenseArrayShapeQType* GetInstance() {
    static absl::NoDestructor<JaggedDenseArrayShapeQType> result;
    return result.get();
  }

  JaggedDenseArrayShapeQType()
      : JaggedShapeQType(meta::type<JaggedDenseArrayShape>(),
                         "JAGGED_DENSE_ARRAY_SHAPE") {}

  QTypePtr edge_qtype() const override { return GetQType<DenseArrayEdge>(); };
};

}  // namespace

QTypePtr QTypeTraits<JaggedDenseArrayShape>::type() {
  return JaggedDenseArrayShapeQType::GetInstance();
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kQTypes}, .init_fn = [] {
          return SetEdgeQTypeToJaggedShapeQType(
              GetQType<DenseArrayEdge>(), GetQType<JaggedDenseArrayShape>());
        })

}  // namespace arolla
