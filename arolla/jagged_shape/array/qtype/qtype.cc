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
#include "arolla/jagged_shape/array/qtype/qtype.h"

#include "absl/base/no_destructor.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/jagged_shape/array/jagged_shape.h"
#include "arolla/jagged_shape/qtype/qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

// Not movable or copyable.
class JaggedArrayShapeQType final : public JaggedShapeQType {
 public:
  static const JaggedArrayShapeQType* GetInstance() {
    static absl::NoDestructor<JaggedArrayShapeQType> result;
    return result.get();
  }

  JaggedArrayShapeQType()
      : JaggedShapeQType(meta::type<JaggedArrayShape>(), "JAGGED_ARRAY_SHAPE") {
  }

  QTypePtr edge_qtype() const override { return GetQType<ArrayEdge>(); };
};

}  // namespace

QTypePtr QTypeTraits<JaggedArrayShape>::type() {
  return JaggedArrayShapeQType::GetInstance();
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kQTypes},  //
        .init_fn = [] {
          return SetEdgeQTypeToJaggedShapeQType(GetQType<ArrayEdge>(),
                                                GetQType<JaggedArrayShape>());
        })

}  // namespace arolla
