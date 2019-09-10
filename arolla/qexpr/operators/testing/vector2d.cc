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
#include "arolla/qexpr/operators/testing/vector2d.h"

#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

void FingerprintHasherTraits<Vector2D<float>>::operator()(
    FingerprintHasher* hasher, const Vector2D<float>& value) const {
  hasher->Combine(value.x, value.y);
}

void FingerprintHasherTraits<Vector2D<double>>::operator()(
    FingerprintHasher* hasher, const Vector2D<double>& value) const {
  hasher->Combine(value.x, value.y);
}

AROLLA_DEFINE_SIMPLE_QTYPE(VECTOR_2D_FLOAT32, Vector2D<float>);
AROLLA_DEFINE_SIMPLE_QTYPE(VECTOR_2D_FLOAT64, Vector2D<double>);

}  // namespace arolla
