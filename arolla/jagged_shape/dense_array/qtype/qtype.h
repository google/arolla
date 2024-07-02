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
#ifndef AROLLA_JAGGED_SHAPE_DENSE_ARRAY_QTYPE_QTYPE_H_
#define AROLLA_JAGGED_SHAPE_DENSE_ARRAY_QTYPE_QTYPE_H_

// IWYU pragma: always_keep, the file defines QTypeTraits<T> specialization.

#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/qtype/qtype_traits.h"

namespace arolla {

AROLLA_DECLARE_QTYPE(JaggedDenseArrayShape);

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_DENSE_ARRAY_QTYPE_QTYPE_H_
