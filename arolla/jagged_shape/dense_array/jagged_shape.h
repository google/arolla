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
#ifndef AROLLA_JAGGED_SHAPE_DENSE_ARRAY_JAGGED_SHAPE_H_
#define AROLLA_JAGGED_SHAPE_DENSE_ARRAY_JAGGED_SHAPE_H_

#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/jagged_shape.h"
#include "arolla/util/repr.h"

namespace arolla {

using JaggedDenseArrayShape = JaggedShape<DenseArrayEdge>;

// Example repr:
//   JaggedShape(2, [2, 1], 2) represents a JaggedDenseArrayShape with split
//   points:
//     - [0, 2]
//     - [0, 2, 3]
//     - [0, 2, 4, 6]
AROLLA_DECLARE_REPR(JaggedDenseArrayShape);

}  // namespace arolla

#endif  // AROLLA_JAGGED_SHAPE_DENSE_ARRAY_JAGGED_SHAPE_H_
