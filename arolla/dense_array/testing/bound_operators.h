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
#ifndef AROLLA_BLOCK_TESTING_BOUND_OPERATORS_H
#define AROLLA_BLOCK_TESTING_BOUND_OPERATORS_H

#include <memory>

#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/operators.h"

namespace arolla {

std::unique_ptr<BoundOperator> DenseArrayAddOperator(
    FrameLayout::Slot<DenseArray<float>> arg1,
    FrameLayout::Slot<DenseArray<float>> arg2,
    FrameLayout::Slot<DenseArray<float>> result);

std::unique_ptr<BoundOperator> DenseArrayUnionAddOperator(
    FrameLayout::Slot<DenseArray<float>> arg1,
    FrameLayout::Slot<DenseArray<float>> arg2,
    FrameLayout::Slot<DenseArray<float>> result);

}  // namespace arolla

#endif  // AROLLA_BLOCK_TESTING_BOUND_OPERATORS_H
