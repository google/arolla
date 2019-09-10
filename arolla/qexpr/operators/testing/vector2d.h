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
#ifndef AROLLA_QEXPR_TESTING_VECTOR2D_H_
#define AROLLA_QEXPR_TESTING_VECTOR2D_H_

#include <string>

#include "absl/strings/str_cat.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

template <typename T>
struct Vector2D {
  Vector2D() = default;
  Vector2D(T x, T y) : x(x), y(y) {}

  std::string ToString() const { return absl::StrCat("(", x, ", ", y, ")"); }

  T x;
  T y;
};

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Vector2D<float>);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(Vector2D<double>);

AROLLA_DECLARE_SIMPLE_QTYPE(VECTOR_2D_FLOAT32, Vector2D<float>);
AROLLA_DECLARE_SIMPLE_QTYPE(VECTOR_2D_FLOAT64, Vector2D<double>);

struct MakeVector2DOp {
  template <typename T>
  Vector2D<T> operator()(const T& x, const T& y) const {
    return Vector2D<T>(x, y);
  }
};

struct GetXOp {
  template <typename T>
  T operator()(const Vector2D<T>& vec) const {
    return vec.x;
  }
};

struct GetYOp {
  template <typename T>
  T operator()(const Vector2D<T>& vec) const {
    return vec.y;
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_TESTING_VECTOR2D_H_
