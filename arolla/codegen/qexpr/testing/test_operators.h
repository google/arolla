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
#ifndef AROLLA_CODEGEN_QEXPR_TESTING_TEST_OPERATORS_H_
#define AROLLA_CODEGEN_QEXPR_TESTING_TEST_OPERATORS_H_

#include <cmath>
#include <tuple>

#include "absl/base/no_destructor.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace testing {

//
// User defined "Vector3" data types.
//

template <class T>
struct Vector3 {
  Vector3() = default;
  Vector3(T x_, T y_, T z_) : x(x_), y(y_), z(z_) {}
  T x;
  T y;
  T z;
};

struct AddOp {
  template <typename T>
  T operator()(T arg1, T arg2) const {
    return arg1 + arg2;
  }
};

struct AddWithContextOp {
  template <typename T>
  T operator()(EvaluationContext*, T arg1, T arg2) const {
    return arg1 + arg2;
  }
};

struct AddWithStatusOrOp {
  template <typename T>
  absl::StatusOr<T> operator()(T arg1, T arg2) const {
    return arg1 + arg2;
  }
};

struct AddWithArgAsFunction {
  template <typename T>
  T operator()(T arg1, T arg2) const {
    return arg1 + arg2;
  }
  template <typename T, typename Fn>
  T operator()(T arg1, const Fn& arg2) const {
    return arg1 + arg2();
  }
};

struct Add3Op {
  template <typename T>
  T operator()(T arg1, T arg2, T arg3) const {
    return arg1 + arg2 + arg3;
  }
};

struct MulOp {
  template <typename T>
  T operator()(T arg1, T arg2) const {
    return arg1 * arg2;
  }
};

struct NegateOp {
  template <typename T>
  T operator()(T arg1) const {
    return -arg1;
  }
};

struct EqOp {
  template <typename T>
  bool operator()(T arg1, T arg2) const {
    return arg1 == arg2;
  }
};

struct NeqOp {
  template <typename T>
  bool operator()(T arg1, T arg2) const {
    return arg1 != arg2;
  }
};

// OperatorTraits for an operator on Vector3 lifted from scalar SimpleOperator.
template <typename PointwiseOp>
struct Vector3LiftedOperatorTraits {
  template <typename... T>
  auto operator()(const Vector3<T>&... args) const {
    return Vector3(PointwiseOp()(args.x...), PointwiseOp()(args.y...),
                   PointwiseOp()(args.z...));
  }
};

// Define a "Vector3" operator family which converts a set of 3 values
// into a corresponding Vector3.
struct Vector3Op {
  template <class T>
  Vector3<T> operator()(T x, T y, T z) const {
    return Vector3<T>{x, y, z};
  }
};

// Define a "DotProd" operator family which computes the dot product
// of two Vector3's.
struct DotProdOp {
  template <class T>
  T operator()(const Vector3<T>& a, const Vector3<T>& b) const {
    return (a.x * b.x) + (a.y * b.y) + (a.z * b.z);
  }
};

// Define an "VectorComponents" operator family which splits Vector3<T>
// into 3 output slots.
struct VectorComponentsOp {
  template <class T>
  std::tuple<T, T, T> operator()(const Vector3<T>& v) const {
    return std::make_tuple(v.x, v.y, v.z);
  }
};

// Define an "Id" operator which just returns its input unmodified.
struct IdOp {
  template <class... Ts>
  std::tuple<Ts...> operator()(const Ts&... ts) const {
    return std::make_tuple(ts...);
  }
};

// Define an "Pi" operator which just returns Pi constant.
struct PiOp {
  double operator()() const { return 4 * atan(1.); }
};

}  // namespace testing

template <typename T>
struct FingerprintHasherTraits<testing::Vector3<T>> {
  void operator()(FingerprintHasher* hasher,
                  const testing::Vector3<T>& value) const {
    hasher->Combine(value.x, value.y, value.z);
  }
};

template <typename T>
struct QTypeTraits<testing::Vector3<T>> {
  static QTypePtr type() {
    static const absl::NoDestructor<SimpleQType> result(
        meta::type<testing::Vector3<T>>(),
        absl::StrFormat("Vector3<%s>", GetQType<T>()->name()));
    return result.get();
  }
};

}  // namespace arolla

#endif  // AROLLA_CODEGEN_QEXPR_TESTING_TEST_OPERATORS_H_
