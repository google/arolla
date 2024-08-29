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
#ifndef AROLLA_OPERATORS_MATH_MATH_H_
#define AROLLA_OPERATORS_MATH_MATH_H_

#include <cmath>

namespace arolla {

struct LogOp {
  float operator()(float x) const { return std::log(x); }
  double operator()(double x) const {
    return std::log(x);
  }
};

struct Log2Op {
  template <typename T>
  T operator()(T x) const {
    return std::log2(x);
  }
};

struct Log10Op {
  template <typename T>
  T operator()(T x) const {
    return std::log10(x);
  }
};

struct Log1pOp {
  template <typename T>
  T operator()(T x) const {
    return std::log1p(x);
  }
};

struct Symlog1pOp {
  template <typename T>
  T operator()(T x) const {
    return x >= 0 ? std::log1p(x) : -std::log1p(-x);
  }
};

struct ExpOp {
  float operator()(float x) const {
    return std::exp(x);
  }
  double operator()(double x) const {
    return std::exp(x);
  }
};

struct Expm1Op {
  template <typename T>
  T operator()(T x) const {
    return std::expm1(x);
  }
};

// math.pow(a, b) operator returns a ** b.
struct PowOp {
  template <typename T>
  T operator()(T a, T b) const {
    return std::pow(a, b);
  }
  double operator()(double a, double b) const {
    return std::pow(a, b);
  }
};

// math.sigmoid operator
struct SigmoidOp {
  template <typename T>
  T operator()(T value, T half, T slope) const {
    return 1.0f / (1.0f + ExpOp()(slope * (half - value)));
  }
};

// math.logit operator
// logit(p) is defined for 0 <= p <= 1, with infinities at 0 and 1.
// logit(p) = log(p / (1-p)) = log(p) - log(1-p) = log(p) - log1p(-p).
struct LogitOp {
  template <typename T>
  T operator()(T p) const {
    return LogOp()(p) - std::log1p(-p);
  }
};

// math.log_sigmoid operator
// Numerically stable implementation of log(sigmoid(x)). A naive implementation
// would break for large |x| because 1 + e**-x rounds to exactly 1 or to +inf.
// Instead, we use log1p instead of log(1 + ...) to avoid rounding to 1.
// We also structure the calculation so that e is never raised to large positive
// values to avoid overflowing to +inf.
struct LogSigmoidOp {
  template <typename T>
  T operator()(T x) const {
    if (x >= 0) {
      return -std::log1p(std::exp(-x));
    }
    return x - std::log1p(std::exp(x));
  }
};

// math.trig.sin(a) operator.
struct SinOp {
  template <typename T>
  T operator()(T x) const {
    return std::sin(x);
  }
};

// math.trig.cos(a) operator.
struct CosOp {
  template <typename T>
  T operator()(T x) const {
    return std::cos(x);
  }
};

// math.trig.sinh(a) operator.
struct SinhOp {
  template <typename T>
  T operator()(T x) const {
    return std::sinh(x);
  }
};

// math.trig.atan(a) operator.
struct AtanOp {
  template <typename T>
  T operator()(T x) const {
    return std::atan(x);
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_MATH_MATH_OPERATORS_H_
