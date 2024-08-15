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
#ifndef AROLLA_QEXPR_OPERATORS_MATH_EXTRA_CDF_H_
#define AROLLA_QEXPR_OPERATORS_MATH_EXTRA_CDF_H_

#include <cmath>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "boost/math/distributions/beta.hpp"
#include "boost/math/distributions/normal.hpp"
#include "boost/math/distributions/students_t.hpp"

namespace arolla {

using BoostMathIgnoreAllErrorsPolicy =
    decltype(boost::math::policies::make_policy(
        boost::math::policies::domain_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::pole_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::overflow_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::underflow_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::denorm_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::evaluation_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::rounding_error<
            boost::math::policies::ignore_error>(),
        boost::math::policies::indeterminate_result_error<
            boost::math::policies::ignore_error>()));

// math.t_distribution_inverse_cdf operator.
struct TDistributionInverseCdfOp {
  template <typename T>
  absl::StatusOr<T> operator()(T x, T degrees_of_freedom) const {
    if (!(x >= 0.0 && x <= 1.0)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "inverse CDF accepts only values between 0 and 1, got x: %f", x));
    }
    if (degrees_of_freedom <= 0.0 || std::isnan(degrees_of_freedom) ||
        std::isinf(degrees_of_freedom)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("degrees_of_freedom for t-distribution must be a "
                          "positive number, got: %f",
                          degrees_of_freedom));
    }

    return boost::math::quantile(
        boost::math::students_t_distribution<T, BoostMathIgnoreAllErrorsPolicy>(
            degrees_of_freedom),
        x);
  }
};

// math.beta_distribution_inverse_cdf operator.
struct BetaDistributionInverseCdfOp {
  template <typename T>
  absl::StatusOr<T> operator()(T cdf, T alpha, T beta) const {
    if (!(cdf >= 0.0 && cdf <= 1.0)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "inverse CDF accepts only values between 0 and 1, got: %f", cdf));
    }
    if (alpha <= 0 || std::isnan(alpha) || std::isinf(alpha)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("alpha for Beta distribution must be a positive "
                          "finite number, got: %f",
                          alpha));
    }
    if (beta <= 0 || std::isnan(beta) || std::isinf(beta)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("beta for Beta distribution must be a positive "
                          "finite number, got: %f",
                          beta));
    }
    return boost::math::quantile(
        boost::math::beta_distribution<T, BoostMathIgnoreAllErrorsPolicy>(alpha,
                                                                          beta),
        cdf);
  }
};

// math.normal_distribution_inverse_cdf operator.
struct NormalDistributionInverseCdfOp {
  template <typename T>
  absl::StatusOr<T> operator()(T x) const {
    if (!(x >= 0.0 && x <= 1.0)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "inverse CDF accepts only values between 0 and 1, got x: %f", x));
    }
    return boost::math::quantile(
        boost::math::normal_distribution<T, BoostMathIgnoreAllErrorsPolicy>(),
        x);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_MATH_EXTRA_CDF_H_
