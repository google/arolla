# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for math.beta_distribution_inverse_cdf operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
import scipy.stats

M = arolla.M


def gen_test_data():
  """Yields (cdf, alpha, beta, expected beta_distribution_inverse_cdf)."""
  yield (None, None, None, None)
  yield (None, 1, 2, None)
  yield (0.5, 1, None, None)
  yield (0.5, None, 2, None)

  for cdf in [0, 0.01, 0.1, 0.5, 0.9, 0.99, 1]:
    for alpha in [0.1, 1, 2, 3, 10, 100, 1e6]:
      for beta in [0.1, 1, 2, 3, 10, 100, 1e6]:
        yield (cdf, alpha, beta, scipy.stats.beta.ppf(cdf, alpha, beta))


def gen_qtype_signatures():
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
  for arg_1, arg_2, arg_3 in itertools.product(qtypes, repeat=3):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (
          arg_1,
          arg_2,
          arg_3,
          arolla.types.common_float_qtype(arg_1, arg_2, arg_3),
      )


def is_integral_qtype(qtype):
  return arolla.types.get_scalar_qtype(qtype) in arolla.types.INTEGRAL_QTYPES


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())
# To reduce the number of tests we skip cases where some of the arguments are
# integral, except for the case when all of them are integral.
FEWER_QTYPE_SIGNATURES = tuple(
    (*args, result)
    for *args, result in QTYPE_SIGNATURES
    if sum(map(is_integral_qtype, args)) in [0, 3]
)


class MathBetaDistributionInverseCdfTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.math.beta_distribution_inverse_cdf, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *FEWER_QTYPE_SIGNATURES)
  )
  def test_value(self, cdf, alpha, beta, expected_value):
    actual_value = arolla.eval(
        M.math.beta_distribution_inverse_cdf(cdf, alpha, beta)
    )
    arolla.testing.assert_qvalue_allclose(
        actual_value, expected_value, rtol=1e-5, atol=1e-15
    )

  @parameterized.parameters(
      (float('-inf'),),
      (float('inf'),),
      (float('nan'),),
      (-0.1,),
      (1.1,),
  )
  def test_bad_x(self, cdf):
    with self.assertRaisesRegex(
        ValueError, 'inverse CDF accepts only values between 0 and 1'
    ):
      arolla.eval(M.math.beta_distribution_inverse_cdf(cdf, 1.0, 1.0))

  @parameterized.parameters(
      (-1.0,),
      (0.0,),
      (float('-inf'),),
      (float('inf'),),
      (float('nan'),),
  )
  def test_bad_alpha_or_beta(self, value):
    with self.assertRaisesRegex(
        ValueError,
        'alpha for Beta distribution must be a positive finite number',
    ):
      arolla.eval(M.math.beta_distribution_inverse_cdf(0.5, value, 1.0))
    with self.assertRaisesRegex(
        ValueError,
        'beta for Beta distribution must be a positive finite number',
    ):
      arolla.eval(M.math.beta_distribution_inverse_cdf(0.5, 1.0, value))


if __name__ == '__main__':
  absltest.main()
