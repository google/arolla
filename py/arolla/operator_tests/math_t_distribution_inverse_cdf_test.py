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

"""Tests for math.t_distribution_inverse_cdf operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from scipy import stats

M = arolla.M

INF = float('inf')


def gen_test_data():
  """Yields (x, degrees_of_freedom, expected t_distribution_inverse_cdf)."""
  yield (None, None, None)
  yield (None, 1, None)
  yield (0.5, None, None)

  for x in [0, 0.01, 0.1, 0.5, 0.9, 0.99, 1]:
    for degrees_of_freedom in [0.1, 1, 2, 3, 10, 100, 1e6]:
      yield (x, degrees_of_freedom, stats.t.ppf(x, degrees_of_freedom))


def gen_qtype_signatures():
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
  for arg_1, arg_2 in itertools.product(qtypes, repeat=2):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (arg_1, arg_2, arolla.types.common_float_qtype(arg_1, arg_2))


# Cases with very low degrees of freedom may behave unstably, either
# returning excessively large values versus infinities or excessively
# small values versus zeros. Therefore, we test them separately,
# assuming that values greater than or equal to 1e100 are technically
# considered as infinities.
TEST_DATA_WITH_LOW_DEGREES_OF_FREEDOM = (
    (0, 1e-6, -INF),
    (0.49, 1e-6, -INF),
    (0.5, 1e-6, 0),
    (0.51, 1e-6, INF),
    (1, 1e-6, INF),
    # A case with two ints, just to suppress gen_cases complaint about uncovered
    # signatures.
    (1, 1, INF),
)


def roundup_xlarge_to_inf(x):
  x = (arolla.weak_float(INF) & (x >= arolla.weak_float(1e100))) | x
  x = (-arolla.weak_float(INF) & (x <= -arolla.weak_float(1e100))) | x
  return x


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class MathTDistributionInverseCdfTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.math.t_distribution_inverse_cdf, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_value(self, x, degrees_of_freedom, expected_value):
    actual_value = arolla.eval(
        M.math.t_distribution_inverse_cdf(x, degrees_of_freedom)
    )
    arolla.testing.assert_qvalue_allclose(
        actual_value, expected_value, rtol=1e-5, atol=1e-15
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          TEST_DATA_WITH_LOW_DEGREES_OF_FREEDOM, *QTYPE_SIGNATURES
      )
  )
  def test_low_degree_value(self, x, degrees_of_freedom, expected_value):
    actual_value = arolla.eval(
        M.math.t_distribution_inverse_cdf(x, degrees_of_freedom)
    )
    arolla.testing.assert_qvalue_allclose(
        roundup_xlarge_to_inf(actual_value), expected_value
    )

  @parameterized.parameters(
      (float('-inf'), 3.0),
      (float('inf'), 3.0),
      (float('nan'), 3.0),
      (-0.1, 3.0),
      (1.1, 3.0),
  )
  def test_bad_x(self, x, degrees_of_freedom):
    with self.assertRaisesRegex(
        ValueError, 'inverse CDF accepts only values between 0 and 1'
    ):
      arolla.eval(M.math.t_distribution_inverse_cdf(x, degrees_of_freedom))

  @parameterized.parameters(
      (0.5, -1.0),
      (0.5, 0.0),
      (0.5, float('-inf')),
      (0.5, float('inf')),
      (0.5, float('nan')),
  )
  def test_bad_degrees_of_freedom(self, x, degrees_of_freedom):
    with self.assertRaisesRegex(
        ValueError,
        'degrees_of_freedom for t-distribution must be a positive number',
    ):
      arolla.eval(M.math.t_distribution_inverse_cdf(x, degrees_of_freedom))


if __name__ == '__main__':
  absltest.main()
