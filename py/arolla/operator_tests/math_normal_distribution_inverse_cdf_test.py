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

"""Tests for math.normal_distribution_inverse_cdf operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
import scipy.stats

M = arolla.M


def gen_test_data():
  """Yields (cdf, alpha, beta, expected normal_distribution_inverse_cdf)."""
  yield (None, None)
  for cdf in [0, 0.01, 0.1, 0.5, 0.9, 0.99, 1]:
    yield (cdf, scipy.stats.norm.ppf(cdf))


def gen_qtype_signatures():
  for arg in pointwise_test_utils.lift_qtypes(
      *arolla.types.NUMERIC_QTYPES
  ):
    yield (arg, arolla.types.common_float_qtype(arg))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class MathNormalDistributionInverseCdfTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.math.normal_distribution_inverse_cdf, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_value(self, cdf, expected_value):
    actual_value = arolla.eval(M.math.normal_distribution_inverse_cdf(cdf))
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
      arolla.eval(M.math.normal_distribution_inverse_cdf(cdf))

if __name__ == '__main__':
  absltest.main()
