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

"""Tests for M.experimental.serial_correlation."""

from absl.testing import absltest
from arolla import arolla

M = arolla.M


class ExperimentalSerialCorrelation(absltest.TestCase):

  def testLagZero(self):
    input_qvalue = arolla.dense_array([1, 2, 3, 4, 5], arolla.FLOAT64)
    actual_qvalue = arolla.eval(
        M.experimental.serial_correlation(series=input_qvalue, lag=0)
    )
    arolla.testing.assert_qvalue_allclose(
        actual_qvalue, arolla.optional_float64(1.0)
    )

  def testLagOne(self):
    input_qvalue = arolla.dense_array([1, 4, 2, 10, 4], arolla.FLOAT64)
    actual_qvalue = arolla.eval(
        M.experimental.serial_correlation(series=input_qvalue, lag=1)
    )
    arolla.testing.assert_qvalue_allclose(
        actual_qvalue, arolla.optional_float64(-0.31031644541708764)
    )

  def testMissingValues(self):
    with self.subTest('value'):
      input_qvalue = arolla.dense_array(
          [1, 4, None, 10, 4, 5, 2, 3, 7], arolla.FLOAT64
      )
      actual_qvalue = arolla.eval(
          M.experimental.serial_correlation(series=input_qvalue, lag=1)
      )
      arolla.testing.assert_qvalue_allclose(
          actual_qvalue, arolla.optional_float64(-0.11532107388889594)
      )
    with self.subTest('nan'):
      input_qvalue = arolla.dense_array([1, 2, None, None], arolla.FLOAT64)
      actual_qvalue = arolla.eval(
          M.experimental.serial_correlation(series=input_qvalue, lag=1)
      )
      arolla.testing.assert_qvalue_allclose(
          actual_qvalue, arolla.optional_float64(float('nan'))
      )


if __name__ == '__main__':
  absltest.main()
