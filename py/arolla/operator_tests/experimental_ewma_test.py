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

"""Tests for M.experimental.ewma."""

from absl.testing import absltest
from arolla import arolla

M = arolla.M


class ExperimentalEwma(absltest.TestCase):

  def testWithDenseArray(self):
    input_qvalue = arolla.dense_array(
        [1, 2, 3, None, 5, 6, 7, 8], arolla.FLOAT32
    )
    expected_qvalue = arolla.dense_array(
        [1, 1.6, 2.44, 2.44, 3.976, 5.1904, 6.27616, 7.310464], arolla.FLOAT32
    )
    actual_qvalue = arolla.eval(
        M.experimental.ewma(
            series=input_qvalue, alpha=0.6, adjust=False, ignore_missing=True
        )
    )
    arolla.testing.assert_qvalue_allclose(actual_qvalue, expected_qvalue)

  def testDefaultArguments(self):
    input_qvalue = arolla.dense_array(
        [1, 2, 3, None, 5, 6, 7, 8], arolla.FLOAT32
    )
    expected_qvalue = arolla.dense_array(
        [
            1,
            1.7142857,
            2.5384614,
            2.5384614,
            4.5083227,
            5.50288,
            6.4386177,
            7.390695,
        ],
        arolla.FLOAT32,
    )
    actual_qvalue = arolla.eval(
        M.experimental.ewma(series=input_qvalue, alpha=0.6)
    )
    arolla.testing.assert_qvalue_allclose(actual_qvalue, expected_qvalue)


if __name__ == '__main__':
  absltest.main()
