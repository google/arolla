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

"""Tests for M.qtype.get_float_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


TEST_CASES = pointwise_test_utils.lift_qtypes(
    (arolla.INT32, arolla.FLOAT32),
    (arolla.INT64, arolla.FLOAT32),
    (arolla.WEAK_FLOAT, arolla.WEAK_FLOAT),
    (arolla.FLOAT32, arolla.FLOAT32),
    (arolla.FLOAT64, arolla.FLOAT64),
) + (
    (arolla.UNSPECIFIED, arolla.NOTHING),
    (arolla.NOTHING, arolla.NOTHING),
    (arolla.UNIT, arolla.NOTHING),
    (arolla.BYTES, arolla.NOTHING),
    (arolla.OPTIONAL_BYTES, arolla.NOTHING),
    (arolla.DENSE_ARRAY_BYTES, arolla.NOTHING),
    (arolla.ARRAY_BYTES, arolla.NOTHING),
)

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeGetFloatQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(M.qtype.get_float_qtype),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.get_float_qtype(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
