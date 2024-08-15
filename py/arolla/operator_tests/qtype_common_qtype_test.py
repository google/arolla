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

"""Tests for M.qtype.common_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


TEST_CASES = (
    (arolla.UNIT, arolla.UNIT, arolla.UNIT),
    (arolla.UNIT, arolla.OPTIONAL_UNIT, arolla.OPTIONAL_UNIT),
    (arolla.OPTIONAL_UNIT, arolla.UNIT, arolla.OPTIONAL_UNIT),
    (arolla.BOOLEAN, arolla.UNIT, arolla.NOTHING),
    (arolla.BOOLEAN, arolla.INT32, arolla.NOTHING),
    (arolla.BOOLEAN, arolla.FLOAT32, arolla.NOTHING),
    (arolla.BOOLEAN, arolla.OPTIONAL_UNIT, arolla.NOTHING),
    (arolla.FLOAT32, arolla.OPTIONAL_WEAK_FLOAT, arolla.OPTIONAL_FLOAT32),
    (arolla.INT32, arolla.OPTIONAL_WEAK_FLOAT, arolla.OPTIONAL_FLOAT32),
    (arolla.INT64, arolla.OPTIONAL_WEAK_FLOAT, arolla.OPTIONAL_FLOAT32),
    (arolla.FLOAT64, arolla.OPTIONAL_WEAK_FLOAT, arolla.OPTIONAL_FLOAT64),
    (arolla.OPTIONAL_INT32, arolla.WEAK_FLOAT, arolla.OPTIONAL_FLOAT32),
    (arolla.OPTIONAL_FLOAT64, arolla.ARRAY_FLOAT32, arolla.ARRAY_FLOAT64),
    (arolla.INT64, arolla.OPTIONAL_INT32, arolla.OPTIONAL_INT64),
    (arolla.INT64, arolla.FLOAT64, arolla.FLOAT64),
    (arolla.ARRAY_FLOAT64, arolla.DENSE_ARRAY_FLOAT64, arolla.NOTHING),
)

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeCommonQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.qtype.common_qtype),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, arg_1, arg_2, expected_value):
    actual_value = arolla.eval(M.qtype.common_qtype(arg_1, arg_2))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
