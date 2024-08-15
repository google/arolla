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

"""Tests for M.qtype.is_scalar_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

TEST_CASES = (
    (arolla.UNIT, arolla.present()),
    (arolla.BOOLEAN, arolla.present()),
    (arolla.INT32, arolla.present()),
    (arolla.FLOAT32, arolla.present()),
    (arolla.OPTIONAL_UNIT, arolla.missing()),
    (arolla.OPTIONAL_BOOLEAN, arolla.missing()),
    (arolla.OPTIONAL_INT32, arolla.missing()),
    (arolla.OPTIONAL_FLOAT32, arolla.missing()),
    (arolla.DENSE_ARRAY_UNIT, arolla.missing()),
    (arolla.DENSE_ARRAY_BOOLEAN, arolla.missing()),
    (arolla.DENSE_ARRAY_INT32, arolla.missing()),
    (arolla.DENSE_ARRAY_FLOAT32, arolla.missing()),
    (arolla.ARRAY_UNIT, arolla.missing()),
    (arolla.ARRAY_BOOLEAN, arolla.missing()),
    (arolla.ARRAY_INT32, arolla.missing()),
    (arolla.ARRAY_FLOAT32, arolla.missing()),
    (arolla.DENSE_ARRAY_EDGE, arolla.missing()),
    (arolla.DENSE_ARRAY_TO_SCALAR_EDGE, arolla.missing()),
    (arolla.DENSE_ARRAY_SHAPE, arolla.missing()),
    (arolla.ARRAY_EDGE, arolla.missing()),
    (arolla.ARRAY_TO_SCALAR_EDGE, arolla.missing()),
    (arolla.ARRAY_SHAPE, arolla.missing()),
    (arolla.NOTHING, arolla.missing()),
    (arolla.make_tuple_qtype(), arolla.missing()),
    (arolla.make_tuple_qtype(arolla.INT32), arolla.missing()),
)

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeIsScalarQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.qtype.is_scalar_qtype),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.is_scalar_qtype(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
