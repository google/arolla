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

"""Tests for M.qtype.get_shape_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

L = arolla.L
M = arolla.M


TEST_CASES = (
    (arolla.UNIT, arolla.SCALAR_SHAPE),
    (arolla.BOOLEAN, arolla.SCALAR_SHAPE),
    (arolla.INT32, arolla.SCALAR_SHAPE),
    (arolla.FLOAT32, arolla.SCALAR_SHAPE),
    (arolla.OPTIONAL_UNIT, arolla.OPTIONAL_SCALAR_SHAPE),
    (arolla.OPTIONAL_BOOLEAN, arolla.OPTIONAL_SCALAR_SHAPE),
    (arolla.OPTIONAL_INT32, arolla.OPTIONAL_SCALAR_SHAPE),
    (arolla.OPTIONAL_FLOAT32, arolla.OPTIONAL_SCALAR_SHAPE),
    (arolla.DENSE_ARRAY_UNIT, arolla.DENSE_ARRAY_SHAPE),
    (arolla.DENSE_ARRAY_BOOLEAN, arolla.DENSE_ARRAY_SHAPE),
    (arolla.DENSE_ARRAY_INT32, arolla.DENSE_ARRAY_SHAPE),
    (arolla.DENSE_ARRAY_FLOAT32, arolla.DENSE_ARRAY_SHAPE),
    (arolla.ARRAY_UNIT, arolla.ARRAY_SHAPE),
    (arolla.ARRAY_BOOLEAN, arolla.ARRAY_SHAPE),
    (arolla.ARRAY_INT32, arolla.ARRAY_SHAPE),
    (arolla.ARRAY_FLOAT32, arolla.ARRAY_SHAPE),
    (arolla.DENSE_ARRAY_EDGE, arolla.NOTHING),
    (arolla.DENSE_ARRAY_TO_SCALAR_EDGE, arolla.NOTHING),
    (arolla.DENSE_ARRAY_SHAPE, arolla.NOTHING),
    (arolla.ARRAY_EDGE, arolla.NOTHING),
    (arolla.ARRAY_TO_SCALAR_EDGE, arolla.NOTHING),
    (arolla.ARRAY_SHAPE, arolla.NOTHING),
    (arolla.NOTHING, arolla.NOTHING),
    (arolla.make_tuple_qtype(), arolla.NOTHING),
    (arolla.make_tuple_qtype(arolla.INT32), arolla.NOTHING),
)

QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeGetShapeQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.qtype.get_shape_qtype),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.get_shape_qtype(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(*TEST_CASES)
  def test_infer_attr(self, arg, expected_value):
    actual_value = M.qtype.get_shape_qtype(arg).qvalue
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
