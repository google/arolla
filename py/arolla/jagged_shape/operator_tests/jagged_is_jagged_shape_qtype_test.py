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

"""Tests for M.jagged.is_jagged_shape_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape
from arolla.operator_tests import pointwise_test_utils

M = arolla.OperatorsContainer(jagged_shape)
L = arolla.L

# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = (
    (arolla.NOTHING, False),
    (arolla.INT32, False),
    (arolla.OPTIONAL_INT32, False),
    (arolla.ARRAY_INT32, False),
    (arolla.make_tuple_qtype(), False),
    (arolla.make_tuple_qtype(arolla.INT32, arolla.INT32, arolla.INT32), False),
    (jagged_shape.JAGGED_ARRAY_SHAPE, True),
    (jagged_shape.JAGGED_DENSE_ARRAY_SHAPE, True),
)

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.OPTIONAL_UNIT),)


class JaggedIsJaggedShapeQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(
            M.jagged.is_jagged_shape_qtype
        ),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.jagged.is_jagged_shape_qtype(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_error(self):
    with self.assertRaisesRegex(ValueError, "expected QTYPE, got x: INT32"):
      _ = M.jagged.is_jagged_shape_qtype(arolla.int32(1))


if __name__ == "__main__":
  absltest.main()
