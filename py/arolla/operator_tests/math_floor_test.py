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

"""Tests for M.math.floor operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
import numpy.testing

NAN = float("nan")
INF = float("inf")

FLOAT32_MANTISSA_DIGITS = 24
FLOAT64_MANTISSA_DIGITS = 53

M = arolla.M

# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = (
    (None, None),
    (-INF, -INF),
    (-10, -10),
    (-5.1, -6.0),
    (-4.9, -5.0),
    (-1, -1),
    (-0.0, -0.0),
    (0, 0),
    (1, 1),
    (4.9, 4.0),
    (5.1, 5.0),
    (10, 10),
    (INF, INF),
    (NAN, NAN),
)

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = tuple(
    (arg, arg)
    for arg in pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
)


class MathFloorTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.floor, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = self.eval(M.math.floor(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(
      (  # max non-integer float32
          arolla.float32((2**FLOAT32_MANTISSA_DIGITS - 1) / 2),
          2 ** (FLOAT32_MANTISSA_DIGITS - 1) - 1,
      ),
      (  # max non-integer float32
          arolla.float64((2**FLOAT64_MANTISSA_DIGITS - 1) / 2),
          2 ** (FLOAT64_MANTISSA_DIGITS - 1) - 1,
      ),
      (  # min non-integer float32
          arolla.float32(-(2**FLOAT32_MANTISSA_DIGITS - 1) / 2),
          -(2 ** (FLOAT32_MANTISSA_DIGITS - 1)),
      ),
      (  # min non-integer float64
          arolla.float64(-(2**FLOAT64_MANTISSA_DIGITS - 1) / 2),
          -(2 ** (FLOAT64_MANTISSA_DIGITS - 1)),
      ),
  )
  def testExtremeValue(self, arg, expected_py_value):
    actual_value = self.eval(M.math.floor(arg))
    self.assertFalse(arg.py_value().is_integer())
    self.assertTrue(actual_value.py_value().is_integer())
    numpy.testing.assert_equal(actual_value.py_value(), expected_py_value)


if __name__ == "__main__":
  absltest.main()
