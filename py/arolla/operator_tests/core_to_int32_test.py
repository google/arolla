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

"""Tests for M.core.to_int32 operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
import numpy

INF = float("inf")
NAN = float("nan")

M = arolla.M

# Exact limits of int32.
_min_value = -(2**31)
_max_value = 2**31 - 1

# Minumal and maximal float32 values that safly castable to int32.
_min_safe_float32_value = -2147483648.0
_max_safe_float32_value = 2147483520.0

# Minumal and maximal float64 values that safly castable to int32.
_min_safe_float64_value = -2147483648.9999995
_max_safe_float64_value = 2147483647.9999998

# Test data: tuple(
#     (input, expected_output),
#     ...
# )
TEST_DATA = (
    (None, None),
    (False, 0),
    (True, 1),
    (0, 0),
    (1, 1),
    (_min_value, _min_value),
    (_max_value, _max_value),
    (1.1, 1),
    (-1.1, -1),
    (1.9, 1),
    (-1.9, -1),
)


def gen_qtype_signatures():
  """Yields qtype signatures for core.to_int32.

  Yields: (arg_qtype, result_qtype)
  """
  for arg in (
      arolla.BOOLEAN,
      arolla.types.UINT64,
  ) + arolla.types.NUMERIC_QTYPES:
    yield from pointwise_test_utils.lift_qtypes((arg, arolla.INT32))


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreToInt32Test(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.core.to_int32)
        ),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES),
  )
  def testValue(self, arg, expected_value):
    actual_value = self.eval(M.core.to_int32(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(
      arolla.int64(_min_value),
      arolla.int64(_max_value),
      arolla.float32(_min_safe_float32_value),
      arolla.float32(_max_safe_float32_value),
      arolla.float64(_min_safe_float64_value),
      arolla.float64(_max_safe_float64_value),
  )
  def testExtremeValue(self, arg):
    actual_value = self.eval(M.core.to_int32(arg))
    expected_value = int(arg)
    self.assertEqual(actual_value.qtype, arolla.INT32)
    numpy.testing.assert_equal(actual_value.py_value(), expected_value)

  @parameterized.parameters(
      arolla.int64(_min_value - 1),
      arolla.int64(_max_value + 1),
      arolla.float32(
          numpy.nextafter(_min_safe_float32_value, -INF, dtype=numpy.float32)
      ),
      arolla.float32(
          numpy.nextafter(_max_safe_float32_value, INF, dtype=numpy.float32)
      ),
      arolla.float64(
          numpy.nextafter(_min_safe_float64_value, -INF, dtype=numpy.float64)
      ),
      arolla.float64(
          numpy.nextafter(_max_safe_float64_value, INF, dtype=numpy.float64)
      ),
      arolla.float32(INF),
      arolla.float32(-INF),
      arolla.float32(NAN),
      arolla.float64(INF),
      arolla.float64(-INF),
      arolla.float64(NAN),
  )
  def testError(self, arg):
    with self.assertRaisesRegex(
        ValueError, re.escape("cannot cast {} to int32".format(arg))
    ):
      _ = self.eval(M.core.to_int32(arg))

  @parameterized.parameters(*pointwise_test_utils.lift_qtypes(arolla.INT32))
  def testOptOut(self, qtype):
    self.require_self_eval_is_called = False
    x = M.annotation.qtype(arolla.L.x, qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.to_int32(x)), x
    )


if __name__ == "__main__":
  absltest.main()
