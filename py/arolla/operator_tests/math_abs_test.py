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

"""Tests for M.math.abs operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")
INF = float("inf")

M = arolla.M

# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = (
    (None, None),
    (-INF, INF),
    (-100.5, 100.5),
    (-10, 10),
    (-1, 1),
    (-0.0, 0.0),
    (0, 0),
    (1, 1),
    (10, 10),
    (100.5, 100.5),
    (INF, INF),
    (NAN, NAN),
    # Test data for integer overflow.
    (arolla.int32(-(2**31)), -(2**31)),
    (arolla.int64(-(2**63)), -(2**63)),
)

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = tuple(
    (arg, arg)
    for arg in pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
)


class MathAbsTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.math.abs, QTYPE_SIGNATURES)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg, expected_value):
    actual_value = self.eval(M.math.abs(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
