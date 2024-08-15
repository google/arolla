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

"""Tests for M.core.to_bool operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

INF = float("inf")
NAN = float("nan")

M = arolla.M

# Test data: tuple(
#     (input, expected_output),
#     ...
# )
TEST_DATA = (
    (None, None),
    (False, False),
    (True, True),
    (0, False),
    (1, True),
    (57, True),
    (0.0, False),
    (-0.0, False),
    (1.1, True),
    (-1.1, True),
    (NAN, True),
    (INF, True),
    (-INF, True),
    (-1.1, True),
)


def gen_qtype_signatures():
  """Yields qtype signatures for core.to_bool.

  Yields: (arg_qtype, result_qtype)
  """
  for arg in (
      arolla.BOOLEAN,
      arolla.types.UINT64,
  ) + arolla.types.NUMERIC_QTYPES:
    yield from pointwise_test_utils.lift_qtypes((arg, arolla.BOOLEAN))


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreToBoolTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(pointwise_test_utils.detect_qtype_signatures(M.core.to_bool)),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES),
  )
  def testValue(self, arg, expected_value):
    actual_value = self.eval(M.core.to_bool(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(*pointwise_test_utils.lift_qtypes(arolla.BOOLEAN))
  def testOptOut(self, qtype):
    self.require_self_eval_is_called = False
    x = M.annotation.qtype(arolla.L.x, qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.to_bool(x)), x
    )


if __name__ == "__main__":
  absltest.main()
