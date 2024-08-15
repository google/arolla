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

"""Tests for M.core.get_nth."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M
P = arolla.P


class CoreGetNthEvalTest(backend_test_base.SelfEvalMixin):
  """Tests operator evaluation."""

  def testTuple(self):
    x = arolla.eval(
        M.core.make_tuple(
            arolla.float32(0.0), arolla.int32(1), arolla.bytes_(b'2')
        )
    )
    expected_value = arolla.int32(1)
    actual_value = self.eval(M.core.get_nth(x, 1))
    self.assertEqual(actual_value, expected_value)


class CoreGetNthExprTest(parameterized.TestCase):
  """Non-eval operator tests."""

  @parameterized.parameters(
      (arolla.int32),
      (arolla.int64),
      (arolla.optional_int32),
      (arolla.optional_int64),
  )
  def testLowering(self, wrap_int):
    op = arolla.abc.to_lower_node(M.core.get_nth(P.x, wrap_int(2))).op
    self.assertEqual(op, arolla.types.GetNthOperator(2))

  def testLiteralValuePropagation(self):
    self.assertEqual(M.core.get_nth((1, 2, 3), 1).qvalue, 2)

  def testErrorNonIntegralN(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an integer, got n: FLOAT32')
    ):
      M.core.get_nth(P.x, 2.0)

  def testErrorMissingN(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an integer, got n=missing')
    ):
      M.core.get_nth(P.x, arolla.optional_int64(None))

  def testErrorNegativeN(self):
    with self.assertRaisesRegex(
        ValueError, r'expected a non-negative integer, got n=-1'
    ):
      M.core.get_nth(P.x, -1)

  def testErrorNonCompoundType(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a compound type, got value: FLOAT32')
    ):
      M.core.get_nth(1.5, 0)

  def testErrorOutOfRangeN(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('index out of range: n=0, value.field_count=0')
    ):
      M.core.get_nth(M.core.make_tuple(), 0)
    with self.assertRaisesRegex(
        ValueError, re.escape('index out of range: n=3, value.field_count=2')
    ):
      M.core.get_nth(M.core.make_tuple(0, 1), 3)

  def testLoweringLiteralExpression(self):
    expr = arolla.abc.to_lowest(
        arolla.M.core.get_nth(P.x, arolla.M.core.get_nth((1,), 0))
    )
    self.assertEqual(
        expr.fingerprint, arolla.types.GetNthOperator(1)(P.x).fingerprint
    )


if __name__ == '__main__':
  absltest.main()
