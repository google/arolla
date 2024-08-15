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

"""Tests for M.core.apply operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


class CoreApplyTest(parameterized.TestCase):

  def testInferenceNone(self):
    expr = M.core.apply(M.math.add, 1, L.x)
    self.assertIsNone(expr.qtype)
    self.assertIsNone(expr.qvalue)

  def testInferenceQType(self):
    expr = M.core.apply(M.math.add, 1, 2)
    self.assertEqual(expr.qtype, arolla.INT32)
    self.assertIsNone(expr.qvalue)

  def testInferenceQValue(self):
    expr = M.core.apply(M.qtype.qtype_of, 1)
    self.assertEqual(expr.qtype, arolla.QTYPE)
    self.assertEqual(expr.qvalue, arolla.INT32)

  def testToLowerNoop(self):
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.apply(L.x, L.x)), M.core.apply(L.x, L.x)
    )

  def testToLowerRegular(self):
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.apply(M.math.add, 1, L.x)), 1 + L.x
    )

  def testToLowerLiteral(self):
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(M.core.apply(M.qtype.qtype_of, 1)),
        arolla.abc.literal(arolla.INT32),
    )

  def testError_NotOp(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an operator, got op: INT32')
    ):
      _ = M.core.apply(1)

  def testError_NonLiteralOp(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`op` has to be literal')
    ):
      _ = M.core.apply(M.annotation.qtype(L.op, arolla.OPERATOR))

  def testError_Context(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f"while calling core.apply with args {{{M.math.add}, 1, b'foo'}}"
        ),
    ):
      _ = M.core.apply(M.math.add, 1, b'foo')


if __name__ == '__main__':
  absltest.main()
