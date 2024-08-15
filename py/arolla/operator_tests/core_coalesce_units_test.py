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

"""Tests for M.core.coalesce_units operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


class CoreCoalesceUnitsTest(parameterized.TestCase):

  @parameterized.parameters(
      (L.x),
      (arolla.unit(), L.x),
      (L.x, arolla.unit()),
      (L.x, arolla.unit(), arolla.as_expr(1)),
      (arolla.unit(), L.x, arolla.as_expr(1)),
      (arolla.unit(), arolla.unit(), L.x),
  )
  def testEmpty(self, *args):
    expr = M.core.coalesce_units(*args)
    self.assertIsNone(expr.qtype)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(expr), expr
    )

  @parameterized.parameters(
      (arolla.as_expr(1),),
      (arolla.as_expr(1), arolla.unit()),
      (arolla.as_expr(1), L.x),
      (arolla.as_expr(1), arolla.as_expr(1.5)),
      (arolla.unit(), arolla.as_expr(1), arolla.unit()),
      (arolla.unit(), arolla.as_expr(1), L.x),
      (arolla.unit(), arolla.as_expr(1), arolla.as_expr(1.5)),
      (arolla.unit(), arolla.unit(), arolla.as_expr(1)),
  )
  def testLiteral(self, *args):
    expr = M.core.coalesce_units(*args)
    self.assertEqual(expr.qtype, arolla.INT32)
    self.assertEqual(expr.qvalue, arolla.int32(1))
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(expr), arolla.as_expr(1)
    )

  @parameterized.parameters(
      (M.annotation.qtype(L.x, arolla.INT32),),
      (M.annotation.qtype(L.x, arolla.INT32), arolla.unit()),
      (M.annotation.qtype(L.x, arolla.INT32), L.x),
      (M.annotation.qtype(L.x, arolla.INT32), arolla.as_expr(1.5)),
      (arolla.unit(), M.annotation.qtype(L.x, arolla.INT32), arolla.unit()),
      (arolla.unit(), M.annotation.qtype(L.x, arolla.INT32), L.x),
      (
          arolla.unit(),
          M.annotation.qtype(L.x, arolla.INT32),
          arolla.as_expr(1.5),
      ),
      (arolla.unit(), arolla.unit(), M.annotation.qtype(L.x, arolla.INT32)),
  )
  def testNonLiteral(self, *args):
    expr = M.core.coalesce_units(*args)
    self.assertEqual(expr.qtype, arolla.INT32)
    self.assertIsNone(expr.qvalue)
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lowest(expr), M.annotation.qtype(L.x, arolla.INT32)
    )

  @parameterized.parameters(
      (),
      (arolla.unit(),),
      (arolla.unit(), arolla.unit()),
      (arolla.unit(), arolla.unit(), arolla.unit()),
  )
  def testError_AllUnits(self, *args):
    with self.assertRaisesRegex(
        ValueError, re.escape('at least one argument must be non-unit')
    ):
      _ = M.core.coalesce_units(*args)


if __name__ == '__main__':
  absltest.main()
