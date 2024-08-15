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

"""Tests for M.core.apply_varargs."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
P = arolla.P
M = arolla.M


class CoreApplyTupleTest(parameterized.TestCase):

  @parameterized.parameters(
      (M.core.apply_varargs(L.op, M.core.make_tuple(5)), None),
      (M.core.apply_varargs(M.core.has, L.x), None),
      (M.core.apply_varargs(M.core.has, M.core.make_tuple(L.x)), None),
      (M.core.apply_varargs(M.math.neg, 5, M.core.make_tuple()), arolla.INT32),
      (M.core.apply_varargs(M.math.neg, M.core.make_tuple(5)), arolla.INT32),
      (
          M.core.apply_varargs(M.math.multiply, 3, 19, M.core.make_tuple()),
          arolla.INT32,
      ),
      (
          M.core.apply_varargs(M.math.multiply, 3, M.core.make_tuple(19)),
          arolla.INT32,
      ),
      (
          M.core.apply_varargs(M.math.multiply, M.core.make_tuple(3, 19)),
          arolla.INT32,
      ),
      (
          M.core.apply_varargs(
              M.core.make_tuple, M.core.make_tuple(1, 2, 3, 4)
          ),
          arolla.make_tuple_qtype(
              arolla.INT32, arolla.INT32, arolla.INT32, arolla.INT32
          ),
      ),
      (
          M.core.apply_varargs(
              M.core.make_tuple, 1, 2, M.core.make_tuple(3, 4)
          ),
          arolla.make_tuple_qtype(
              arolla.INT32, arolla.INT32, arolla.INT32, arolla.INT32
          ),
      ),
  )
  def testQType(self, expr, expected_qtype):
    self.assertEqual(expr.qtype, expected_qtype)

  @parameterized.parameters(
      (M.core.apply_varargs(L.op, M.core.make_tuple(5))),
      (M.core.apply_varargs(M.core.has, L.x)),
      (M.core.apply_varargs(M.core.has, M.core.make_tuple(L.x))),
      (M.core.apply_varargs(M.core.has, L.x, M.core.make_tuple(5))),
  )
  def testNoToLower(self, expr):
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(expr), expr
    )

  @parameterized.parameters(
      (
          M.core.apply_varargs(M.math.neg, M.core.make_tuple(5)),
          M.math.neg(arolla.types.GetNthOperator(0)(M.core.make_tuple(5))),
      ),
      (
          M.core.apply_varargs(M.math.multiply, 3, 19, M.core.make_tuple()),
          M.math.multiply(3, 19),
      ),
      (
          M.core.apply_varargs(M.math.multiply, 3, M.core.make_tuple(19)),
          M.math.multiply(
              3,
              arolla.types.GetNthOperator(0)(M.core.make_tuple(19)),
          ),
      ),
      (
          M.core.apply_varargs(M.math.multiply, M.core.make_tuple(3, 19)),
          M.math.multiply(
              arolla.types.GetNthOperator(0)(M.core.make_tuple(3, 19)),
              arolla.types.GetNthOperator(1)(M.core.make_tuple(3, 19)),
          ),
      ),
      (
          M.core.apply_varargs(
              M.core.make_tuple, M.core.make_tuple(1, 2, 3, 4)
          ),
          M.core.make_tuple(
              arolla.types.GetNthOperator(0)(M.core.make_tuple(1, 2, 3, 4)),
              arolla.types.GetNthOperator(1)(M.core.make_tuple(1, 2, 3, 4)),
              arolla.types.GetNthOperator(2)(M.core.make_tuple(1, 2, 3, 4)),
              arolla.types.GetNthOperator(3)(M.core.make_tuple(1, 2, 3, 4)),
          ),
      ),
  )
  def testToLower(self, expr, lower_expr):
    arolla.testing.assert_expr_equal_by_fingerprint(
        arolla.abc.to_lower_node(expr), lower_expr
    )

  @parameterized.parameters(
      (
          M.core.apply_varargs(M.math.neg, M.core.make_tuple(L.x)),
          dict(x=57),
          arolla.int32(-57),
      ),
      (
          M.core.apply_varargs(M.math.multiply, M.core.make_tuple(L.x, L.y)),
          dict(x=3, y=19),
          arolla.int32(57),
      ),
      (
          M.core.apply_varargs(
              M.core.make_tuple, L.x, L.x, M.core.make_tuple(L.x, L.x)
          ),
          dict(x=57),
          arolla.tuple(57, 57, 57, 57),
      ),
      (
          M.core.apply_varargs(
              M.core.make_tuple, M.core.make_tuple(L.x, L.x, L.x, L.x)
          ),
          dict(x=57),
          arolla.tuple(57, 57, 57, 57),
      ),
  )
  def testValue(self, expr, args, expected):
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(expr, **args), expected
    )

  def testError(self):
    with self.subTest('too-few-args'):
      with self.assertRaisesRegex(
          ValueError, re.escape('too few arguments: expected at least 2, got 1')
      ):
        M.core.apply_varargs(M.math.neg)
    with self.subTest('expected-op'):
      with self.assertRaisesRegex(
          ValueError, re.escape('expected an operator, got op: INT32')
      ):
        M.core.apply_varargs(M.annotation.qtype(L.op, arolla.INT32), L.tuple)
    with self.subTest('expected-tuple'):
      with self.assertRaisesRegex(
          ValueError, re.escape('expected a tuple, got args_tuple: INT32')
      ):
        M.core.apply_varargs(L.op, 1)


if __name__ == '__main__':
  absltest.main()
