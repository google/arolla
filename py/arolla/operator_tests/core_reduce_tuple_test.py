# Copyright 2025 Google LLC
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

"""Tests for M.core.reduce_tuple."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
P = arolla.P
M = arolla.M

_add_n = arolla.LambdaOperator('*args', M.core.reduce_tuple(M.math.add, P.args))  # pyrefly: ignore[missing-attribute]


def _expected_lower_expr(op, tuple_value):
  tuple_expr = arolla.as_expr(tuple_value)
  result = M.core.get_nth(tuple_expr, arolla.int64(0))  # pyrefly: ignore[missing-attribute]
  for i in range(1, len(tuple_value)):
    result = op(result, M.core.get_nth(tuple_expr, arolla.int64(i)))  # pyrefly: ignore[missing-attribute]
  return result


class CoreReduceTupleTest(parameterized.TestCase):

  @parameterized.parameters(
      (L.op, L.tuple, None),
      (M.math.add, L.tuple, None),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0,), arolla.FLOAT32),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0, 2.0), arolla.FLOAT32),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0, arolla.float64(2.0)), arolla.FLOAT64),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1,), arolla.INT32),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1, 2), arolla.INT32),  # pyrefly: ignore[missing-attribute]
      (M.math.add, tuple(range(5)), arolla.INT32),  # pyrefly: ignore[missing-attribute]
  )
  def testReturnQType(self, arg_1, arg_2, expected_return_qtype):
    self.assertEqual(
        M.core.reduce_tuple(arg_1, arg_2).qtype, expected_return_qtype  # pyrefly: ignore[missing-attribute]
    )

  @parameterized.parameters(
      (L.op, L.tuple, M.core.reduce_tuple(L.op, L.tuple)),  # pyrefly: ignore[missing-attribute]
      (M.math.add, L.tuple, M.core.reduce_tuple(M.math.add, L.tuple)),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0,), _expected_lower_expr(M.math.add, (1.0,))),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0, 2.0), _expected_lower_expr(M.math.add, (1.0, 2.0))),  # pyrefly: ignore[missing-attribute]
      (
          M.math.add,  # pyrefly: ignore[missing-attribute]
          (1.0, arolla.float64(2.0)),
          _expected_lower_expr(M.math.add, (1.0, arolla.float64(2.0))),  # pyrefly: ignore[missing-attribute]
      ),
      (M.math.add, (1,), _expected_lower_expr(M.math.add, (1,))),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1, 2), _expected_lower_expr(M.math.add, (1, 2))),  # pyrefly: ignore[missing-attribute]
      (
          M.math.add,  # pyrefly: ignore[missing-attribute]
          tuple(range(5)),
          _expected_lower_expr(M.math.add, tuple(range(5))),  # pyrefly: ignore[missing-attribute]
      ),
  )
  def testToLower(self, arg_1, arg_2, expected_lowered_expr):
    self.assertEqual(
        arolla.abc.to_lower_node(M.core.reduce_tuple(arg_1, arg_2)).fingerprint,  # pyrefly: ignore[missing-attribute]
        expected_lowered_expr.fingerprint,
    )

  @parameterized.parameters(
      (M.math.add, (1.0,), 1.0),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0, 2.0), 3.0),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1.0, arolla.float64(2.0)), 3.0),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1,), 1),  # pyrefly: ignore[missing-attribute]
      (M.math.add, (1, 2), 3),  # pyrefly: ignore[missing-attribute]
      (M.math.add, tuple(range(5)), 10),  # pyrefly: ignore[missing-attribute]
  )
  def testReturnValue(self, arg_1, arg_2, expected_return_value):
    self.assertEqual(
        arolla.eval(M.core.reduce_tuple(arg_1, arg_2)), expected_return_value  # pyrefly: ignore[missing-attribute]
    )

  def testError(self):
    with self.subTest('expected-op'):
      with self.assertRaisesRegex(
          ValueError, re.escape('expected EXPR_OPERATOR, got op:INT32')
      ):
        M.core.reduce_tuple(M.annotation.qtype(L.op, arolla.INT32), L.tuple)  # pyrefly: ignore[missing-attribute, not-callable]
    with self.subTest('op-must-be-literal'):
      with self.assertRaisesRegex(
          ValueError, re.escape('`op` must be literal')
      ):
        M.core.reduce_tuple(M.annotation.qtype(L.op, arolla.OPERATOR), L.tuple)  # pyrefly: ignore[missing-attribute, not-callable]
    with self.subTest('expected-binary-op'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "expected a binary operator, got <RegisteredOperator 'math.exp'>"
          ),
      ):
        M.core.reduce_tuple(M.math.exp, L.tuple)  # pyrefly: ignore[missing-attribute]
    with self.subTest('expected-tuple'):
      with self.assertRaisesRegex(
          ValueError, re.escape('expected a tuple, got tuple: INT32')
      ):
        M.core.reduce_tuple(L.op, 1)  # pyrefly: ignore[missing-attribute]
    with self.subTest('empty-tuple'):
      with self.assertRaisesRegex(
          ValueError, re.escape('unable to reduce an empty tuple')
      ):
        M.core.reduce_tuple(L.op, ())  # pyrefly: ignore[missing-attribute]

  def testDemoAddN(self):
    self.assertEqual(arolla.eval(_add_n(1, 2, 3, 4)), 10)


if __name__ == '__main__':
  absltest.main()
