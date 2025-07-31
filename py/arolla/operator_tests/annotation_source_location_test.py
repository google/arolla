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

"""Tests for M.annotation.source_location operator."""

import re
import traceback

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.experimental import eval_util


M = arolla.M
L = arolla.L
P = arolla.P


class AnnotationSourceLocationTest(parameterized.TestCase):

  def test_ok(self):
    x1 = M.annotation.source_location(
        L.x, 'func', 'file.py', 1, 2, 'x = y + 1'
    )  # no except
    x2 = M.annotation.source_location(
        L.x,
        function_name='func',
        file_name='file.py',
        line=1,
        column=2,
        line_text='x = y + 1',
    )  # no except
    arolla.testing.assert_expr_equal_by_fingerprint(x1, x2)

  def test_error_non_literal(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`function_name` must be a TEXT literal')
    ):
      M.annotation.source_location(L.x, P.x, 'file.py', 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`file_name` must be a TEXT literal')
    ):
      M.annotation.source_location(L.x, 'func', P.x, 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`line` must be a INT32 literal')
    ):
      M.annotation.source_location(L.x, 'func', 'file.py', P.x, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`column` must be a INT32 literal')
    ):
      M.annotation.source_location(L.x, 'func', 'file.py', 1, P.x, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`line_text` must be a TEXT literal')
    ):
      M.annotation.source_location(L.x, 'func', 'file.py', 1, 2, P.x)

  def test_error_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a TEXT literal, got function_name: BYTES'),
    ):
      M.annotation.source_location(L.x, b'func', 'file.py', 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a TEXT literal, got file_name: BYTES'),
    ):
      M.annotation.source_location(L.x, 'func', b'file.py', 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a INT32 literal, got line: INT64'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', arolla.int64(1), 2, 'x = y + 1'
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a INT32 literal, got column: INT64'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', 1, arolla.int64(2), 'x = y + 1'
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a TEXT literal, got line_text: BYTES'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', 1, 2, b'x = y + 1'
      )

  def test_eval_support_simple(self):
    expr = M.annotation.source_location(
        L.x // L.y, 'func', 'file.py', 57, 2, 'L.x // L.y'
    )
    try:
      eval_util.eval_with_expr_stack_trace(expr, x=1, y=0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'division by zero')
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(tb, 'file.py.*line 57.*func')

  def test_eval_support_lambdas(self):

    @arolla.optools.as_lambda_operator('inner_lambda')
    def inner_lambda(x, y):
      return M.annotation.source_location(
          x // y, 'inner_lambda', 'file.py', 57, 2, 'x // y'
      )

    @arolla.optools.as_lambda_operator('outer_lambda')
    def outer_lambda(x, y):
      inner = M.annotation.source_location(
          inner_lambda(x, y),
          'outer_lambda',
          'file.py',
          58,
          2,
          'inner_lambda(x, y)',
      )
      return M.annotation.source_location(
          inner + 1, 'outer_lambda', 'file.py', 59, 2, 'inner + 1'
      )

    expr = M.annotation.source_location(
        outer_lambda(L.x, L.y),
        'main',
        'file.py',
        60,
        2,
        'outer_lambda(L.x, L.y)',
    )

    try:
      eval_util.eval_with_expr_stack_trace(expr, x=1, y=0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'division by zero')
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(tb, 'file.py.*line 57.*inner_lambda')
    self.assertRegex(tb, 'file.py.*line 58.*outer_lambda')
    # file.py:59 annotation is an ancestor of the broken inner_lambda(x, y)
    # in the expression, but semantically does not belong to the stack trace and
    # so is not included.
    self.assertNotIn('line 59', tb)
    self.assertRegex(tb, 'file.py.*line 60.*main')

  def test_eval_support_literal_folding(self):

    @arolla.optools.as_lambda_operator('inner_lambda')
    def inner_lambda(x, y):
      return M.annotation.source_location(
          x // y, 'inner_lambda', 'file.py', 57, 2, 'x // y'
      )

    @arolla.optools.as_lambda_operator('outer_lambda')
    def outer_lambda(x, y):
      inner = M.annotation.source_location(
          inner_lambda(x, y),
          'outer_lambda',
          'file.py',
          58,
          2,
          'inner_lambda(x, y)',
      )
      return M.annotation.source_location(
          inner + 1, 'outer_lambda', 'file.py', 59, 2, 'inner + 1'
      )

    expr = M.annotation.source_location(
        outer_lambda(1, 0),
        'main',
        'file.py',
        60,
        2,
        'outer_lambda(1, 0)',
    )

    try:
      eval_util.eval_with_expr_stack_trace(expr)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'division by zero')
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(tb, 'file.py.*line 57.*inner_lambda')
    self.assertRegex(tb, 'file.py.*line 58.*outer_lambda')
    # file.py:59 annotation is an ancestor of the broken inner_lambda(x, y)
    # in the expression, but semantically does not belong to the stack trace and
    # so is not included.
    self.assertNotIn('line 59', tb)
    self.assertRegex(tb, 'file.py.*line 60.*main')


if __name__ == '__main__':
  absltest.main()
