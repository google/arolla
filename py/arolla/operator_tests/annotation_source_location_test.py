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

import inspect
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
    loc = arolla.namedtuple(
        function_name='func',
        file_name='file.py',
        line=1,
        column=2,
        line_text='x = y + 1',
    )
    x1 = M.annotation.source_location(L.x, loc)
    x2 = M.annotation.source_location(L.x, loc=loc)
    arolla.testing.assert_expr_equal_by_fingerprint(x1, x2)

  def test_error_non_literal(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('invalid argument for `loc`'),
    ):
      M.annotation.source_location(L.x, P.x)

  def test_error_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('invalid argument for `loc`'),
    ):
      M.annotation.source_location(L.x, arolla.text('func'))

    with self.assertRaisesRegex(
        TypeError,
        re.escape('takes 2 positional arguments but 6 were given'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', 1, 2, 'x = y + 1'
      )

  def test_eval_support_simple(self):
    expr = M.annotation.source_location(
        L.x // L.y,  # pyrefly: ignore[unsupported-operation]
        arolla.namedtuple(
            function_name='func',
            file_name='file.py',
            line=57,
            column=7,
            line_text='L.x // L.y',
        ),
    )
    try:
      eval_util.eval_with_expr_stack_trace(expr, x=1, y=0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'division by zero')  # pyrefly: ignore[unbound-name]
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(tb, 'file.py.*line 57.*func')

    tb_frames = traceback.extract_tb(ex.__traceback__)
    file_frame = next(f for f in tb_frames if f.filename == 'file.py')
    self.assertEqual(file_frame.lineno, 57)
    self.assertEqual(file_frame.colno, 7)

  @parameterized.parameters(False, True)
  def test_eval_support_lambdas(self, literal_folding):
    frame = inspect.currentframe()
    assert frame is not None  # Make pytype happy.

    inner_line = frame.f_lineno + 3
    @arolla.optools.as_lambda_operator('inner_lambda')
    def inner_lambda(x, y):
      return x // y

    outer_line = frame.f_lineno + 3
    @arolla.optools.as_lambda_operator('outer_lambda')
    def outer_lambda(x, y):
      inner = inner_lambda(x, y)
      return inner + 1

    lambda_line = frame.f_lineno + 2
    expr = arolla.optools.trace_function(
        lambda x, y: outer_lambda(x, y),  # pylint: disable=unnecessary-lambda
        gen_tracer=arolla.abc.leaf,
        annotate_with_source_locations=True,
    )

    if literal_folding:
      expr = arolla.abc.sub_by_fingerprint(
          expr,
          {
              L.x.fingerprint: arolla.abc.literal(arolla.int32(1)),
              L.y.fingerprint: arolla.abc.literal(arolla.int32(0)),
          },
      )

    try:
      if literal_folding:
        eval_util.eval_with_expr_stack_trace(expr)
      else:
        eval_util.eval_with_expr_stack_trace(expr, x=1, y=0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'division by zero')  # pyrefly: ignore[unbound-name]
    tb_frames = traceback.extract_tb(ex.__traceback__)

    inner_frame = next(f for f in tb_frames if f.name == 'inner_lambda')
    self.assertEqual(inner_frame.lineno, inner_line)
    self.assertEqual(inner_frame.colno, 13)
    self.assertEqual(inner_frame.line, 'return x // y')

    outer_frame = next(f for f in tb_frames if f.name == 'outer_lambda')
    self.assertEqual(outer_frame.lineno, outer_line)
    self.assertEqual(outer_frame.colno, 14)
    self.assertEqual(outer_frame.line, 'inner = inner_lambda(x, y)')

    lambda_frame = next(
        f for f in tb_frames if f.name == '<lambda>' and f.lineno == lambda_line
    )
    self.assertEqual(lambda_frame.colno, 21)
    self.assertIn('lambda x, y: outer_lambda(x, y)', lambda_frame.line)  # pyrefly: ignore[bad-argument-type]

    # The `inner + 1` annotation is an ancestor of inner_lambda(x, y) in the
    # expression, but semantically does not belong to the stack trace.
    self.assertFalse(any('inner + 1' in (f.line or '') for f in tb_frames))

  def test_source_location_under_anonymous_lambda(self):
    x = arolla.abc.leaf('x')
    y = arolla.abc.leaf('y')

    @arolla.optools.as_lambda_operator('anonymous.lambda')
    def lambda_op(x, y):
      return M.annotation.source_location(
          M.math.floordiv(x, y),
          arolla.namedtuple(
              function_name='main',
              file_name='file.py',
              line=1,
              column=2,
              line_text='x // y',
          ),
      )

    expr = lambda_op(x, y)

    with self.assertRaises(ValueError) as cm:
      eval_util.eval_with_expr_stack_trace(expr, x=1, y=0)

    self.assertEqual(
        getattr(cm.exception, '__notes__', []),
        ['operator_name: math.floordiv'],
    )

  def test_repr(self):
    expr = M.annotation.source_location(
        L.x,
        arolla.namedtuple(
            function_name='func',
            file_name='file.py',
            line=1,
            column=2,
            line_text='x = y + 1',
        ),
    )
    self.assertEqual(repr(expr), 'L.x📍')


if __name__ == '__main__':
  absltest.main()
