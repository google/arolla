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

"""Tests for eval_util."""

import re
import time
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.experimental import eval_util


M = arolla.M
L = arolla.L
P = arolla.P


@arolla.optools.as_py_function_operator(
    'test.sleep_fn', qtype_inference_expr=P.x
)
def sleep_fn(x):
  del x
  time.sleep(0.5)
  raise ValueError('fn must fail')


ERROR_TEMPL = (
    'Evaluation of:\n\n  {expr_str}\n\nwith leaf values:\n\n'
    '{leaf_values_str}\n\nfailed. Specifically, the evaluation of the'
    ' following sub-expression failed:\n\n  {sub_expr_str}\n\nwhen'
    ' evaluated on the resolved input arguments as:\n\n '
    ' {bound_node_str}'
)


class EvalUsingVisitorTest(parameterized.TestCase):

  def assertErrorFormat(
      self, expr, values, expected_error, expected_cause=None
  ):
    raised_error = None
    try:
      eval_util.eval_using_visitor(expr, **values)
    except ValueError as e:
      raised_error = e
    self.assertIsNotNone(raised_error)
    self.assertEqual(str(raised_error), expected_error)
    if expected_cause is not None:
      self.assertIsNotNone(raised_error.__cause__)
      self.assertEqual(str(raised_error.__cause__), expected_cause)

  @parameterized.parameters(
      (1, {}),
      (arolla.array_float32([1, 2, 3]), {}),
      (L.x, {'x': arolla.array_float32([1, 2, 3])}),
      (
          L.x + 1.0 + L.y,
          {
              'x': arolla.array_float32([1, 2]),
              'y': arolla.array_float64([3, 4]),
          },
      ),
      (
          M.annotation.qtype(L.x, arolla.ARRAY_FLOAT32),
          {'x': arolla.array_float32([1, 2])},
      ),
  )
  def test_evaluation(self, expr, kwargs):
    arolla.testing.assert_qvalue_allequal(
        eval_util.eval_using_visitor(expr, **kwargs),
        arolla.eval(expr, **kwargs),
    )

  def test_missing_leaf_error(self):
    with self.assertRaisesWithLiteralMatch(KeyError, "'x'"):
      eval_util.eval_using_visitor(L.x)

  def test_placeholder_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'placeholders are not supported'
    ):
      eval_util.eval_using_visitor(P.x)

  def test_binding_error(self):
    try:
      arolla.eval(L._0 + L._1, _0=1, _1='2')
    except ValueError as e:
      eval_error_msg = str(e)
    expected_error_msg = ERROR_TEMPL.format(
        expr_str='L.x + L.y - 2',
        leaf_values_str="{'x': 1, 'y': '2'}",
        sub_expr_str='L.x + L.y',
        bound_node_str="1 + '2'",
    )
    self.assertErrorFormat(
        (L.x + L.y) - 2, {'x': 1, 'y': '2'}, expected_error_msg, eval_error_msg
    )

  def test_runtime_error(self):
    expected_error_msg = ERROR_TEMPL.format(
        expr_str='L.x // L.y - 2',
        leaf_values_str="{'x': 1, 'y': 0}",
        sub_expr_str='L.x // L.y',
        bound_node_str='1 // 0',
    )
    self.assertErrorFormat(
        (L.x // L.y) - 2, {'x': 1, 'y': 0}, expected_error_msg
    )

  def test_lambda_node_error(self):
    # Should include the lambda, not only the lowered expr.
    @arolla.optools.as_lambda_operator('test.foo_fn')
    def foo_fn(x, y):
      return x // y

    expected_error_msg = ERROR_TEMPL.format(
        expr_str='test.foo_fn(L.x, L.y) - 2',
        leaf_values_str="{'x': 1, 'y': 0}",
        sub_expr_str='test.foo_fn(L.x, L.y)',
        bound_node_str='test.foo_fn(1, 0)',
    )
    self.assertErrorFormat(
        foo_fn(L.x, L.y) - 2,
        {'x': 1, 'y': 0},
        expected_error_msg,
    )

  def test_leaf_values_pprint(self):

    @arolla.optools.as_py_function_operator('foo', qtype_inference_expr=P.x)
    def foo(x, y):
      raise ValueError('foo failure')

    x = arolla.types.PyObject(1234567, codec=arolla.types.PICKLE_CODEC)
    y = arolla.types.PyObject(2345678, codec=arolla.types.PICKLE_CODEC)
    # Without `pprint.pformat`, the leaves are printed as a one-liner.
    leaf_values_str = f"{{'x': {x},\n 'y': {y}}}"
    with self.assertRaisesRegex(
        ValueError, f'with leaf values:\n\n{re.escape(leaf_values_str)}'
    ):
      eval_util.eval_using_visitor(foo(L.x, L.y), x=x, y=y)


class EvalWithExceptionContextTest(parameterized.TestCase):

  @mock.patch.object(
      eval_util,
      'eval_with_expr_stack_trace',
      wraps=eval_util.eval_with_expr_stack_trace,
  )
  @mock.patch.object(eval_util, 'eval_using_visitor')
  def test_successful_evaluation(
      self, eval_using_visitor_mock, arolla_eval_mock
  ):
    expr = L.x + L.y
    res = eval_util.eval_with_exception_context(expr, x=1, y=2)
    arolla.testing.assert_qvalue_allequal(res, arolla.int32(3))
    # Assert that we take the fast path only if the evaluation is successful.
    arolla_eval_mock.assert_called_once_with(expr, x=1, y=2)
    eval_using_visitor_mock.assert_not_called()

  def test_exception_with_context(self):
    expr = L.x // L.y
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Specifically, the evaluation of the following sub-expression'
            ' failed:\n\n  L.x // L.y'
        ),
    ):
      eval_util.eval_with_exception_context(expr, x=1, y=0)

  def test_exception_with_context_and_expr_stack_trace(self):
    division_op = arolla.abc.make_lambda('x, y', P.x // P.y, name='division')
    expr = division_op(L.x, L.y)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            ERROR_TEMPL.format(
                expr_str='division(L.x, L.y)',
                leaf_values_str="{'x': 1, 'y': 0}",
                sub_expr_str='division(L.x, L.y)',
                bound_node_str='division(1, 0)',
            )
        ),
    ):
      eval_util.eval_with_exception_context(expr, x=1, y=0)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'ORIGINAL NODE: division(L.x, L.y)\n'
            'COMPILED NODE: M.math.floordiv(L.x, L.y)'
        ),
    ):
      try:
        eval_util.eval_with_exception_context(expr, x=1, y=0)
      except Exception as e:
        raise e.__cause__

  def test_timeout_with_context(self):
    expr = sleep_fn(L.x)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Specifically, the evaluation of the following sub-expression'
            ' failed:\n\n  test.sleep_fn(L.x)'
        ),
    ):
      eval_util.eval_with_exception_context(expr, x=1)

  @mock.patch.object(eval_util, '_ExprEvaluatorWithTimeout')
  def test_timeout_without_context_slow_arolla_eval(self, expr_evaluator_mock):
    # If `arolla.eval` is too slow, we skip calling the python visitor entirely
    # in order to avoid starting expensive jobs.
    expr = sleep_fn(L.x)
    try:
      eval_util.eval_with_expr_stack_trace(expr, x=1)
    except ValueError as e:
      raw_error = str(e)

    with self.assertRaisesWithLiteralMatch(ValueError, raw_error):
      # Set the timeout to less than the sleep.
      eval_util.eval_with_exception_context(expr, 0.25, x=1)
    expr_evaluator_mock.assert_not_called()

  @mock.patch.object(
      eval_util,
      '_ExprEvaluatorWithTimeout',
      wraps=eval_util._ExprEvaluatorWithTimeout,
  )
  def test_timeout_without_context_fast_arolla_eval(self, expr_evaluator_mock):
    # If `arolla.eval` is fast, the python visitor may still be slow (e.g. due
    # to `arolla.eval` caching). We test that the evaluation is interrupted
    # appropriately.

    @arolla.optools.as_py_function_operator('test.fn', qtype_inference_expr=P.x)
    def slow_fn(x):
      time.sleep(1)
      return x

    expr = slow_fn(L.x) + L.y
    try:
      # Eval once to cache the compilation where slow_fn(L.x) will be folded.
      eval_util.eval_with_expr_stack_trace(expr, x=1, y='2')
    except ValueError as e:
      raw_error = str(e)

    with self.assertRaisesWithLiteralMatch(ValueError, raw_error):
      # Set the timeout to less than the sleep.
      eval_util.eval_with_exception_context(expr, 0.25, x=1, y='2')
    expr_evaluator_mock.assert_called()

  @mock.patch.object(eval_util, 'eval_using_visitor', returns=arolla.int32(1))
  def test_internal_eval_error_is_raised(self, unused_eval_using_visitor_mock):
    # Tests that the error message raised in the call to arolla.eval is raised,
    # even when `eval_using_visitor` succeeds. This ensures that we raise errors
    # for internal issues as well.

    def fake_arolla_eval(*args, **kwargs):
      raise NotImplementedError('should raise')

    with mock.patch.object(
        eval_util, 'eval_with_expr_stack_trace', wraps=fake_arolla_eval
    ):
      with self.assertRaisesWithLiteralMatch(
          ValueError,
          'failed to recreate the exception with more context - please report'
          ' this to the arolla-team@. original'
          " error:\n\nNotImplementedError('should raise')",
      ):
        eval_util.eval_with_exception_context(L.x, x=1)


class EvalWithExprStackTraceTest(parameterized.TestCase):

  def test_exception(self):
    division_op = arolla.abc.make_lambda('x, y', P.x // P.y, name='division')
    expr = division_op(L.x, L.y)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'ORIGINAL NODE: division(L.x, L.y)\n'
            'COMPILED NODE: M.math.floordiv(L.x, L.y)'
        ),
    ):
      eval_util.eval_with_expr_stack_trace(expr, x=1, y=0)


if __name__ == '__main__':
  absltest.main()
