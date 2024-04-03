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

"""Tests for arolla.abc.eval_expr."""

import gc
import inspect
import re

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype


make_tuple_op = abc_expr.make_lambda(
    '*args', abc_expr.placeholder('args'), name='make_tuple'
)


class EvalExprTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    clib.clear_eval_compile_cache()

  def test_eval_expr(self):
    expr = abc_expr.bind_op(
        make_tuple_op, abc_expr.leaf('x'), abc_expr.leaf('y')
    )
    x = abc_qtype.NOTHING
    y = abc_qtype.unspecified()
    self.assertEqual(
        repr(clib.eval_expr(expr, dict(x=x, y=y))),
        '(NOTHING, unspecified)',
    )

  def test_eval_expr_with_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.eval_expr() missing 1 required positional argument:'
        " 'expr'",
    ):
      clib.eval_expr()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.eval_expr() takes 2 positional arguments but 3 were given',
    ):
      clib.eval_expr(1, 2, 3)  # pytype: disable=wrong-arg-count

  def test_eval_expr_with_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.eval_expr() expected an expression, got expr: object',
    ):
      clib.eval_expr(object())  # pytype: disable=wrong-arg-types

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.eval_expr() expected a dict[str, QValue], got'
        ' input_qvalues: object',
    ):
      clib.eval_expr(abc_expr.leaf('x'), object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.eval_expr() expected all input_qvalues.keys() to be'
        ' strings, got bytes',
    ):
      clib.eval_expr(abc_expr.leaf('x'), {b'x': abc_qtype.QTYPE})  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.eval_expr() expected all input_qvalues.values() to be'
        ' QValues, got object',
    ):
      clib.eval_expr(abc_expr.leaf('x'), dict(x=object()))  # pytype: disable=wrong-arg-types

  def test_eval_expr_with_redundant_input(self):
    expr = abc_expr.bind_op(
        make_tuple_op, abc_expr.leaf('x'), abc_expr.leaf('y')
    )
    x = abc_qtype.NOTHING
    y = abc_qtype.unspecified()
    self.assertEqual(
        repr(clib.eval_expr(expr, dict(x=x, y=y, z=make_tuple_op))),
        '(NOTHING, unspecified)',
    )

  def test_eval_expr_with_missing_input_values(self):
    expr = abc_expr.bind_op(
        make_tuple_op,
        abc_expr.leaf('x'),
        abc_expr.leaf('y'),
        abc_expr.leaf('z'),
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'arolla.abc.eval_expr() missing values for: L.x, L.z',
    ):
      clib.eval_expr(expr, dict(y=abc_qtype.NOTHING))

  def test_eval_expr_with_placeholders(self):
    expr = abc_expr.bind_op(
        make_tuple_op,
        abc_expr.leaf('x'),
        abc_expr.placeholder('y'),
        abc_expr.placeholder('1'),
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "arolla.abc.eval_expr() expression contains placeholders: P['1'], P.y",
    ):
      clib.eval_expr(expr, dict(x=abc_qtype.NOTHING))

  def test_eval_expr_with_error(self):
    expr = abc_expr.bind_op(
        'annotation.qtype', abc_expr.leaf('x'), abc_qtype.NOTHING
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('inconsistent qtype annotation')
    ):
      clib.eval_expr(expr, dict(x=abc_qtype.QTYPE))

  def test_eval_expr_on_heap(self):
    inputs = {f'_{i}': abc_qtype.QTYPE for i in range(10000)}
    expr = abc_expr.bind_op(make_tuple_op, *map(abc_expr.leaf, inputs.keys()))
    self.assertEqual(
        repr(clib.eval_expr(expr, inputs)),
        '(' + ', '.join('QTYPE' for _ in inputs.keys()) + ')',
    )

  def test_eval_expr_caching(self):
    gc.collect()
    cnt = dummy_types.count_dummy_value_instances()
    clib.eval_expr(abc_expr.literal(dummy_types.make_dummy_value()))
    gc.collect()
    self.assertGreater(dummy_types.count_dummy_value_instances(), cnt)
    clib.clear_eval_compile_cache()
    self.assertEqual(dummy_types.count_dummy_value_instances(), cnt)

  def test_eval_expr_caching_performance(self):
    expr = abc_expr.leaf('x')
    for _ in range(10000):
      expr = abc_expr.bind_op('annotation.qtype', expr, abc_qtype.QTYPE)
    for _ in range(10000):  # Takes > 60s if the caching is disabled.
      _ = clib.eval_expr(expr, dict(x=abc_qtype.QTYPE))
      _ = clib.eval_expr(expr, dict(x=abc_qtype.UNSPECIFIED))
      _ = clib.eval_expr(expr, dict(x=abc_qtype.NOTHING))
      _ = clib.eval_expr(expr, dict(x=abc_expr.EXPR_QUOTE))

  def test_eval_expr_signature(self):
    self.assertEqual(
        inspect.signature(clib.eval_expr),
        inspect.signature(lambda expr, input_qvalues={}, /: None),
    )


if __name__ == '__main__':
  absltest.main()
