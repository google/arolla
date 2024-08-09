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

"""Tests for arolla.abc.CompiledExpr."""

import re
import signal
import threading
import time

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype
from arolla.abc import testing_clib as _  # provides the `test.fail` operator
from arolla.types import types
from arolla.while_loop import while_loop


make_tuple_op = abc_expr.make_lambda(
    '*args', abc_expr.placeholder('args'), name='make_tuple'
)


class CompiledExprTest(absltest.TestCase):

  def test_compile_expr_and_call(self):
    expr = abc_expr.bind_op(
        make_tuple_op, abc_expr.leaf('x'), abc_expr.leaf('y')
    )
    x = abc_qtype.NOTHING
    y = abc_qtype.unspecified()
    compiled_expr = clib.CompiledExpr(expr, dict(x=x.qtype, y=y.qtype))
    self.assertEqual(
        repr(compiled_expr(x, y)),
        '(NOTHING, unspecified)',
    )
    self.assertEqual(
        repr(compiled_expr(x, y=y)),
        '(NOTHING, unspecified)',
    )
    self.assertEqual(
        repr(compiled_expr(x=x, y=y)),
        '(NOTHING, unspecified)',
    )

  def test_compile_with_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__new__() missing required argument'
        " 'expr' (pos 1)",
    ):
      clib.CompiledExpr(input_qtypes={})  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__new__() missing required argument'
        " 'input_qtypes' (pos 2)",
    ):
      clib.CompiledExpr(expr=abc_expr.leaf('x'))  # pytype: disable=missing-parameter

  def test_compile_with_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__new__() expected an expression, got expr:'
        ' object',
    ):
      clib.CompiledExpr(object(), {})  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__new__() expected a dict[str, QType], got'
        ' input_qtypes: object',
    ):
      clib.CompiledExpr(abc_expr.leaf('x'), object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__new__() expected all input_qtypes.keys() to'
        ' be strings, got bytes',
    ):
      clib.CompiledExpr(abc_expr.leaf('x'), {b'x': abc_qtype.QTYPE})  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__new__() expected all input_qtypes.values()'
        ' to be QTypes, got object',
    ):
      clib.CompiledExpr(abc_expr.leaf('x'), dict(x=object()))  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a dict, got options: object'),
    ):
      clib.CompiledExpr(abc_expr.leaf('x'), {}, options=object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected all options.keys() to be strings, got int'),
    ):
      clib.CompiledExpr(abc_expr.leaf('x'), {}, options={0: True})  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        ValueError,
        'expected value of `enable_expr_stack_trace` in `options` to be boolean'
        ', got int',
    ):
      clib.CompiledExpr(
          abc_expr.leaf('x'), {}, options={'enable_expr_stack_trace': 0}
      )

  def test_compile_with_unexpected_options(self):
    with self.assertRaisesRegex(
        ValueError,
        'unexpected keyword argument `enable_2x_faster_execution` in '
        '`options` dict',
    ):
      clib.CompiledExpr(
          abc_expr.leaf('x'), {}, options={'enable_2x_faster_execution': True}
      )

  def test_compile_with_missing_input_qtypes(self):
    expr = abc_expr.bind_op(
        make_tuple_op,
        abc_expr.leaf('x'),
        abc_expr.leaf('y'),
        abc_expr.leaf('z'),
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'arolla.abc.CompiledExpr.__new__() missing input_qtypes for: L.x, L.z',
    ):
      clib.CompiledExpr(expr, dict(y=abc_qtype.NOTHING))

  def test_compile_with_placeholders(self):
    expr = abc_expr.bind_op(
        make_tuple_op,
        abc_expr.leaf('x'),
        abc_expr.placeholder('y'),
        abc_expr.placeholder('1'),
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'arolla.abc.CompiledExpr.__new__() expression contains placeholders:'
        " P.y, P['1']",
    ):
      clib.CompiledExpr(expr, dict(x=abc_qtype.NOTHING))

  def test_compile_with_error(self):
    expr = abc_expr.bind_op(
        'annotation.qtype', abc_expr.leaf('x'), abc_qtype.NOTHING
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('inconsistent qtype annotation')
    ):
      clib.CompiledExpr(expr, dict(x=abc_qtype.QTYPE))

  def test_call_with_too_many_positional_arguments(self):
    expr = abc_expr.bind_op(make_tuple_op, abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(expr, dict(x=abc_qtype.NOTHING))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() takes 2 positional'
        ' arguments but 3 were given',
    ):
      compiled_expr(abc_qtype.QTYPE, abc_qtype.QTYPE)

  def test_call_with_wrong_arg_types(self):
    expr = abc_expr.bind_op(make_tuple_op, abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(expr, dict(x=abc_qtype.NOTHING))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() expected a qvalue, got x: object',
    ):
      compiled_expr(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() expected NOTHING, got x: QTYPE',
    ):
      compiled_expr(abc_qtype.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() expected a qvalue, got x: object',
    ):
      compiled_expr(x=object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() expected NOTHING, got x: QTYPE',
    ):
      compiled_expr(x=abc_qtype.QTYPE)

  def test_call_with_unexpected_keyword_argument(self):
    expr = abc_expr.bind_op(make_tuple_op, abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(expr, dict(x=abc_qtype.QTYPE))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() got an unexpected keyword'
        " argument 'y'",
    ):
      compiled_expr(y=abc_qtype.QTYPE)

  def test_eval_with_multiple_values_for_argument(self):
    expr = abc_expr.bind_op(
        make_tuple_op, abc_expr.leaf('x'), abc_expr.leaf('y')
    )
    compiled_expr = clib.CompiledExpr(
        expr, dict(x=abc_qtype.QTYPE, y=abc_qtype.QTYPE)
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() got multiple values for'
        " argument 'x'",
    ):
      compiled_expr(abc_qtype.QTYPE, abc_qtype.QTYPE, x=abc_qtype.QTYPE)

  def test_eval_with_missing_arguments(self):
    expr = abc_expr.bind_op(
        make_tuple_op,
        abc_expr.leaf('x'),
        abc_expr.leaf('y'),
        abc_expr.leaf('z'),
    )
    compiled_expr = clib.CompiledExpr(
        expr, dict(x=abc_qtype.QTYPE, y=abc_qtype.QTYPE, z=abc_qtype.NOTHING)
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.__call__() missing required arguments: '
        'x: QTYPE, z: NOTHING',
    ):
      compiled_expr(y=abc_qtype.QTYPE)

  def test_execute(self):
    expr = abc_expr.bind_op(
        make_tuple_op, abc_expr.leaf('x'), abc_expr.leaf('y')
    )
    x = abc_qtype.NOTHING
    y = abc_qtype.unspecified()
    compiled_expr = clib.CompiledExpr(expr, dict(x=x.qtype, y=y.qtype))
    self.assertEqual(
        repr(compiled_expr.execute(dict(x=x, y=y))),
        '(NOTHING, unspecified)',
    )

  def test_execute_with_wrong_arguments(self):
    expr = abc_expr.bind_op(make_tuple_op, abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(expr, dict(x=abc_qtype.NOTHING))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.execute() expected a dict[str, QValue], got'
        ' input_qvalues: object',
    ):
      compiled_expr.execute(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.execute() expected all input_qvalues.keys()'
        ' to be strings, got bytes',
    ):
      compiled_expr.execute({b'x': abc_qtype.QTYPE})  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.execute() expected all input_qvalues.values()'
        ' to be QValues, got object',
    ):
      compiled_expr.execute(dict(x=object()))  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.execute() expected NOTHING, got'
        " input_qvalues['x']: QTYPE",
    ):
      compiled_expr.execute(dict(x=abc_qtype.QTYPE))

  def test_execute_with_unexpected_input(self):
    expr = abc_expr.bind_op(make_tuple_op, abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(expr, dict(x=abc_qtype.QTYPE))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.CompiledExpr.execute() got an unexpected input 'y'",
    ):
      compiled_expr.execute(dict(y=abc_qtype.QTYPE))

  def test_execute_with_missing_arguments(self):
    expr = abc_expr.bind_op(
        make_tuple_op,
        abc_expr.leaf('x'),
        abc_expr.leaf('y'),
        abc_expr.leaf('z'),
    )
    compiled_expr = clib.CompiledExpr(
        expr, dict(x=abc_qtype.QTYPE, y=abc_qtype.QTYPE, z=abc_qtype.NOTHING)
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.CompiledExpr.execute() missing required input: '
        'x: QTYPE, z: NOTHING',
    ):
      compiled_expr.execute(dict(y=abc_qtype.QTYPE))

  def test_eval_on_heap(self):
    inputs = {f'_{i}': abc_qtype.QTYPE for i in range(10000)}
    expr = abc_expr.bind_op(make_tuple_op, *map(abc_expr.leaf, inputs.keys()))
    compiled_expr = clib.CompiledExpr(expr, inputs)
    self.assertEqual(
        repr(compiled_expr(**inputs)),
        '(' + ', '.join('QTYPE' for _ in inputs.keys()) + ')',
    )

  def test_eval_non_identifiers(self):
    inputs = {f'{i}': abc_qtype.QTYPE for i in range(10)}
    expr = abc_expr.bind_op(make_tuple_op, *map(abc_expr.leaf, inputs.keys()))
    compiled_expr = clib.CompiledExpr(expr, inputs)
    self.assertEqual(
        repr(compiled_expr.execute(inputs)),
        '(' + ', '.join('QTYPE' for _ in inputs.keys()) + ')',
    )

  def test_eval_with_stack_trace(self):
    fail_lambda = abc_expr.make_lambda(
        'x', abc_expr.bind_op('test.fail', abc_expr.placeholder('x'))
    )

    expr = abc_expr.bind_op(fail_lambda, abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(
        expr,
        {'x': abc_qtype.UNSPECIFIED},
        options={'enable_expr_stack_trace': True},
    )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'intentional failure at `test.fail`; '
            'during evaluation of operator test.fail\n'
            'ORIGINAL NODE: anonymous.lambda(L.x)\n'
            'COMPILED NODE: M.test.fail(L.x)'
        ),
    ):
      compiled_expr.execute({'x': abc_qtype.unspecified()})

  def test_eval_test_fail(self):
    expr = abc_expr.bind_op('test.fail', abc_expr.leaf('x'))
    compiled_expr = clib.CompiledExpr(expr, dict(x=abc_qtype.UNSPECIFIED))
    with self.assertRaisesRegex(
        ValueError, re.escape('intentional failure at `test.fail`')
    ):
      compiled_expr(abc_qtype.unspecified())

  def test_interrupt(self):
    compiled_expr = clib.CompiledExpr(
        while_loop.while_loop(
            initial_state=dict(i=0, s=0),
            condition=abc_expr.placeholder('i') < abc_expr.leaf('n'),
            body=dict(
                i=abc_expr.placeholder('i') + 1,
                s=abc_expr.placeholder('s') + abc_expr.placeholder('i'),
            ),
        ),
        dict(n=types.INT32),
    )

    def do_keyboard_interrupt():
      time.sleep(0.5)
      signal.raise_signal(signal.SIGINT)

    threading.Thread(target=do_keyboard_interrupt).start()

    with self.assertRaisesRegex(ValueError, re.escape('interrupt')):
      compiled_expr.execute({'n': types.int32(10**8)})


if __name__ == '__main__':
  absltest.main()
