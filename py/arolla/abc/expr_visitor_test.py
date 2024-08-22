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

"""Tests for expr_visitor."""

import re
from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import expr_visitor as abc_expr_visitor


class ExprVisitorTest(absltest.TestCase):

  def assertExprListEqual(self, actual, expected):
    self.assertListEqual(
        [n.fingerprint for n in actual], [n.fingerprint for n in expected]
    )

  def testTransform(self):
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity1')
    op2 = abc_expr.make_lambda('y', abc_expr.placeholder('y'), name='identity2')
    op3 = abc_expr.make_lambda('z', abc_expr.placeholder('z'), name='identity3')

    def convert_to_next_op(node):
      if node.op is not None:
        if node.op.fingerprint == op1.fingerprint:
          return abc_expr.bind_op(op2, *node.node_deps)
        elif node.op.fingerprint == op2.fingerprint:
          return abc_expr.bind_op(op3, *node.node_deps)
      return node

    value = dummy_types.make_dummy_value()
    expr = abc_expr.bind_op(  # op1(op2(op3(value)))
        op1, abc_expr.bind_op(op2, abc_expr.bind_op(op3, value))
    )
    actual_expr = abc_expr_visitor.transform(expr, convert_to_next_op)
    expected_expr = abc_expr.bind_op(  # op2(op3(op3(value)))
        op2, abc_expr.bind_op(op3, abc_expr.bind_op(op3, value))
    )
    self.assertEqual(actual_expr.fingerprint, expected_expr.fingerprint)

  def testTransformError(self):
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity1')

    def raise_error(node):
      if node.op is not None and node.op.fingerprint == op1.fingerprint:
        raise RuntimeError('op1 is not supported')
      else:
        return node

    value = dummy_types.make_dummy_value()
    expr = abc_expr.bind_op(op1, value)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'op1 is not supported'
    ):
      abc_expr_visitor.transform(expr, raise_error)

  def testTransformTypeErrors(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape("arolla.abc.transform() missing required argument 'expr'"),
    ):
      abc_expr_visitor.transform(  # pytype: disable=wrong-keyword-args
          unknown_arg=object(),
      )
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            "arolla.abc.transform() missing required argument 'transform_fn'"
        ),
    ):
      abc_expr_visitor.transform(  # pytype: disable=wrong-keyword-args
          abc_expr.placeholder('x'), unknown_arg=object()
      )
    with self.assertRaisesRegex(
        TypeError,
        re.escape('arolla.abc.transform() takes at most 2 arguments'),
    ):
      abc_expr_visitor.transform(  # pytype: disable=wrong-keyword-args
          abc_expr.placeholder('x'), lambda x: x, unknown_arg=object()
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.transform() expected an expression, got expr: object',
    ):
      abc_expr_visitor.transform(object(), lambda x: x)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.transform() expected Callable[[Expr], Expr], got'
        ' transform_fn: object',
    ):
      abc_expr_visitor.transform(abc_expr.placeholder('x'), object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'transform_fn() unexpected return type: object'
    ):
      abc_expr_visitor.transform(abc_expr.placeholder('x'), lambda _: object())

  def testDeepTransform(self):
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity1')
    op2 = abc_expr.make_lambda('y', abc_expr.placeholder('y'), name='identity2')
    op3 = abc_expr.make_lambda('z', abc_expr.placeholder('z'), name='identity3')

    def convert_to_next_op(node):
      if node.op is not None:
        if node.op.fingerprint == op1.fingerprint:
          return abc_expr.bind_op(op2, *node.node_deps)
        elif node.op.fingerprint == op2.fingerprint:
          return abc_expr.bind_op(op3, *node.node_deps)
      return node

    value = dummy_types.make_dummy_value()
    expr = abc_expr.bind_op(  # op1(op2(op3(value)))
        op1, abc_expr.bind_op(op2, abc_expr.bind_op(op3, value))
    )
    actual_expr = abc_expr_visitor.deep_transform(expr, convert_to_next_op)
    expected_expr = abc_expr.bind_op(  # op3(op3(op3(value)))
        op3, abc_expr.bind_op(op3, abc_expr.bind_op(op3, value))
    )
    self.assertEqual(
        actual_expr.fingerprint,
        expected_expr.fingerprint,
        f'{actual_expr} != {expected_expr}',
    )

  def testDeepTransformError(self):
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity1')

    def raise_error(node):
      if node.op is not None and node.op.fingerprint == op1.fingerprint:
        raise RuntimeError('op1 is not supported')
      return node

    value = dummy_types.make_dummy_value()
    expr = abc_expr.bind_op(op1, value)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'op1 is not supported'
    ):
      abc_expr_visitor.deep_transform(expr, raise_error)

  def testDeepTransformTypeErrors(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            "arolla.abc.deep_transform() missing required argument 'expr'"
        ),
    ):
      abc_expr_visitor.deep_transform(  # pytype: disable=wrong-keyword-args
          unknown_arg=object(),
      )
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'arolla.abc.deep_transform() missing required argument'
            " 'transform_fn'"
        ),
    ):
      abc_expr_visitor.deep_transform(  # pytype: disable=wrong-keyword-args
          abc_expr.placeholder('x'), unknown_arg=object()
      )
    with self.assertRaisesRegex(
        TypeError,
        re.escape('arolla.abc.deep_transform() takes at most 2 arguments'),
    ):
      abc_expr_visitor.deep_transform(  # pytype: disable=wrong-keyword-args
          abc_expr.placeholder('x'), lambda x: x, unknown_arg=object()
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.deep_transform() expected an expression, got expr: object',
    ):
      abc_expr_visitor.deep_transform(object(), lambda x: x)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.deep_transform() expected Callable[[Expr], Expr], got'
        ' transform_fn: object',
    ):
      abc_expr_visitor.deep_transform(abc_expr.placeholder('x'), object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'transform_fn() unexpected return type: object'
    ):
      abc_expr_visitor.deep_transform(
          abc_expr.placeholder('x'), lambda _: object()
      )

  def testPostOrder(self):
    op = abc_expr.make_lambda(
        'x,_y', abc_expr.placeholder('x'), name='identity'
    )
    x = abc_expr.placeholder('placeholder_key')
    y = abc_expr.literal(dummy_types.make_dummy_value())
    expr = abc_expr.bind_op(op, x, y)
    visitor_order = list(abc_expr_visitor.post_order(expr))
    self.assertExprListEqual(visitor_order, [x, y, expr])

  def testPreAndPostOrder(self):
    op = abc_expr.make_lambda(
        'x,_y', abc_expr.placeholder('x'), name='identity'
    )
    x = abc_expr.placeholder('placeholder_key')
    y = abc_expr.literal(dummy_types.make_dummy_value())
    expr = abc_expr.bind_op(op, x, y)
    visitor_order = list(clib.internal_pre_and_post_order(expr))
    self.assertLen(visitor_order, 3 * 2)
    to_fp = lambda visit: (visit[0], visit[1].fingerprint)
    self.assertEqual((True, expr.fingerprint), to_fp(visitor_order[0]))
    self.assertEqual((False, expr.fingerprint), to_fp(visitor_order[-1]))
    self.assertEqual((True, x.fingerprint), to_fp(visitor_order[1]))
    self.assertEqual((False, x.fingerprint), to_fp(visitor_order[2]))
    self.assertEqual((True, y.fingerprint), to_fp(visitor_order[3]))
    self.assertEqual((False, y.fingerprint), to_fp(visitor_order[4]))

  def testPostOrderTraverse(self):
    op = abc_expr.make_lambda(
        'x,_y', abc_expr.placeholder('x'), name='identity'
    )
    x = abc_expr.placeholder('placeholder_key')
    y = abc_expr.literal(dummy_types.make_dummy_value())
    expr = abc_expr.bind_op(op, x, y)

    def count_visitor(node, dep_results: list[int]) -> int:
      if node.fingerprint == x.fingerprint:
        self.assertEmpty(dep_results)
        return 0
      if node.fingerprint == y.fingerprint:
        self.assertEmpty(dep_results)
        return 1
      if node.fingerprint == expr.fingerprint:
        self.assertListEqual(dep_results, [0, 1])
        return 2
      self.fail('Unexpected node %s' % node)

    self.assertEqual(
        abc_expr_visitor.post_order_traverse(expr, count_visitor), 2
    )

    def transform_visitor(node, dep_results):
      if node.fingerprint == x.fingerprint:
        self.assertEmpty(dep_results)
        return y
      if node.fingerprint == y.fingerprint:
        self.assertEmpty(dep_results)
        return x
      if node.fingerprint == expr.fingerprint:
        self.assertExprListEqual(dep_results, [y, x])
        return abc_expr.bind_op(op, *dep_results)
      self.fail('Unexpected node %s' % node)

    self.assertEqual(
        abc_expr_visitor.post_order_traverse(
            expr, transform_visitor
        ).fingerprint,
        abc_expr.bind_op(op, y, x).fingerprint,
    )

  def testPreAndPostOrderTraverse(self):
    op = abc_expr.make_lambda(
        'x,_y', abc_expr.placeholder('x'), name='identity'
    )
    x = abc_expr.placeholder('placeholder_key')
    y = abc_expr.literal(dummy_types.make_dummy_value())
    xy = abc_expr.bind_op(op, x, y)
    expr = abc_expr.bind_op(op, xy, y)

    visits = []

    def count_visitor(node, dep_results: list[int]) -> int:
      visits.append(node)
      if node.fingerprint == x.fingerprint:
        self.assertEmpty(dep_results)
        return 0
      if node.fingerprint == y.fingerprint:
        self.assertEmpty(dep_results)
        return 1
      if node.fingerprint == xy.fingerprint:
        self.assertListEqual(dep_results, [0, 1])
        return 2
      if node.fingerprint == expr.fingerprint:
        self.assertListEqual(dep_results, [2, 1])
        return 3
      self.fail('Unexpected node %s' % node)

    # no previsit
    visits = []
    self.assertEqual(
        abc_expr_visitor.pre_and_post_order_traverse(
            expr, lambda x: None, count_visitor
        ),
        3,
    )
    self.assertExprListEqual(visits, [x, y, xy, expr])

    # return -1 on root
    visits = []
    self.assertEqual(
        abc_expr_visitor.pre_and_post_order_traverse(
            expr, lambda x: -1, count_visitor
        ),
        -1,
    )
    self.assertExprListEqual(visits, [])

    # return special object to signal missed result (from docstring example)
    my_none = object()

    def return_none_previsit(x):
      del x  # unused
      return my_none

    visits = []
    self.assertIs(
        abc_expr_visitor.pre_and_post_order_traverse(
            expr, return_none_previsit, count_visitor
        ),
        my_none,
    )
    self.assertExprListEqual(visits, [])

    previsits = []

    def skip_xy_previsit(node) -> int | None:
      previsits.append(node)
      if node.fingerprint == xy.fingerprint:
        return 2

    visits = []
    self.assertEqual(
        abc_expr_visitor.pre_and_post_order_traverse(
            expr, skip_xy_previsit, count_visitor
        ),
        3,
    )
    self.assertExprListEqual(visits, [y, expr])
    self.assertExprListEqual(previsits, [expr, xy, y])


if __name__ == '__main__':
  absltest.main()
