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

"""Test for arolla.abc.[unsafe_]make_operator_node()."""

import inspect
import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype


op_id = abc_expr.make_lambda(
    'x', abc_expr.placeholder('x'), name='make_op_node_test.id'
)

op_tuple = abc_expr.make_lambda(
    '*args', abc_expr.placeholder('args'), name='make_op_node_test.tuple'
)
abc_expr.register_operator('make_op_node_test.tuple', op_tuple)


class MakeOperatorNodeTest(absltest.TestCase):

  def test_op(self):
    self.assertEqual(
        repr(abc_expr.make_operator_node(op_tuple)),
        'make_op_node_test.tuple()',
    )
    self.assertEqual(
        repr(
            abc_expr.make_operator_node(
                op_tuple,
                (abc_qtype.NOTHING, abc_expr.literal(abc_qtype.unspecified())),
            )
        ),
        'make_op_node_test.tuple(NOTHING, unspecified)',
    )

  def test_op_name(self):
    self.assertEqual(
        repr(abc_expr.make_operator_node('make_op_node_test.tuple')),
        'M.make_op_node_test.tuple()',
    )
    self.assertEqual(
        repr(
            abc_expr.make_operator_node(
                'make_op_node_test.tuple',
                (abc_qtype.NOTHING, abc_expr.literal(abc_qtype.unspecified())),
            )
        ),
        'M.make_op_node_test.tuple(NOTHING, unspecified)',
    )

  def test_attr(self):
    self.assertEqual(
        abc_expr.make_operator_node(op_id, (abc_qtype.Unspecified(),)).qvalue,
        abc_qtype.Unspecified(),
    )
    self.assertIsNone(
        abc_expr.make_operator_node(op_id, (abc_expr.placeholder('x'),)).qtype
    )

  def test_error_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.make_operator_node() missing 1 required positional'
        " argument: 'op'",
    ):
      abc_expr.make_operator_node()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.make_operator_node() takes 2 positional arguments but 3'
        ' were given',
    ):
      abc_expr.make_operator_node(1, 2, 3)  # pytype: disable=wrong-arg-count

  def test_error_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.make_operator_node() expected Operator|str, got op:'
        ' arolla.abc.qtype.QType',
    ):
      abc_expr.make_operator_node(abc_qtype.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.make_operator_node() expected Operator|str, got op: object',
    ):
      abc_expr.make_operator_node(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.make_operator_node() expected a tuple[Expr|QValue, ...],'
        ' got inputs: object',
    ):
      abc_expr.make_operator_node(op_tuple, object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.make_operator_node() expected Expr|QValue, got inputs[1]:'
        ' object',
    ):
      abc_expr.make_operator_node(op_tuple, (abc_qtype.NOTHING, object()))  # pytype: disable=wrong-arg-types

  def test_error_no_such_operator(self):
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        'arolla.abc.make_operator_node() operator not found:'
        " 'no.such.operator'",
    ):
      abc_expr.make_operator_node('no.such.operator')

  def test_error_wrong_inputs_count(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'incorrect number of dependencies passed to an operator node'
        ),
    ):
      abc_expr.make_operator_node(op_id, (abc_qtype.QTYPE, abc_qtype.QTYPE))

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.make_operator_node),
        inspect.signature(lambda op, inputs=(), /: None),
    )


class UnsafeMakeOperatorNodeTest(absltest.TestCase):

  def test_op(self):
    self.assertEqual(
        repr(abc_expr.unsafe_make_operator_node(op_tuple)),
        'make_op_node_test.tuple()',
    )
    self.assertEqual(
        repr(
            abc_expr.unsafe_make_operator_node(
                op_tuple,
                (abc_qtype.NOTHING, abc_expr.literal(abc_qtype.unspecified())),
            )
        ),
        'make_op_node_test.tuple(NOTHING, unspecified)',
    )

  def test_op_name(self):
    self.assertEqual(
        repr(abc_expr.unsafe_make_operator_node('make_op_node_test.tuple')),
        'M.make_op_node_test.tuple()',
    )
    self.assertEqual(
        repr(
            abc_expr.unsafe_make_operator_node(
                'make_op_node_test.tuple',
                (abc_qtype.NOTHING, abc_expr.literal(abc_qtype.unspecified())),
            )
        ),
        'M.make_op_node_test.tuple(NOTHING, unspecified)',
    )

  def test_no_attr(self):
    self.assertIsNone(
        abc_expr.unsafe_make_operator_node(
            op_id, (abc_qtype.Unspecified(),)
        ).qtype
    )
    self.assertIsNone(
        abc_expr.unsafe_make_operator_node(
            op_id, (abc_expr.placeholder('x'),)
        ).qtype
    )

  def test_error_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() missing 1 required positional'
        " argument: 'op'",
    ):
      abc_expr.unsafe_make_operator_node()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() takes 2 positional arguments'
        ' but 3 were given',
    ):
      abc_expr.unsafe_make_operator_node(1, 2, 3)  # pytype: disable=wrong-arg-count

  def test_error_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() expected Operator|str, got op:'
        ' arolla.abc.qtype.QType',
    ):
      abc_expr.unsafe_make_operator_node(abc_qtype.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() expected Operator|str, got op:'
        ' object',
    ):
      abc_expr.unsafe_make_operator_node(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() expected a tuple[Expr|QValue,'
        ' ...], got inputs: object',
    ):
      abc_expr.unsafe_make_operator_node(op_tuple, object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() expected Expr|QValue, got'
        ' inputs[1]: object',
    ):
      abc_expr.unsafe_make_operator_node(
          op_tuple, (abc_qtype.NOTHING, object())
      )  # pytype: disable=wrong-arg-types

  def test_no_such_operator_operator(self):
    self.assertEqual(
        repr(abc_expr.unsafe_make_operator_node('no.such.operator')),
        'M.no.such.operator()',
    )

  def test_wrong_inputs_count(self):
    self.assertEqual(
        repr(
            abc_expr.unsafe_make_operator_node(
                op_id, (abc_qtype.QTYPE, abc_qtype.QTYPE)
            )
        ),
        'make_op_node_test.id(QTYPE, QTYPE)',
    )

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.unsafe_make_operator_node),
        inspect.signature(lambda op, inputs=(), /: None),
    )


class UnsafeParseSExprTest(parameterized.TestCase):

  @parameterized.parameters(
      (abc_expr.leaf('leaf'), 'L.leaf'),
      (abc_qtype.QTYPE, 'QTYPE'),
      (
          ('op', abc_expr.leaf('leaf'), abc_qtype.Unspecified()),
          'M.op(L.leaf, unspecified)',
      ),
      (
          (
              'op1',
              (
                  'op2',
                  abc_expr.leaf('x'),
                  abc_qtype.Unspecified(),
              ),
          ),
          'M.op1(M.op2(L.x, unspecified))',
      ),
  )
  def test_basics(self, sexpr, expected_repr):
    self.assertEqual(repr(abc_expr.unsafe_parse_sexpr(sexpr)), expected_repr)

  def test_error_wrong_arg_types(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected Expr, QValue, or tuple, got sexpr: object',
    ):
      _ = abc_expr.unsafe_parse_sexpr(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() expected Operator|str, got'
        ' op: object',
    ):
      _ = abc_expr.unsafe_parse_sexpr((object(),))  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.unsafe_make_operator_node() expected Operator|str, got'
        ' op: arolla.abc.qtype.QType',
    ):
      _ = abc_expr.unsafe_parse_sexpr((abc_qtype.QTYPE,))  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
