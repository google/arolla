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

"""Tests for arolla.abc.expr."""

import collections
import inspect
import re
from unittest import mock
import warnings
import weakref

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype
from arolla.abc import testing_clib
from arolla.abc import utils as abc_utils

lit_nothing_qtype = abc_expr.literal(abc_qtype.NOTHING)

l_x = abc_expr.leaf('x')
l_y = abc_expr.leaf('y')
p_x = abc_expr.placeholder('x')

op_id = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='id')

op_lit = abc_expr.bind_op(
    abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='id'),
    abc_qtype.NOTHING,
)


class ExprTest(absltest.TestCase):

  def test_is_final_class(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "type 'arolla.abc.Expr' is not an acceptable base type"
    ):

      class SubExpr(abc_expr.Expr):
        pass

      del SubExpr

  def test_weakref(self):
    p_y = abc_expr.placeholder('y')
    weak_p_y = weakref.ref(p_y)
    self.assertIs(weak_p_y(), p_y)
    del p_y
    self.assertIsNone(weak_p_y())

  def test_repr(self):
    self.assertEqual(repr(l_x), 'L.x')
    self.assertEqual(repr(op_lit), 'id(NOTHING)')

  def test_format(self):
    self.assertEqual(f'{l_x}', 'L.x')
    self.assertEqual(f'{op_lit}', 'id(NOTHING)')
    self.assertEqual(f'{l_x:verbose}', 'L.x')
    self.assertEqual(f'{op_lit:verbose}', 'id(NOTHING):QTYPE')
    with self.assertRaisesWithLiteralMatch(
        ValueError, "expected format_spec='' or 'verbose', got format_spec='v'"
    ):
      _ = f'{l_x:v}'

  def test_equals(self):
    self.assertTrue(l_x.equals(l_x))
    self.assertFalse(l_x.equals(l_y))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "expected 'arolla.abc.Expr', got 'int'"
    ):
      l_x.equals(1)  # pytype: disable=wrong-arg-types

  def test_non_bool_castable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "__bool__ disabled for 'arolla.abc.Expr'"
    ):
      bool(l_x)

  def test_unhashable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "unhashable type: 'arolla.abc.Expr'; please consider using"
        ' `arolla.quote(expr)`',
    ):
      hash(l_x)

  def test_fingerprint(self):
    self.assertEqual(l_x.fingerprint, l_x.fingerprint)
    self.assertNotEqual(l_x.fingerprint, l_y.fingerprint)

  def test_is_literal(self):
    self.assertTrue(lit_nothing_qtype.is_literal)
    self.assertFalse(l_x.is_literal)
    self.assertFalse(p_x.is_literal)
    self.assertFalse(op_lit.is_literal)

  def test_is_leaf(self):
    self.assertFalse(lit_nothing_qtype.is_leaf)
    self.assertTrue(l_x.is_leaf)
    self.assertFalse(p_x.is_leaf)
    self.assertFalse(op_lit.is_leaf)

  def test_is_placeholder(self):
    self.assertFalse(lit_nothing_qtype.is_placeholder)
    self.assertFalse(l_x.is_placeholder)
    self.assertTrue(p_x.is_placeholder)
    self.assertFalse(op_lit.is_placeholder)

  def test_is_operator(self):
    self.assertFalse(lit_nothing_qtype.is_operator)
    self.assertFalse(l_x.is_operator)
    self.assertFalse(p_x.is_operator)
    self.assertTrue(op_lit.is_operator)

  def test_qtype(self):
    self.assertEqual(lit_nothing_qtype.qtype, abc_qtype.QTYPE)
    self.assertIsNone(l_x.qtype)

  def test_qvalue(self):
    self.assertEqual(
        lit_nothing_qtype.qvalue.fingerprint, abc_qtype.NOTHING.fingerprint
    )
    self.assertIsNone(l_x.qvalue)

  def test_leaf_key(self):
    self.assertEqual(l_x.leaf_key, 'x')
    self.assertEqual(p_x.leaf_key, '')

  def test_placeholder_key(self):
    self.assertEqual(l_x.placeholder_key, '')
    self.assertEqual(p_x.placeholder_key, 'x')

  def test_op(self):
    self.assertEqual(op_lit.op.fingerprint, op_id.fingerprint)
    self.assertIsNone(l_x.op)

  def test_node_deps(self):
    self.assertEqual(l_x.node_deps, ())
    self.assertIsInstance(op_lit.node_deps, tuple)
    self.assertLen(op_lit.node_deps, 1)
    self.assertEqual(
        op_lit.node_deps[0].fingerprint, lit_nothing_qtype.fingerprint
    )

  def test_iter(self):
    self.assertNotIsInstance(l_x, collections.abc.Iterable)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'arolla.abc.Expr' object is not iterable"
    ):
      for _ in l_x:
        self.fail()

  def test_dir(self):
    self.assertLessEqual({'fingerprint', 'equals', 'op'}, set(dir(l_x)))


class LiteralTest(absltest.TestCase):

  def test_basic(self):
    expr = abc_expr.literal(abc_qtype.Unspecified())
    self.assertIsInstance(expr, abc_expr.Expr)
    self.assertTrue(expr.is_literal)
    self.assertEqual(expr.qvalue, abc_qtype.Unspecified())

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected QValue, got object'
    ):
      abc_expr.literal(object())  # pytype: disable=wrong-arg-types

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.literal),
        inspect.signature(lambda value, /: None),
    )


class LeafTest(absltest.TestCase):

  def test_basic(self):
    expr = abc_expr.leaf('key')
    self.assertIsInstance(expr, abc_expr.Expr)
    self.assertTrue(expr.is_leaf)
    self.assertEqual(expr.leaf_key, 'key')

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a leaf key, got object'
    ):
      abc_expr.leaf(object())  # pytype: disable=wrong-arg-types

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.leaf),
        inspect.signature(lambda leaf_key, /: None),
    )


class PlaceholderTest(absltest.TestCase):

  def test_basic(self):
    expr = abc_expr.placeholder('key')
    self.assertIsInstance(expr, abc_expr.Expr)
    self.assertTrue(expr.is_placeholder)
    self.assertEqual(expr.placeholder_key, 'key')

  def test_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a placeholder key, got object'
    ):
      abc_expr.placeholder(object())  # pytype: disable=wrong-arg-types

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.placeholder),
        inspect.signature(lambda placeholder_key, /: None),
    )


class BindOpTest(absltest.TestCase):

  def test_args(self):
    with self.subTest('expr'):
      expr = abc_expr.bind_op(op_id, p_x)
      self.assertEqual(repr(expr), 'id(P.x)')
    with self.subTest('qvalue'):
      expr = abc_expr.bind_op(op_id, abc_qtype.unspecified())
      self.assertEqual(repr(expr), 'id(unspecified)')

  def test_kwargs(self):
    with self.subTest('expr'):
      expr = abc_expr.bind_op(op_id, x=p_x)
      self.assertEqual(repr(expr), 'id(P.x)')
    with self.subTest('qvalue'):
      expr = abc_expr.bind_op(op_id, x=abc_qtype.unspecified())
      self.assertEqual(repr(expr), 'id(unspecified)')

  def test_bad_op(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.bind_op() missing 1 required positional argument: 'op'",
    ):
      _ = abc_expr.bind_op()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.bind_op() expected Operator|str, got op:'
        ' arolla.abc.qtype.QType',
    ):
      _ = abc_expr.bind_op(abc_qtype.NOTHING)
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'arolla.abc.bind_op() expected Operator|str, got op: int'
    ):
      _ = abc_expr.bind_op(1)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        "arolla.abc.bind_op() operator not found: 'missing.operator'",
    ):
      _ = abc_expr.bind_op('missing.operator')

  def test_bad_arg(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.bind_op() expected Expr|QValue, got args[0]: float',
    ):
      _ = abc_expr.bind_op(op_id, 1.5)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.bind_op() expected Expr|QValue, got kwargs['x']: float",
    ):
      _ = abc_expr.bind_op(op_id, x=1.5)  # pytype: disable=wrong-arg-types

  def test_missing_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "missing 1 required argument: 'x'; while binding operator 'id'"
        ),
    ):
      _ = abc_expr.bind_op(op_id)

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.bind_op),
        inspect.signature(lambda op, /, *args, **kwargs: None),
    )


class ExprUtilsTest(absltest.TestCase):

  def assertExprListEqual(self, actual, expected):
    self.assertListEqual(
        [n.fingerprint for n in actual], [n.fingerprint for n in expected]
    )

  def testMakeLambdaUndeclaredParameterError(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('P.x is missing in the list of lambda parameters'),
    ):
      _ = abc_expr.make_lambda('', abc_expr.placeholder('x'))

  def testToLowerNode_Operator(self):
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity1')
    op2 = abc_expr.make_lambda('y', abc_expr.placeholder('y'), name='identity2')
    value = dummy_types.make_dummy_value()
    expr = abc_expr.bind_op(op1, abc_expr.bind_op(op2, value))
    actual_expr = abc_expr.to_lower_node(expr)
    expected_expr = abc_expr.bind_op(op2, value)
    self.assertEqual(actual_expr.fingerprint, expected_expr.fingerprint)

  def testToLowerNode_NonOperator(self):
    expr = abc_expr.leaf('leaf')
    self.assertEqual(abc_expr.to_lower_node(expr).fingerprint, expr.fingerprint)

  def testToLowerNode_TypeError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected arolla.abc.Expr, got object'
    ):
      abc_expr.to_lower_node(object())  # pytype: disable=wrong-arg-types

  def testToLowerNode_Signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.to_lower_node),
        inspect.signature(lambda node, /: None),
    )

  def testToLowest(self):
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity')
    op2 = abc_expr.make_lambda(
        'y', abc_expr.bind_op(op, abc_expr.placeholder('y')), name='identity2'
    )
    value = dummy_types.make_dummy_value()
    expr = abc_expr.bind_op(op2, value)
    actual_expr = abc_expr.to_lowest(expr)
    expected_expr = abc_expr.literal(value)
    self.assertEqual(actual_expr.fingerprint, expected_expr.fingerprint)

  def testToLowest_TypeError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected arolla.abc.Expr, got object'
    ):
      abc_expr.to_lowest(object())  # pytype: disable=wrong-arg-types

  def testToLowest_Signature(self):
    self.assertEqual(
        inspect.signature(abc_expr.to_lowest),
        inspect.signature(lambda expr, /: None),
    )

  def testDoc(self):
    op = abc_expr.make_lambda(
        'x', abc_expr.placeholder('x'), name='identity', doc='doc-string'
    )
    self.assertEqual(clib.get_operator_doc(op), 'doc-string')

  def testGetLeafQTypeMap(self):
    get_fst_op = abc_expr.make_lambda(
        'x, y', abc_expr.placeholder('x'), name='get_fst'
    )
    qtype_op = abc_expr.lookup_operator('annotation.qtype')
    dummy_qtype = dummy_types.make_dummy_value().qtype
    typed_x = abc_expr.bind_op(qtype_op, abc_expr.leaf('x'), dummy_qtype)
    expr = abc_expr.bind_op(get_fst_op, typed_x, abc_expr.leaf('y'))
    leaf_qtypes = abc_expr.get_leaf_qtype_map(expr)
    self.assertDictEqual(leaf_qtypes, {'x': dummy_qtype})

  def testGetLeafQTypeMap_Inconsistent(self):
    get_fst_op = abc_expr.make_lambda(
        'x, y', abc_expr.placeholder('x'), name='get_fst'
    )
    qtype_op = abc_expr.lookup_operator('annotation.qtype')
    dummy_qtype = dummy_types.make_dummy_value().qtype
    typed_x_1 = abc_expr.bind_op(qtype_op, abc_expr.leaf('x'), dummy_qtype)
    typed_x_2 = abc_expr.bind_op(
        qtype_op, abc_expr.leaf('x'), abc_qtype.NOTHING
    )
    expr = abc_expr.bind_op(get_fst_op, typed_x_1, typed_x_2)
    with self.assertRaisesRegex(
        ValueError, 'inconsistent qtype annotations for L.x'
    ):
      _ = abc_expr.get_leaf_qtype_map(expr)

  def testGetRegistryRevisionId(self):
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='identity')
    g0 = abc_expr.get_registry_revision_id()
    self.assertEqual(abc_expr.get_registry_revision_id(), g0)
    abc_expr.register_operator('revision_id_test.op', op)
    self.assertNotEqual(abc_expr.get_registry_revision_id(), g0)

  def testCheckRegisteredOperatorPresence(self):
    reg_op_name = 'expr_test.test_registered_operator_presence.name'
    op_impl = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    self.assertFalse(abc_expr.check_registered_operator_presence(reg_op_name))
    _ = abc_expr.register_operator(reg_op_name, op_impl)
    self.assertTrue(abc_expr.check_registered_operator_presence(reg_op_name))
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an operator name, got object'
    ):
      abc_expr.check_registered_operator_presence(object())  # pytype: disable=wrong-arg-types
    self.assertEqual(
        inspect.signature(abc_expr.check_registered_operator_presence),
        inspect.signature(lambda op_name, /: None),
    )

  def testUnsafeMakeRegisteredOperator(self):
    reg_op_name = 'expr_test.test_unsafe_make_reg_node.name'
    op_impl = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    op = abc_expr.unsafe_make_registered_operator(reg_op_name)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            '[NOT_FOUND] operator'
            " 'expr_test.test_unsafe_make_reg_node.name' not found"
        ),
    ):
      abc_expr.bind_op(op, abc_qtype.QTYPE)
    _ = abc_expr.register_operator(reg_op_name, op_impl)
    _ = abc_expr.bind_op(op, abc_qtype.QTYPE)  # no error
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an operator name, got object'
    ):
      abc_expr.unsafe_make_registered_operator(object())  # pytype: disable=wrong-arg-types
    self.assertEqual(
        inspect.signature(abc_expr.unsafe_make_registered_operator),
        inspect.signature(lambda op_name, /: None),
    )

  def testRegisterOperator(self):
    reg_op_name = 'expr_test.test_register_operator.name'
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    reg_op = abc_expr.register_operator(reg_op_name, op)
    self.assertNotEqual(op.fingerprint, reg_op.fingerprint)
    self.assertEqual(
        reg_op.fingerprint, abc_expr.lookup_operator(reg_op_name).fingerprint
    )
    self.assertEqual(
        op.fingerprint,
        abc_expr.decay_registered_operator(reg_op_name).fingerprint,
    )

  def testOverrideRegisteredOperator(self):
    reg_op_name = 'expr_test.test_override_registered_operator.name'
    op1 = abc_expr.make_lambda('x, unused_y', abc_expr.placeholder('x'))
    op2 = abc_expr.make_lambda('unused_x, y', abc_expr.placeholder('y'))
    reg_op = abc_expr.register_operator(reg_op_name, op1)
    expr1 = abc_expr.bind_op(reg_op, abc_qtype.QTYPE, abc_qtype.NOTHING)
    self.assertEqual(expr1.qvalue, abc_qtype.QTYPE)
    with mock.patch.object(
        abc_utils,
        'clear_caches',
        autospec=True,
    ) as mock_clear_caches:
      with warnings.catch_warnings():
        warnings.simplefilter('error')
        reg_op_2 = abc_expr.unsafe_override_registered_operator(
            reg_op_name, op1
        )
        self.assertEqual(reg_op.fingerprint, reg_op_2.fingerprint)
      mock_clear_caches.assert_not_called()
    with mock.patch.object(
        abc_utils,
        'clear_caches',
        autospec=True,
    ) as mock_clear_caches:
      with self.assertWarnsRegex(
          RuntimeWarning,
          re.escape(
              'expr operator implementation was replaced in the registry:'
              f' {reg_op_name}'
          ),
      ):
        abc_expr.unsafe_override_registered_operator(reg_op_name, op2)
      mock_clear_caches.assert_called_once()
    expr2 = abc_expr.bind_op(reg_op, abc_qtype.QTYPE, abc_qtype.NOTHING)
    self.assertEqual(expr2.qvalue, abc_qtype.NOTHING)
    self.assertNotEqual(expr1.fingerprint, expr2.fingerprint)

  def testDecayRegisteredOperator(self):
    reg_op_name = 'expr_test.test_decay_registered_operator.name'
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    reg_op = abc_expr.register_operator(reg_op_name, op)
    self.assertEqual(
        op.fingerprint,
        abc_expr.decay_registered_operator(reg_op_name).fingerprint,
    )
    self.assertEqual(
        op.fingerprint, abc_expr.decay_registered_operator(reg_op).fingerprint
    )
    self.assertEqual(
        op.fingerprint, abc_expr.decay_registered_operator(op).fingerprint
    )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.decay_registered_operator() expected Operator|str, got'
        ' op: int',
    ):
      abc_expr.decay_registered_operator(1)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.decay_registered_operator() expected Operator|str, got op:'
        ' arolla.abc.qtype.QValue',
    ):
      abc_expr.decay_registered_operator(dummy_types.make_dummy_value())

  def testDecayRegisteredOperatorAlias(self):
    reg_op1_name = 'expr_test.test_decay_registered_operator_alias.name1'
    reg_op2_name = 'expr_test.test_decay_registered_operator_alias.name2'
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    reg_op1 = abc_expr.register_operator(reg_op1_name, op)
    reg_op2 = abc_expr.register_operator(reg_op2_name, reg_op1)
    self.assertEqual(
        op.fingerprint, abc_expr.decay_registered_operator(reg_op2).fingerprint
    )
    self.assertEqual(
        op.fingerprint,
        abc_expr.decay_registered_operator(reg_op2_name).fingerprint,
    )

  def testDecayRegisteredOperatorNotFound(self):
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        'arolla.abc.decay_registered_operator() operator not found:'
        " 'op.not.found'",
    ):
      _ = abc_expr.decay_registered_operator('op.not.found')

  def testDecayRegisteredOperatorSignature(self):
    self.assertEqual(
        inspect.signature(abc_expr.decay_registered_operator),
        inspect.signature(lambda op, /: None),
    )

  def testIsAnnotationOperator(self):
    reg_op_name = 'expr_test.test_is_annotation_operator.op'
    reg_qtype_op_name = 'expr_test.test_is_annotation_operator.qtype_op'
    op = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    reg_op = abc_expr.register_operator(reg_op_name, op)
    qtype_op = abc_expr.lookup_operator('annotation.qtype')
    reg_qtype_op = abc_expr.register_operator(reg_qtype_op_name, qtype_op)
    self.assertFalse(abc_expr.is_annotation_operator(op))
    self.assertFalse(abc_expr.is_annotation_operator(reg_op))
    self.assertFalse(abc_expr.is_annotation_operator(reg_op_name))
    self.assertTrue(abc_expr.is_annotation_operator(qtype_op))
    self.assertTrue(abc_expr.is_annotation_operator(reg_qtype_op))
    self.assertTrue(abc_expr.is_annotation_operator(reg_qtype_op_name))
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.is_annotation_operator() expected Operator|str, got op:'
        ' NoneType',
    ):
      _ = abc_expr.is_annotation_operator(None)  # pytype: disable=wrong-arg-types

  def testIsAnnotationOperatorNotFound(self):
    with self.assertRaisesWithLiteralMatch(
        LookupError,
        'arolla.abc.is_annotation_operator() operator not found:'
        " 'op.not.found'",
    ):
      _ = abc_expr.is_annotation_operator('op.not.found')

  def testIsAnnotationOperatorSignature(self):
    self.assertEqual(
        inspect.signature(abc_expr.is_annotation_operator),
        inspect.signature(lambda op, /: None),
    )

  def testRegisterOperatorAlreadyExistsError(self):
    reg_op_name = 'expr_test.test_register_operator.non_unique_name'
    op1 = abc_expr.make_lambda('x', abc_expr.placeholder('x'))
    op2 = abc_expr.make_lambda('y', abc_expr.placeholder('y'))
    _ = abc_expr.register_operator(reg_op_name, op1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "operator 'expr_test.test_register_operator.non_unique_name'"
            ' already exists'
        ),
    ):
      _ = abc_expr.register_operator(reg_op_name, op2)

  def testLookupUnknownOperator(self):
    with self.assertRaisesRegex(
        LookupError, re.escape('unknown operator: unknown.operator')
    ):
      _ = abc_expr.lookup_operator('unknown.operator')

  def test_get_field_qtypes_of_empty_tuple(self):
    # Note: Most of the get_field_qtypes() tests are located in qtype_test.py.
    # This test is placed here because we need access to lambda operators to
    # construct an empty tuple qtype.
    empty_tuple_qtype = abc_expr.bind_op(
        abc_expr.make_lambda('*x', abc_expr.placeholder('x'))
    ).qtype
    field_qtypes = abc_qtype.get_field_qtypes(empty_tuple_qtype)
    self.assertIsInstance(field_qtypes, tuple)
    self.assertEmpty(field_qtypes)

  def test_quote(self):
    value = dummy_types.make_dummy_value()
    op = abc_expr.make_lambda('x, unused_y', abc_expr.placeholder('x'))
    expr = abc_expr.bind_op(op, abc_expr.leaf('x'), abc_expr.literal(value))
    quoted = abc_expr.quote(expr)
    self.assertEqual(quoted.qtype, abc_expr.EXPR_QUOTE)

    unquoted = quoted.unquote()
    self.assertEqual(expr.fingerprint, unquoted.fingerprint)
    self.assertNotEqual(quoted.fingerprint, unquoted.fingerprint)

  def test_read_name_annotation(self):
    self.assertIsNone(abc_expr.read_name_annotation(l_x))
    self.assertIsNone(abc_expr.read_name_annotation(op_lit))
    self.assertEqual(
        abc_expr.read_name_annotation(
            testing_clib.with_name_annotation(l_x, 'hello')
        ),
        'hello',
    )
    self.assertEqual(
        abc_expr.read_name_annotation(
            testing_clib.with_name_annotation(
                testing_clib.with_name_annotation(l_x, 'hello'), 'world'
            )
        ),
        'world',
    )

    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected arolla.abc.Expr, got object'
    ):
      abc_expr.read_name_annotation(object())  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
