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

"""Tests for arolla.abc.expr_view."""

import re
from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import expr_view as abc_expr_view
from arolla.abc import qtype as abc_qtype


Expr = abc_expr.Expr

l_x = abc_expr.leaf('x')

op_identity = abc_expr.make_lambda('x', abc_expr.placeholder('x'), name='id')


class ExprViewTest(absltest.TestCase):

  def tearDown(self):
    abc_expr_view.unsafe_set_default_expr_view(None)
    abc_expr_view.set_expr_view_for_registered_operator(
        'annotation.qtype', None
    )
    abc_expr_view.set_expr_view_for_operator_family(
        op_identity._specialization_key, None
    )
    abc_expr_view.set_expr_view_for_qtype(
        dummy_types.make_dummy_value().qtype, None
    )
    abc_expr_view.set_expr_view_for_qtype_family(
        dummy_types.make_dummy_value().qtype._specialization_key, None
    )
    super().tearDown()

  def test_register_remove_default_view_member(self):
    self.assertFalse(hasattr(l_x, 'attr_name'))
    abc_expr_view.unsafe_register_default_expr_view_member('attr_name', 1)
    self.assertTrue(hasattr(l_x, 'attr_name'))
    self.assertTrue(l_x.attr_name, 1)
    abc_expr_view.unsafe_remove_default_expr_view_member('attr_name')
    self.assertFalse(hasattr(l_x, 'attr_name'))
    with self.assertRaisesWithLiteralMatch(
        ValueError, "an expr-view cannot have a member: '__doc__'"
    ):
      abc_expr_view.unsafe_register_default_expr_view_member('__doc__', 'text')

  def test_set_default_expr_view(self):
    class View(abc_expr_view.ExprView):
      attr_name = 1

    self.assertFalse(hasattr(l_x, 'attr_name'))
    abc_expr_view.unsafe_set_default_expr_view(View)
    self.assertTrue(hasattr(l_x, 'attr_name'))
    self.assertTrue(l_x.attr_name, 1)
    abc_expr_view.unsafe_set_default_expr_view(None)
    self.assertFalse(hasattr(l_x, 'attr_name'))

  def test_bad_expr_view_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a subclass of rl.abc.ExprView'
    ):
      abc_expr_view.unsafe_set_default_expr_view(int)  # pytype: disable=wrong-arg-types

  def test_bad_expr_view_member(self):
    class BadView(abc_expr_view.ExprView):

      def __hash__(self):
        return id(self)

    with self.assertRaisesWithLiteralMatch(
        ValueError, "an expr-view cannot have a member: '__hash__'"
    ):
      abc_expr_view.unsafe_set_default_expr_view(BadView)

  def test_member_method(self):
    def op_name(node: Expr) -> str | None:
      if node.op is None:
        return None
      return clib.get_operator_name(node.op)

    expr = abc_expr.bind_op(op_identity, abc_expr.leaf('x'))
    abc_expr_view.unsafe_register_default_expr_view_member('op_name', op_name)
    self.assertEqual(expr.op_name(), 'id')
    self.assertIsNone(l_x.op_name())

  def test_member_property(self):
    @property
    def op_name(node: Expr) -> str | None:
      if node.op is None:
        return None
      return clib.get_operator_name(node.op)

    expr = abc_expr.bind_op(op_identity, abc_expr.leaf('x'))
    abc_expr_view.unsafe_register_default_expr_view_member('op_name', op_name)
    self.assertEqual(expr.op_name, 'id')
    self.assertIsNone(l_x.op_name)

  def test_member_static_method(self):
    @staticmethod
    def static_method(*args):
      return args

    expr = abc_expr.bind_op(op_identity, abc_expr.leaf('x'))
    abc_expr_view.unsafe_register_default_expr_view_member(
        'static_method', static_method
    )
    self.assertEqual(expr.static_method(), ())
    self.assertEqual(l_x.static_method(1, 2), (1, 2))

  def test_member_class_method(self):
    @classmethod
    def class_method(*args):
      return args

    expr = abc_expr.bind_op(op_identity, abc_expr.leaf('x'))
    abc_expr_view.unsafe_register_default_expr_view_member(
        'class_method', class_method
    )
    self.assertEqual(expr.class_method(), (Expr,))
    self.assertEqual(l_x.class_method(1, 2), (Expr, 1, 2))

  def test_member_object(self):
    obj = object()
    abc_expr_view.unsafe_register_default_expr_view_member('obj', obj)
    self.assertIs(l_x.obj, obj)

  def test_member_unknown(self):
    with self.assertRaisesWithLiteralMatch(
        AttributeError, "'arolla.abc.Expr' object has no attribute 'unknown'"
    ):
      _ = l_x.unknown

  def test_richcompare(self):
    class RichCompareView(abc_expr_view.ExprView):

      def __lt__(self, other):
        return ('lt', self.fingerprint, other)

      def __le__(self, other):
        return ('le', self.fingerprint, other)

      def __eq__(self, other):
        return ('eq', self.fingerprint, other)

      def __ne__(self, other):
        return ('ne', self.fingerprint, other)

      def __ge__(self, other):
        return ('ge', self.fingerprint, other)

      def __gt__(self, other):
        return ('gt', self.fingerprint, other)

    abc_expr_view.unsafe_set_default_expr_view(RichCompareView)
    l_y = abc_expr.leaf('x')
    self.assertEqual((l_y < 0), ('lt', l_y.fingerprint, 0))
    self.assertEqual((l_y <= 0), ('le', l_y.fingerprint, 0))
    self.assertEqual((l_y == 0), ('eq', l_y.fingerprint, 0))
    self.assertEqual((l_y != 0), ('ne', l_y.fingerprint, 0))
    self.assertEqual((l_y >= 0), ('ge', l_y.fingerprint, 0))
    self.assertEqual((l_y > 0), ('gt', l_y.fingerprint, 0))

  def test_richcompare_default(self):
    l_y = abc_expr.leaf('x')
    with self.assertRaisesRegex(
        TypeError,
        re.escape('__eq__ and __ne__ are disabled for arolla.abc.Expr'),
    ):
      _ = l_y == 1
    with self.assertRaisesRegex(
        TypeError,
        re.escape('__eq__ and __ne__ are disabled for arolla.abc.Expr'),
    ):
      _ = l_y != 1
    with self.assertRaises(TypeError):
      _ = l_y < 1

  def test_as_number_power(self):
    class EnablePow(abc_expr_view.ExprView):

      def __pow__(self, other, *args):
        return ('pow', self.fingerprint, other) + args

      def __rpow__(self, other, *args):
        return ('rpow', self.fingerprint, other) + args

    abc_expr_view.unsafe_set_default_expr_view(EnablePow)
    p_x = abc_expr.placeholder('x')
    self.assertEqual(Expr.__pow__(p_x, 2), ('pow', p_x.fingerprint, 2))
    self.assertEqual(p_x**2, ('pow', p_x.fingerprint, 2))
    self.assertEqual(pow(p_x, 2, 3), ('pow', p_x.fingerprint, 2, 3))  # pytype: disable=wrong-arg-types
    p_y = abc_expr.placeholder('y')
    self.assertEqual(Expr.__rpow__(p_y, 2), ('rpow', p_y.fingerprint, 2))
    self.assertEqual(2**p_y, ('rpow', p_y.fingerprint, 2))
    self.assertEqual(pow(2, p_y, 3), ('rpow', p_y.fingerprint, 2, 3))  # pytype: disable=wrong-arg-types

  def test_as_number_unary(self):
    class EnableNeg(abc_expr_view.ExprView):

      def __neg__(self):
        return ('neg', self.fingerprint)

    abc_expr_view.unsafe_set_default_expr_view(EnableNeg)
    p_x = abc_expr.placeholder('x')
    self.assertEqual(Expr.__neg__(p_x), ('neg', p_x.fingerprint))
    self.assertEqual(-p_x, ('neg', p_x.fingerprint))

  def test_as_number_binary(self):
    class EnableAdd(abc_expr_view.ExprView):

      def __add__(self, *args):
        return ('add', self.fingerprint) + args

      def __radd__(self, *args):
        return ('radd', self.fingerprint) + args

    abc_expr_view.unsafe_set_default_expr_view(EnableAdd)
    p_x = abc_expr.placeholder('x')
    self.assertEqual(Expr.__add__(p_x, 2), ('add', p_x.fingerprint, 2))
    self.assertEqual(p_x + 2, ('add', p_x.fingerprint, 2))
    p_y = abc_expr.placeholder('y')
    self.assertEqual(Expr.__radd__(p_y, 2), ('radd', p_y.fingerprint, 2))
    self.assertEqual(2 + p_y, ('radd', p_y.fingerprint, 2))

  def test_as_number_default(self):
    p_x = abc_expr.placeholder('x')
    with self.assertRaises(TypeError):
      _ = p_x + 1
    with self.assertRaises(TypeError):
      _ = 1 + p_x
    with self.assertRaises(TypeError):
      _ = p_x - 1
    with self.assertRaises(TypeError):
      _ = 1 - p_x
    with self.assertRaises(TypeError):
      _ = p_x * 1
    with self.assertRaises(TypeError):
      _ = 1 * p_x
    with self.assertRaises(TypeError):
      _ = p_x % 1
    with self.assertRaises(TypeError):
      _ = 1 % p_x
    with self.assertRaises(TypeError):
      _ = p_x**2
    with self.assertRaises(TypeError):
      _ = 2**p_x
    with self.assertRaisesWithLiteralMatch(
        TypeError, "no expr-view provides '__pos__' implementation"
    ):
      _ = +p_x
    with self.assertRaisesWithLiteralMatch(
        TypeError, "no expr-view provides '__neg__' implementation"
    ):
      _ = -p_x
    with self.assertRaisesWithLiteralMatch(
        TypeError, "no expr-view provides '__invert__' implementation"
    ):
      _ = ~p_x
    with self.assertRaises(TypeError):
      _ = p_x << 1
    with self.assertRaises(TypeError):
      _ = 1 << p_x
    with self.assertRaises(TypeError):
      _ = p_x >> 1
    with self.assertRaises(TypeError):
      _ = 1 >> p_x
    with self.assertRaises(TypeError):
      _ = p_x & 1
    with self.assertRaises(TypeError):
      _ = 1 & p_x
    with self.assertRaises(TypeError):
      _ = p_x ^ 1
    with self.assertRaises(TypeError):
      _ = 1 ^ p_x
    with self.assertRaises(TypeError):
      _ = p_x | 1
    with self.assertRaises(TypeError):
      _ = 1 | p_x
    with self.assertRaises(TypeError):
      _ = p_x // 1
    with self.assertRaises(TypeError):
      _ = 1 // p_x
    with self.assertRaises(TypeError):
      _ = p_x / 1
    with self.assertRaises(TypeError):
      _ = 1 / p_x
    with self.assertRaises(TypeError):
      _ = p_x @ 1
    with self.assertRaises(TypeError):
      _ = 1 @ p_x

  def test_expr_view_for_operator(self):
    class View1(abc_expr_view.ExprView):
      view1_attr = True
      view_name = 'view1'

    class View2(abc_expr_view.ExprView):
      view2_attr = True
      view_name = 'view2'

    expr = abc_expr.bind_op(
        'annotation.qtype', abc_expr.leaf('x'), abc_qtype.QTYPE
    )
    self.assertFalse(hasattr(expr, 'view_name'))
    self.assertFalse(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))
    abc_expr_view.set_expr_view_for_registered_operator(
        'annotation.qtype', View1
    )
    self.assertTrue(hasattr(expr, 'view_name'))
    self.assertTrue(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))
    self.assertEqual(expr.view_name, 'view1')
    abc_expr_view.set_expr_view_for_operator_family(
        expr.op._specialization_key, View2
    )
    self.assertTrue(hasattr(expr, 'view_name'))
    self.assertTrue(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))
    self.assertEqual(expr.view_name, 'view1')
    abc_expr_view.set_expr_view_for_registered_operator(
        'annotation.qtype', None
    )
    self.assertTrue(hasattr(expr, 'view_name'))
    self.assertFalse(hasattr(expr, 'view1_attr'))
    self.assertTrue(hasattr(expr, 'view2_attr'))
    self.assertEqual(expr.view_name, 'view2')
    abc_expr_view.set_expr_view_for_operator_family(
        expr.op._specialization_key, None
    )
    self.assertFalse(hasattr(expr, 'view_name'))
    self.assertFalse(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))

  def test_expr_view_for_qtype(self):
    class View1(abc_expr_view.ExprView):
      view1_attr = True
      view_name = 'view1'

    class View2(abc_expr_view.ExprView):
      view2_attr = True
      view_name = 'view2'

    expr = abc_expr.literal(dummy_types.make_dummy_value())
    self.assertFalse(hasattr(expr, 'attr'))
    abc_expr_view.set_expr_view_for_qtype(expr.qtype, View1)
    self.assertTrue(hasattr(expr, 'view_name'))
    self.assertTrue(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))
    self.assertEqual(expr.view_name, 'view1')
    abc_expr_view.set_expr_view_for_qtype_family(
        expr.qtype._specialization_key, View2
    )
    self.assertTrue(hasattr(expr, 'view_name'))
    self.assertTrue(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))
    self.assertEqual(expr.view_name, 'view1')
    abc_expr_view.set_expr_view_for_qtype(expr.qtype, None)
    self.assertTrue(hasattr(expr, 'view_name'))
    self.assertFalse(hasattr(expr, 'view1_attr'))
    self.assertTrue(hasattr(expr, 'view2_attr'))
    self.assertEqual(expr.view_name, 'view2')
    abc_expr_view.set_expr_view_for_qtype_family(
        expr.qtype._specialization_key, None
    )
    self.assertFalse(hasattr(expr, 'view_name'))
    self.assertFalse(hasattr(expr, 'view1_attr'))
    self.assertFalse(hasattr(expr, 'view2_attr'))

  def test_expr_view_precedence(self):
    class AnnotationQTypeView(abc_expr_view.ExprView):

      def attr1(self):
        return 'annotation_qtype_view'

    class LambdaView(abc_expr_view.ExprView):

      def attr1(self):
        return 'lambda_view'

      def attr2(self):
        return 'lambda_view'

    class QTypeView(abc_expr_view.ExprView):

      def attr1(self):
        return 'qtype_view'

      def attr2(self):
        return 'qtype_view'

      def attr3(self):
        return 'qtype_view'

    class DefaultView(abc_expr_view.ExprView):

      def attr1(self):
        return 'default_view'

      def attr2(self):
        return 'default_view'

      def attr3(self):
        return 'default_view'

      def attr4(self):
        return 'default_view'

    expr = abc_expr.bind_op(
        'annotation.qtype',
        abc_expr.bind_op(op_identity, dummy_types.make_dummy_value()),
        dummy_types.make_dummy_value().qtype,
    )
    abc_expr_view.set_expr_view_for_registered_operator(
        'annotation.qtype', AnnotationQTypeView
    )
    abc_expr_view.set_expr_view_for_operator_family(
        op_identity._specialization_key, LambdaView
    )
    abc_expr_view.set_expr_view_for_qtype(expr.qtype, QTypeView)
    abc_expr_view.unsafe_set_default_expr_view(DefaultView)
    self.assertEqual(expr.attr1(), 'annotation_qtype_view')
    self.assertEqual(expr.attr2(), 'lambda_view')
    self.assertEqual(expr.attr3(), 'qtype_view')
    self.assertEqual(expr.attr4(), 'default_view')

  def test_op_with_empty_specialization_key(self):
    # we picked an arbitrary operator without a specialization key
    expr = abc_expr.bind_op(
        abc_expr.decay_registered_operator('annotation.qtype'),
        abc_expr.placeholder('x'),
        abc_expr.placeholder('y'),
    )
    assert not expr.op._specialization_key
    self.assertFalse(hasattr(expr, 'unkonwn_operator'))

  def test_expr_view_for_registered_operator_with_empty_name(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'cannot associate an expr-view to an operator with empty name',
    ):
      abc_expr_view.set_expr_view_for_registered_operator(
          '', abc_expr_view.ExprView
      )

  def test_expr_view_for_operator_family_with_empty_specialization_key(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'cannot associate an expr-view to an operator family with empty'
        ' qvalue_specialization_key',
    ):
      abc_expr_view.set_expr_view_for_operator_family(
          '', abc_expr_view.ExprView
      )

  def test_expr_view_for_qtype_family_with_empty_specialization_key(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'cannot associate an expr-view to a qtype family with empty'
        ' qtype_specialization_key',
    ):
      abc_expr_view.set_expr_view_for_qtype_family('', None)

  def test_default_expr_view_getattr(self):
    class DefaultView(abc_expr_view.ExprView):

      def attr(self, *args):
        return ('attr', (self.fingerprint,) + args)

      def __getattr__(self, key):
        # Use a lambda we want to check the handing of the method `__get__`.
        return lambda *args: ('getattr', (self.fingerprint, key), args)

    self.assertFalse(hasattr(l_x, 'attr'))
    self.assertFalse(hasattr(l_x, 'dynamic_attr'))
    abc_expr_view.unsafe_set_default_expr_view(DefaultView)
    self.assertTrue(hasattr(l_x, 'attr'))
    self.assertTrue(hasattr(l_x, 'dynamic_attr'))
    self.assertEqual(l_x.attr(1, 2), ('attr', (l_x.fingerprint, 1, 2)))
    self.assertEqual(
        l_x.dynamic_attr(1, 2),
        ('getattr', (l_x.fingerprint, 'dynamic_attr'), (1, 2)),
    )
    abc_expr_view.unsafe_remove_default_expr_view_member('__getattr__')
    self.assertTrue(hasattr(l_x, 'attr'))
    self.assertFalse(hasattr(l_x, 'dynamic_attr'))
    self.assertEqual(l_x.attr(1, 2), ('attr', (l_x.fingerprint, 1, 2)))

  def test_default_expr_view_getitem(self):
    class DefaultView(abc_expr_view.ExprView):

      def __getitem__(self, key):
        return ('getitem', self.fingerprint, key)

    abc_expr_view.unsafe_set_default_expr_view(DefaultView)
    self.assertEqual(l_x['key'], ('getitem', l_x.fingerprint, 'key'))
    self.assertEqual(l_x[1], ('getitem', l_x.fingerprint, 1))
    self.assertEqual(l_x[1:2], ('getitem', l_x.fingerprint, slice(1, 2)))
    abc_expr_view.unsafe_remove_default_expr_view_member('__getitem__')
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'arolla.abc.Expr' object is not subscriptable"
    ):
      _ = l_x[1]

  def test_default_expr_view_call(self):
    class DefaultView(abc_expr_view.ExprView):

      def __call__(self, *args, **kwargs):
        return ('call', self.fingerprint, args, tuple(kwargs.items()))

    abc_expr_view.unsafe_set_default_expr_view(DefaultView)
    self.assertEqual(l_x(1, w=2), ('call', l_x.fingerprint, (1,), (('w', 2),)))
    abc_expr_view.unsafe_remove_default_expr_view_member('__call__')
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'arolla.abc.Expr' object is not callable"
    ):
      l_x(1, w=2)

  def test_default_expr_view_iterable_sequence(self):
    class DefaultView(abc_expr_view.ExprView):

      def _arolla_sequence_getitem_(self, i):
        return [1, 2, 3][i]

    abc_expr_view.unsafe_set_default_expr_view(DefaultView)
    x, y, z = l_x
    self.assertEqual((x, y, z), (1, 2, 3))
    self.assertEqual(tuple(l_x), (1, 2, 3))
    self.assertEqual(tuple(iter(l_x)), (1, 2, 3))
    self.assertEqual(list(l_x), [1, 2, 3])
    self.assertEqual(list(iter(l_x)), [1, 2, 3])
    abc_expr_view.unsafe_remove_default_expr_view_member(
        '_arolla_sequence_getitem_')
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'arolla.abc.Expr' object is not iterable"
    ):
      list(l_x)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'arolla.abc.Expr' object is not iterable"
    ):
      list(iter(l_x))

  def test_default_sub_view(self):
    class ExprView1(abc_expr_view.ExprView):

      def attr(self):
        return 'attr1'

    class ExprView2(ExprView1):

      def attr(self):
        return 'attr2'

    class ExprView3(ExprView2):
      pass

    abc_expr_view.unsafe_set_default_expr_view(ExprView3)
    self.assertEqual(l_x.attr(), 'attr2')

  def test_dir(self):
    class DefaultView(abc_expr_view.ExprView):
      default_attr = True

    class View1(abc_expr_view.ExprView):
      attr1 = True

    class View2(abc_expr_view.ExprView):

      @classmethod
      def attr2(cls):
        return None

    abc_expr_view.unsafe_set_default_expr_view(DefaultView)
    abc_expr_view.set_expr_view_for_qtype(
        dummy_types.make_dummy_value().qtype, View1
    )
    abc_expr_view.set_expr_view_for_operator_family(
        op_identity._specialization_key, View2
    )
    expr1 = abc_expr.literal(dummy_types.make_dummy_value())
    expr2 = abc_expr.bind_op(op_identity, l_x)
    expr3 = abc_expr.bind_op(op_identity, expr1)
    self.assertLessEqual({'default_attr', 'attr1'}, set(dir(expr1)))
    self.assertLessEqual({'default_attr', 'attr2'}, set(dir(expr2)))
    self.assertLessEqual({'default_attr', 'attr1', 'attr2'}, set(dir(expr3)))
    self.assertNotIn('view1_attr', dir(expr2))
    self.assertNotIn('view2_attr', dir(expr1))


if __name__ == '__main__':
  absltest.main()
