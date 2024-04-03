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

"""Tests for arolla.abc.ExprQuote."""

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import dummy_types
from arolla.abc import expr as abc_expr
from arolla.abc import qtype as abc_qtype

DUMMY_VALUE = dummy_types.make_dummy_value().qtype


class ExprQuoteTest(absltest.TestCase):

  def tearDown(self):
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    super().tearDown()

  def test_construct(self):
    q = clib.ExprQuote(abc_expr.leaf('x'))
    self.assertIsInstance(q, clib.ExprQuote)
    self.assertEqual(repr(q), "ExprQuote('L.x')")

  def test_nested(self):
    q = clib.ExprQuote(abc_expr.literal(clib.ExprQuote(abc_expr.leaf('x'))))
    self.assertIsInstance(q, clib.ExprQuote)
    self.assertEqual(repr(q), "ExprQuote('ExprQuote(\\'L.x\\')')")

  def test_construct_with_bad_args(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'arolla.abc.ExprQuote() takes no keyword arguments'
    ):
      clib.ExprQuote(expr=abc_expr.leaf('x'))  # pytype: disable=wrong-keyword-args
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.ExprQuote() takes 1 positional argument but 0 were given',
    ):
      clib.ExprQuote()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.ExprQuote() expected an expression, got expr: object',
    ):
      clib.ExprQuote(object())  # pytype: disable=wrong-arg-types

  def test_quote_unqote(self):
    l_x = abc_expr.leaf('x')
    self.assertTrue(l_x.equals(clib.ExprQuote(l_x).unquote()))

  def test_hash(self):
    l_x = abc_expr.leaf('x')
    q = clib.ExprQuote(l_x)
    self.assertEqual(hash(q), hash(l_x.fingerprint))

  def test_comparison(self):
    q1 = clib.ExprQuote(abc_expr.leaf('x'))
    q2 = clib.ExprQuote(abc_expr.leaf('x'))
    p = clib.ExprQuote(abc_expr.leaf('y'))
    self.assertEqual(q1, q2)
    self.assertNotEqual(q1, p)
    with self.assertRaises(TypeError):
      _ = q1 < q2

  def test_qvalue_specialization_cannot_be_changed(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'QValue specialization for EXPR_QUOTE cannot be changed',
    ):
      abc_qtype.register_qvalue_specialization(
          abc_expr.EXPR_QUOTE, abc_qtype.QValue
      )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'QValue specialization for EXPR_QUOTE cannot be changed',
    ):
      abc_qtype.remove_qvalue_specialization(abc_expr.EXPR_QUOTE)

  def test_qvalue_subclass_abuse(self):
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, clib.ExprQuote)
    x = dummy_types.make_dummy_value()
    q = clib.ExprQuote(abc_expr.leaf('x'))
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'unexpected self.qtype=DUMMY_VALUE'
    ):
      hash(x)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'unexpected self.qtype=DUMMY_VALUE'
    ):
      _ = x == q
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'unexpected other.qtype=DUMMY_VALUE'
    ):
      _ = q != x
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'unexpected self.qtype=DUMMY_VALUE'
    ):
      x.unquote()

  def test_expr_quote_is_qvalue_subclass(self):
    self.assertTrue(issubclass(clib.ExprQuote, abc_qtype.QValue))

  def test_expr_quote_is_final_class(self):
    with self.assertRaises(TypeError):

      class T(clib.ExprQuote):
        pass

      del T


if __name__ == '__main__':
  absltest.main()
