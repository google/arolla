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

"""Tests for arolla.abc.attr.py."""

import inspect
import re

from absl.testing import absltest
from arolla.abc import attr as abc_attr
from arolla.abc import expr as abc_expr
from arolla.abc import operator as abc_operator
from arolla.abc import qtype as abc_qtype


class AttrTest(absltest.TestCase):

  def test_empty(self):
    attr = abc_attr.Attr()
    self.assertIsNone(attr.qtype)
    self.assertIsNone(attr.qvalue)
    self.assertFalse(attr)
    self.assertEqual(str(attr), 'Attr()')

  def test_qtype(self):
    attr = abc_attr.Attr(qtype=abc_qtype.QTYPE)
    self.assertEqual(attr.qtype, abc_qtype.QTYPE)
    self.assertIsNone(attr.qvalue)
    self.assertTrue(attr)
    self.assertEqual(str(attr), 'Attr(qtype=QTYPE)')

  def test_qvalue(self):
    attr = abc_attr.Attr(qvalue=abc_qtype.NOTHING)
    self.assertEqual(attr.qtype, abc_qtype.QTYPE)
    self.assertEqual(attr.qvalue, abc_qtype.NOTHING)
    self.assertTrue(attr)
    self.assertEqual(str(attr), 'Attr(qvalue=NOTHING)')

  def test_qtype_qvalue(self):
    attr = abc_attr.Attr(qtype=abc_qtype.QTYPE, qvalue=abc_qtype.NOTHING)
    self.assertEqual(attr.qtype, abc_qtype.QTYPE)
    self.assertEqual(attr.qvalue, abc_qtype.NOTHING)
    self.assertTrue(attr)
    self.assertEqual(str(attr), 'Attr(qvalue=NOTHING)')

  def test_error_unexpected_args(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('invalid keyword argument')
    ):
      _ = abc_attr.Attr(foo=abc_qtype.QTYPE)  # pytype: disable=wrong-keyword-args
    with self.assertRaisesRegex(
        TypeError, re.escape('no positional arguments')
    ):
      _ = abc_attr.Attr(abc_qtype.QTYPE)  # pytype: disable=wrong-arg-count

  def test_error_qtype_qvalue_mismatch(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('qtype mismatch: qtype=EXPR_OPERATOR, qvalue.qtype=QTYPE'),
    ):
      _ = abc_attr.Attr(qtype=abc_operator.OPERATOR, qvalue=abc_qtype.NOTHING)

  def test_error_not_qtype(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('expected QType, got object')
    ):
      _ = abc_attr.Attr(qtype=object())  # pytype: disable=wrong-arg-types

  def test_error_not_qvalue(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('expected QValue, got tuple')
    ):
      _ = abc_attr.Attr(qvalue=())  # pytype: disable=wrong-arg-types


class InferAttrTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._identity_op = abc_expr.make_lambda(
        'x', abc_expr.placeholder('x'), name='identity'
    )

  def test_arg_attr_empty(self):
    output_attr = abc_attr.infer_attr(self._identity_op, (abc_attr.Attr(),))
    self.assertFalse(output_attr)
    self.assertIsNone(output_attr.qtype)
    self.assertIsNone(output_attr.qvalue)

  def test_arg_attr_qtype(self):
    output_attr = abc_attr.infer_attr(
        self._identity_op, (abc_attr.Attr(qtype=abc_operator.OPERATOR),)
    )
    self.assertTrue(output_attr)
    self.assertEqual(output_attr.qtype, abc_operator.OPERATOR)
    self.assertIsNone(output_attr.qvalue)

  def test_arg_attr_qvalue(self):
    output_attr = abc_attr.infer_attr(
        self._identity_op, (abc_attr.Attr(qvalue=abc_operator.OPERATOR),)
    )
    self.assertTrue(output_attr)
    self.assertEqual(output_attr.qtype, abc_qtype.QTYPE)
    self.assertEqual(output_attr.qvalue, abc_operator.OPERATOR)

  def test_arg_none(self):
    output_attr = abc_attr.infer_attr(self._identity_op, (None,))
    self.assertFalse(output_attr)
    self.assertIsNone(output_attr.qtype)
    self.assertIsNone(output_attr.qvalue)

  def test_arg_qtype(self):
    output_attr = abc_attr.infer_attr(
        self._identity_op, (abc_operator.OPERATOR,)
    )
    self.assertTrue(output_attr)
    self.assertEqual(output_attr.qtype, abc_operator.OPERATOR)
    self.assertIsNone(output_attr.qvalue)

  def test_error_wrong_arg_count(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "arolla.abc.infer_attr() missing 1 required positional argument: 'op'",
    ):
      _ = abc_attr.infer_attr()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.infer_attr() takes 2 positional arguments but 3 were given',
    ):
      abc_attr.infer_attr(1, 2, 3)  # pytype: disable=wrong-arg-count

  def test_error_wrong_arg_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'arolla.abc.infer_attr() expected Operator|str, got op: int'
    ):
      _ = abc_attr.infer_attr(1)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.infer_attr() expected Operator|str, got op:'
        ' arolla.abc.qtype.QType',
    ):
      _ = abc_attr.infer_attr(abc_qtype.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.infer_attr() expected a tuple[Attr|QType|None, ...], got'
        ' input_attrs: list',
    ):
      _ = abc_attr.infer_attr(self._identity_op, [])  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.infer_attr() expected Attr or QType, got'
        ' input_attrs[1]: int',
    ):
      _ = abc_attr.infer_attr(self._identity_op, (None, 1))  # pytype: disable=wrong-arg-types

  def test_error_wrong_inputs_count(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('incorrect number of dependencies passed')
    ):
      _ = abc_attr.infer_attr(
          self._identity_op, (None, abc_qtype.QTYPE, abc_attr.Attr())
      )

  def test_signature(self):
    self.assertEqual(
        inspect.signature(abc_attr.infer_attr),
        inspect.signature(lambda op, input_attrs=(), /: None),
    )


if __name__ == '__main__':
  absltest.main()
