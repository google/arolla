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

"""Tests for arolla.types.qvalue.dispatch_operator_qvalue."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import scalar_qtype
from arolla.types.qvalue import dispatch_operator_qvalue


p_x = arolla_abc.placeholder('x')
p_y = arolla_abc.placeholder('y')
p_z = arolla_abc.placeholder('z')

NOTHING = arolla_abc.NOTHING
FLOAT32 = scalar_qtype.FLOAT32
INT32 = scalar_qtype.INT32
TEXT = scalar_qtype.TEXT
UNIT = scalar_qtype.UNIT

DispatchCase = dispatch_operator_qvalue.DispatchCase
equal = arolla_abc.lookup_operator('core.equal')
presence_or = arolla_abc.lookup_operator('core.presence_or')


str_join_dispatch_case = DispatchCase(
    arolla_abc.lookup_operator('strings.join'),
    condition=arolla_abc.bind_op('core.equal', p_x, TEXT),
)


class DispatchOperatorQValueTest(parameterized.TestCase):

  def test_repr(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y',
        name='foo.bar',
        str_case=str_join_dispatch_case,
        default=arolla_abc.lookup_operator('math.add'),
    )
    self.assertEqual(
        repr(op),
        "<DispatchOperator: name='foo.bar', signature='x, y',"
        " cases=['str_case', 'default']>",
    )

  def test_repr_no_default(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y', str_case=str_join_dispatch_case
    )
    self.assertEqual(
        repr(op),
        "<DispatchOperator: name='anonymous.dispatch_operator', signature='x,"
        " y', cases=['str_case']>",
    )

  @parameterized.parameters(
      ((INT32, INT32), INT32),
      ((FLOAT32, FLOAT32), FLOAT32),
      ((TEXT, TEXT), TEXT),
  )
  def test_dispatch(self, input_qtypes, expected_qtype):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y',
        str_case=str_join_dispatch_case,
        default=arolla_abc.lookup_operator('math.add'),
    )
    self.assertEqual(
        arolla_abc.infer_attr(op, input_qtypes).qtype, expected_qtype
    )

  def test_dispatch_ambiguous(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y',
        str_case=str_join_dispatch_case,
        text_case=str_join_dispatch_case,
        default=arolla_abc.lookup_operator('math.add'),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'constraints of the multiple overloads (str_case, text_case) passed'
            ' for argument types (TEXT,TEXT)'
        ),
    ):
      _ = arolla_abc.infer_attr(op, (TEXT, TEXT)).qtype

  @parameterized.parameters(
      ((TEXT, TEXT), TEXT),
  )
  def test_dispatch_no_default(self, input_qtypes, expected_qtype):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y', str_case=str_join_dispatch_case
    )
    self.assertEqual(
        arolla_abc.infer_attr(op, input_qtypes).qtype, expected_qtype
    )

  def test_dispatch_no_matching_overload(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y', str_case=str_join_dispatch_case
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('no suitable overload for argument types (INT32,INT32)'),
    ):
      _ = arolla_abc.infer_attr(op, (INT32, INT32)).qtype

  def test_wrapping_expr(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x,y',
        text_case=str_join_dispatch_case,
        default=arolla_abc.bind_op('math.add', p_x, p_y),
    )
    self.assertEqual(
        repr(op(arolla_abc.leaf('abc'), arolla_abc.leaf('def'))),
        'anonymous.dispatch_operator(L.abc, L.def)',
    )

  def test_name(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x',
        seq_case=str_join_dispatch_case,
        default=arolla_abc.lookup_operator('array.size'),
        name='op.name',
    )
    self.assertEqual(repr(op(arolla_abc.leaf('abc'))), 'op.name(L.abc)')

  def test_unused_param(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x, y',
        seq_case=DispatchCase(p_x, arolla_abc.bind_op('core.equal', p_x, TEXT)),
        default=p_x,
        name='op.name',
    )
    self.assertEqual(arolla_abc.infer_attr(op, (TEXT, TEXT)).qtype, TEXT)

  def test_qvalue_specialization(self):
    op = dispatch_operator_qvalue.DispatchOperator(
        'x, y',
        seq_case=DispatchCase(p_x, arolla_abc.bind_op('core.equal', p_x, TEXT)),
        default=p_x,
        name='op.name',
    )
    self.assertIsInstance(op, dispatch_operator_qvalue.DispatchOperator)


if __name__ == '__main__':
  absltest.main()
