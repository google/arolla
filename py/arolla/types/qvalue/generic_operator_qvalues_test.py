# Copyright 2025 Google LLC
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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.testing import testing as arolla_testing
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qvalue import generic_operator_qvalues
from arolla.types.qvalue import lambda_operator_qvalues

p_x = arolla_abc.placeholder('x')
p_y = arolla_abc.placeholder('y')
p_z = arolla_abc.placeholder('z')

NOTHING = arolla_abc.NOTHING
UNIT = scalar_qtypes.UNIT
INT32 = scalar_qtypes.INT32
FLOAT32 = scalar_qtypes.FLOAT32

equal = arolla_abc.lookup_operator('core.equal')
presence_or = arolla_abc.lookup_operator('core.presence_or')


class GenericOperatorOverloadTest(parameterized.TestCase):

  def test_type(self):
    op = generic_operator_qvalues.GenericOperatorOverload(
        lambda_operator_qvalues.LambdaOperator(p_x),
        equal(p_x, arolla_abc.QTYPE),
    )
    self.assertIsInstance(op, generic_operator_qvalues.GenericOperatorOverload)

  def test_error_unexpected_parameters(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, "problem with an overload condition: 'anonymous.lambda'"
    ) as cm:
      _ = generic_operator_qvalues.GenericOperatorOverload(
          lambda_operator_qvalues.LambdaOperator(p_x),
          equal(p_y, arolla_abc.QTYPE),
      )
    self.assertIsInstance(cm.exception.__cause__, ValueError)
    self.assertRegex(
        str(cm.exception.__cause__),
        re.escape('expression contains unexpected placeholders: P.y'),
    )

  def test_regression(self):
    _ = generic_operator_qvalues.GenericOperatorOverload(
        lambda_operator_qvalues.LambdaOperator(scalar_qtypes.unit()),
        optional_qtypes.present(),
    )  # no failure


class GenericOperatorTest(parameterized.TestCase):

  def test_type(self):
    op = generic_operator_qvalues.GenericOperator('foo.bar', signature='x')
    self.assertIsInstance(op, generic_operator_qvalues.GenericOperator)

  def test_format_signature_qtype(self):
    base_op = lambda_operator_qvalues.LambdaOperator('x, *y', p_x)
    condition = equal(p_x, p_x)
    case_1 = generic_operator_qvalues.GenericOperatorOverload(
        base_op, condition, case_name='<case_1>'
    )
    case_2 = generic_operator_qvalues.GenericOperatorOverload(
        base_op, condition, case_name='<case_2>'
    )
    arolla_abc.register_operator(
        'generic_operator.test_format_signature_qtype.op._1', case_1
    )
    arolla_abc.register_operator(
        'generic_operator.test_format_signature_qtype.op._2', case_2
    )
    op = generic_operator_qvalues.GenericOperator(
        'generic_operator.test_format_signature_qtype.op', signature='x, y, *z'
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape("multiple overload cases matched: '<case_1>', '<case_2>'"),
    ) as cm:
      _ = op(
          scalar_qtypes.int32(1),
          p_x,
          scalar_qtypes.int32(2),
          p_x,
          scalar_qtypes.int32(3),
      )
    self.assertTrue(
        arolla_testing.any_note_regex(
            re.escape(
                'In generic operator:'
                " 'generic_operator.test_format_signature_qtype.op'."
            )
        )(cm.exception)
    )
    self.assertTrue(
        arolla_testing.any_note_regex(
            re.escape(
                'Input qtypes: x: INT32, y: NOTHING,'
                ' *z: (INT32, NOTHING, INT32)'
            )
        )(cm.exception)
    )

  def test_status_not_ok_in_constructor(self):
    with self.assertRaises(ValueError):
      _ = generic_operator_qvalues.GenericOperator(
          '<invalid-namespace-name>', signature='x'
      )


if __name__ == '__main__':
  absltest.main()
