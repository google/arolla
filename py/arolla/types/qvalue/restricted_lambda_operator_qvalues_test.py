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

"""Tests for arolla.types.qvalue.restricted_lambda_operator_qvalues."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qvalue import restricted_lambda_operator_qvalues

P_x = arolla_abc.placeholder('x')
P_y = arolla_abc.placeholder('y')


class RestrictedLambdaOperatorQValueTest(parameterized.TestCase):

  def test_no_name(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(0)
    self.assertIsInstance(
        op, restricted_lambda_operator_qvalues.RestrictedLambdaOperator
    )
    self.assertEqual(repr(op()), 'anonymous.restricted_lambda()')

  def test_name(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
        0, name='op.name'
    )
    self.assertIsInstance(
        op, restricted_lambda_operator_qvalues.RestrictedLambdaOperator
    )
    self.assertEqual(repr(op()), 'op.name()')

  def test_doc(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
        0, doc='op doc'
    )
    self.assertIn(' RestrictedLambdaOperator', type(op).__doc__)
    self.assertEqual(op.getdoc(), 'op doc')

  def test_trivial_signature_0(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(0)
    self.assertIsInstance(
        op, restricted_lambda_operator_qvalues.RestrictedLambdaOperator
    )
    self.assertEqual(repr(arolla_abc.to_lower_node(op())), '0')

  def test_trivial_signature_1(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(P_x)
    self.assertIsInstance(
        op, restricted_lambda_operator_qvalues.RestrictedLambdaOperator
    )
    self.assertEqual(repr(arolla_abc.to_lower_node(op(1))), '1')

  def test_explicit_signature(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
        'x, y', arolla_abc.bind_op('math.add', P_x, P_y)
    )
    self.assertIsInstance(
        op, restricted_lambda_operator_qvalues.RestrictedLambdaOperator
    )
    self.assertEqual(repr(arolla_abc.to_lower_node(op(1, 2))), '1 + 2')

  def test_lambda_body(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
        'x, y', arolla_abc.bind_op('math.add', P_x, P_y)
    )
    self.assertEqual(repr(op.lambda_body), 'P.x + P.y')

  def test_no_args_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('missing 1 required positional argument')
    ):
      _ = restricted_lambda_operator_qvalues.RestrictedLambdaOperator()

  def test_too_many_args_error(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape('takes from 1 to 2 positional arguments but 3 were given'),
    ):
      _ = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(1, 2, 3)

  def test_missing_signature_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('please provide explicit operator signature')
    ):
      _ = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
          arolla_abc.bind_op('math.add', P_x, P_y)
      )

  def test_missing_parameter_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('P.y is missing in the list of lambda parameters')
    ):
      _ = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
          'x', arolla_abc.bind_op('math.add', P_x, P_y)
      )

  def test_qtype_constraint(self):
    op = restricted_lambda_operator_qvalues.RestrictedLambdaOperator(
        P_x,
        qtype_constraints=[
            (
                arolla_abc.bind_op('core.equal', P_x, arolla_abc.QTYPE),
                'expected QTYPE, got x:{x}',
            ),
        ],
    )
    self.assertEqual(
        repr(arolla_abc.to_lower_node(op(arolla_abc.QTYPE))), 'QTYPE'
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected QTYPE, got x:INT32')
    ):
      _ = op(1)


if __name__ == '__main__':
  absltest.main()
