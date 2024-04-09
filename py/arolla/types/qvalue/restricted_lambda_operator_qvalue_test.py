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

"""Tests for arolla.types.qvalue.restricted_lambda_operator_qvalue."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as rl_abc
from arolla.types.qvalue import restricted_lambda_operator_qvalue as rl_restricted_lambda_operator_qvalue

P_x = rl_abc.placeholder('x')
P_y = rl_abc.placeholder('y')


class RestrictedLambdaOperatorQValueTest(parameterized.TestCase):

  def test_no_name(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(0)
    self.assertIsInstance(
        op, rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator
    )
    self.assertEqual(repr(op()), 'anonymous.restricted_lambda()')

  def test_name(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
        0, name='op.name'
    )
    self.assertIsInstance(
        op, rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator
    )
    self.assertEqual(repr(op()), 'op.name()')

  def test_doc(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
        0, doc='op doc'
    )
    self.assertIn(' RestrictedLambdaOperator', type(op).__doc__)
    self.assertEqual(op.getdoc(), 'op doc')

  def test_trivial_signature_0(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(0)
    self.assertIsInstance(
        op, rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator
    )
    self.assertEqual(repr(rl_abc.to_lower_node(op())), '0')

  def test_trivial_signature_1(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(P_x)
    self.assertIsInstance(
        op, rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator
    )
    self.assertEqual(repr(rl_abc.to_lower_node(op(1))), '1')

  def test_explicit_signature(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
        'x, y', rl_abc.bind_op('math.add', P_x, P_y)
    )
    self.assertIsInstance(
        op, rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator
    )
    self.assertEqual(repr(rl_abc.to_lower_node(op(1, 2))), '1 + 2')

  def test_lambda_body(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
        'x, y', rl_abc.bind_op('math.add', P_x, P_y)
    )
    self.assertEqual(repr(op.lambda_body), 'P.x + P.y')

  def test_no_args_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('missing 1 required positional argument')
    ):
      _ = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator()

  def test_too_many_args_error(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape('takes from 1 to 2 positional arguments but 3 were given'),
    ):
      _ = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(1, 2, 3)

  def test_missing_signature_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('please provide explicit operator signature')
    ):
      _ = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
          rl_abc.bind_op('math.add', P_x, P_y)
      )

  def test_missing_parameter_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('P.y is missing in the list of lambda parameters')
    ):
      _ = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
          'x', rl_abc.bind_op('math.add', P_x, P_y)
      )

  def test_qtype_constraint(self):
    op = rl_restricted_lambda_operator_qvalue.RestrictedLambdaOperator(
        P_x,
        qtype_constraints=[
            (
                rl_abc.bind_op('core.equal', P_x, rl_abc.QTYPE),
                'expected QTYPE, got x:{x}',
            ),
        ],
    )
    self.assertEqual(repr(rl_abc.to_lower_node(op(rl_abc.QTYPE))), 'QTYPE')
    with self.assertRaisesRegex(
        ValueError, re.escape('expected QTYPE, got x:INT32')
    ):
      _ = op(1)


if __name__ == '__main__':
  absltest.main()
