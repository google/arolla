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

"""Tests for arolla.types.qvalue.lambda_operator_qvalue."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qvalue import clib
from arolla.types.qvalue import dummy_operator_qvalue as rl_dummy_operator_qvalue
from arolla.types.qvalue import lambda_operator_qvalue as rl_lambda_operator_qvalue


class LambdaOperatorQValueTest(parameterized.TestCase):

  def test_no_name(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(0)
    self.assertIsInstance(op, rl_lambda_operator_qvalue.LambdaOperator)
    self.assertEqual(repr(op()), 'anonymous.lambda()')

  def test_name(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(0, name='op.name')
    self.assertIsInstance(op, rl_lambda_operator_qvalue.LambdaOperator)
    self.assertEqual(repr(op()), 'op.name()')

  def test_doc(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(0, doc='op doc')
    self.assertIn(' LambdaOperator', type(op).__doc__)
    self.assertEqual(op.getdoc(), 'op doc')

  def test_trivial_signature_0(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(0)
    self.assertIsInstance(op, rl_lambda_operator_qvalue.LambdaOperator)
    self.assertEqual(repr(arolla_abc.to_lower_node(op())), '0')

  def test_trivial_signature_1(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(arolla_abc.placeholder('x'))
    self.assertIsInstance(op, rl_lambda_operator_qvalue.LambdaOperator)
    self.assertEqual(repr(arolla_abc.to_lower_node(op(1))), '1')

  def test_explicit_signature(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(
        'x, y',
        arolla_abc.bind_op(
            'math.add', arolla_abc.placeholder('x'), arolla_abc.placeholder('y')
        ),
    )
    self.assertIsInstance(op, rl_lambda_operator_qvalue.LambdaOperator)
    self.assertEqual(repr(arolla_abc.to_lower_node(op(1, 2))), '1 + 2')

  def test_lambda_body(self):
    op = rl_lambda_operator_qvalue.LambdaOperator(
        'x, y',
        arolla_abc.bind_op(
            'math.add', arolla_abc.placeholder('x'), arolla_abc.placeholder('y')
        ),
    )
    self.assertIsInstance(op, rl_lambda_operator_qvalue.LambdaOperator)
    self.assertEqual(repr(op.lambda_body), 'P.x + P.y')

  def test_lambda_body_from_constant_value(self):
    with self.subTest('no_arg'):
      op = rl_lambda_operator_qvalue.LambdaOperator(1)
      self.assertEqual(repr(op.lambda_body), '1')
    with self.subTest('unary'):
      op = rl_lambda_operator_qvalue.LambdaOperator('unused', 2)
      self.assertEqual(repr(op.lambda_body), '2')

  def test_no_args_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('missing 1 required positional argument')
    ):
      _ = rl_lambda_operator_qvalue.LambdaOperator()

  def test_too_many_args_error(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape('takes from 1 to 2 positional arguments but 3 were given'),
    ):
      _ = rl_lambda_operator_qvalue.LambdaOperator(1, 2, 3)

  def test_missing_signature_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('please provide explicit operator signature')
    ):
      _ = rl_lambda_operator_qvalue.LambdaOperator(
          arolla_abc.bind_op(
              'math.add',
              arolla_abc.placeholder('x'),
              arolla_abc.placeholder('y'),
          )
      )

  def test_missing_parameter_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('P.y is missing in the list of lambda parameters')
    ):
      _ = rl_lambda_operator_qvalue.LambdaOperator(
          'x',
          arolla_abc.bind_op(
              'math.add',
              arolla_abc.placeholder('x'),
              arolla_abc.placeholder('y'),
          ),
      )

  def test_get_lambda_operator_body_error(self):
    op = rl_dummy_operator_qvalue.DummyOperator(
        'dummy_op', '*args', result_qtype=arolla_abc.NOTHING
    )
    with self.assertRaisesRegex(
        TypeError, f'expected a lambda operator, got {op!r}'
    ):
      clib.get_lambda_body(op)


if __name__ == '__main__':
  absltest.main()
