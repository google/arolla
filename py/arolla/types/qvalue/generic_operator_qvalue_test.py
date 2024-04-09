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

"""Tests for arolla.types.qvalue.generic_operator_qvalue."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as rl_abc
from arolla.types.qtype import boxing
from arolla.types.qtype import optional_qtype
from arolla.types.qtype import scalar_qtype
from arolla.types.qtype import tuple_qtype
from arolla.types.qvalue import generic_operator_qvalue
from arolla.types.qvalue import lambda_operator_qvalue


p_x = rl_abc.placeholder('x')
p_y = rl_abc.placeholder('y')
p_z = rl_abc.placeholder('z')

NOTHING = rl_abc.NOTHING
UNIT = scalar_qtype.UNIT
INT32 = scalar_qtype.INT32
FLOAT32 = scalar_qtype.FLOAT32

equal = rl_abc.lookup_operator('core.equal')
presence_or = rl_abc.lookup_operator('core.presence_or')


class PrepareOverloadConditionExprTest(parameterized.TestCase):

  @parameterized.parameters(
      ('', optional_qtype.present(), [], True),
      ('', optional_qtype.missing(), [], False),
      ('', optional_qtype.present(), [UNIT], False),
      ('x', optional_qtype.present(), [], False),
      ('x', optional_qtype.present(), [NOTHING], True),
      ('x', optional_qtype.present(), [UNIT], True),
      ('x', optional_qtype.present(), [UNIT, UNIT], False),
      ('x', equal(p_x, p_x), [], False),
      ('x', equal(p_x, p_x), [NOTHING], False),
      ('x', equal(p_x, p_x), [UNIT], True),
      ('x', equal(p_x, p_x), [UNIT, UNIT], False),
      ('x, y', equal(p_x, p_x), [], False),
      ('x, y', equal(p_x, p_x), [NOTHING], False),
      ('x, y', equal(p_x, p_x), [UNIT], False),
      ('x, y', equal(p_x, p_x), [NOTHING, NOTHING], False),
      ('x, y', equal(p_x, p_x), [UNIT, NOTHING], True),
      ('x, y', equal(p_x, p_x), [NOTHING, UNIT], False),
      ('x, y', equal(p_x, p_x), [UNIT, UNIT], True),
      ('x, y', equal(p_x, p_x), [UNIT, UNIT, UNIT], False),
      ('*z', equal(p_z, p_z), [], True),
      ('*z', equal(p_z, p_z), [UNIT], True),
      ('*z', equal(p_z, p_z), [NOTHING], False),
      ('*z', equal(p_z, p_z), [UNIT, UNIT], True),
      ('*z', equal(p_z, p_z), [UNIT, NOTHING], False),
      ('x, *z', optional_qtype.present(), [], False),
      ('x, *z', optional_qtype.present(), [NOTHING], True),
      ('x, *z', optional_qtype.present(), [NOTHING, NOTHING], True),
      ('x, *z', equal(p_x, p_x), [], False),
      ('x, *z', equal(p_x, p_x), [NOTHING], False),
      ('x, *z', equal(p_x, p_x), [UNIT], True),
      ('x, *z', equal(p_x, p_x), [UNIT, NOTHING], True),
      ('x, *z', equal(p_x, p_x), [UNIT, UNIT], True),
  )
  def test_eval(self, sig_spec, condition_expr, input_qtypes, expected_result):
    prepared_conditional_expr = (
        generic_operator_qvalue._prepare_generic_overload_condition_expr(
            rl_abc.make_operator_signature(sig_spec),
            boxing.as_expr(condition_expr),
        )
    )
    actual_result = rl_abc.eval_expr(
        prepared_conditional_expr,
        dict(input_tuple_qtype=tuple_qtype.make_tuple_qtype(*input_qtypes)),
    )
    expected_result = optional_qtype.optional_unit(expected_result or None)
    self.assertEqual(
        rl_abc.eval_expr(
            prepared_conditional_expr,
            dict(input_tuple_qtype=tuple_qtype.make_tuple_qtype(*input_qtypes)),
        ).fingerprint,
        expected_result.fingerprint,
        f'{actual_result} != {expected_result}: {prepared_conditional_expr}',
    )

  def test_error_signature_with_default(self):
    sig = rl_abc.make_operator_signature(
        ('x, b=, a=', rl_abc.unspecified(), NOTHING)
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'operator overloads do not support parameters with default values:'
        ' b=unspecified, a=NOTHING',
    ):
      generic_operator_qvalue._prepare_generic_overload_condition_expr(
          sig, p_x
      )

  def test_error_condition_with_leaves(self):
    sig = rl_abc.make_operator_signature('x, y, z')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'condition cannot contain leaves: L.x, L.y; did you mean to use'
        ' placeholders?',
    ):
      generic_operator_qvalue._prepare_generic_overload_condition_expr(
          sig,
          rl_abc.bind_op(
              'core.make_tuple',
              rl_abc.leaf('y'),
              rl_abc.leaf('x'),
              rl_abc.placeholder('z'),
              rl_abc.leaf('input_tuple_qtype'),
          ),
      )

  def test_error_unexpected_parameters(self):
    sig = rl_abc.make_operator_signature('x, y, z')
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'condition contains unexpected parameters: P.u, P.w'
    ):
      generic_operator_qvalue._prepare_generic_overload_condition_expr(
          sig,
          rl_abc.bind_op(
              'core.make_tuple',
              rl_abc.placeholder('w'),
              rl_abc.placeholder('z'),
              rl_abc.placeholder('u'),
              rl_abc.leaf('input_tuple_qtype'),
          ),
      )

  def test_error_unexpected_output_qtype(self):
    sig = rl_abc.make_operator_signature('x')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected output for the overload condition is OPTIONAL_UNIT,'
        ' the actual output is QTYPE',
    ):
      generic_operator_qvalue._prepare_generic_overload_condition_expr(
          sig,
          rl_abc.literal(rl_abc.NOTHING),
      )


class GenericOperatorOverloadTest(parameterized.TestCase):

  def test_type(self):
    op = generic_operator_qvalue.GenericOperatorOverload(
        lambda_operator_qvalue.LambdaOperator(p_x), equal(p_x, rl_abc.QTYPE)
    )
    self.assertIsInstance(op, generic_operator_qvalue.GenericOperatorOverload)

  def test_error_unexpected_parameters(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'condition contains unexpected parameters: P.y'
    ):
      _ = generic_operator_qvalue.GenericOperatorOverload(
          lambda_operator_qvalue.LambdaOperator(p_x), equal(p_y, rl_abc.QTYPE)
      )

  def test_regression(self):
    _ = generic_operator_qvalue.GenericOperatorOverload(
        lambda_operator_qvalue.LambdaOperator(scalar_qtype.unit()),
        optional_qtype.present(),
    )  # no failure


class GenericOperatorTest(parameterized.TestCase):

  def test_type(self):
    op = generic_operator_qvalue.GenericOperator('foo.bar', signature='x')
    self.assertIsInstance(op, generic_operator_qvalue.GenericOperator)

  def test_format_signature_qtype(self):
    overload = generic_operator_qvalue.GenericOperatorOverload(
        lambda_operator_qvalue.LambdaOperator(
            'x, *y',
            p_x,
            name='generic_operator.test_format_signature_qtype.op_n',
        ),
        equal(p_x, p_x),
    )
    rl_abc.register_operator(
        'generic_operator.test_format_signature_qtype.op._1', overload
    )
    rl_abc.register_operator(
        'generic_operator.test_format_signature_qtype.op._2', overload
    )
    op = generic_operator_qvalue.GenericOperator(
        'generic_operator.test_format_signature_qtype.op', signature='x, y, *z'
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'ambiguous overloads:'
            ' generic_operator.test_format_signature_qtype.op._1,'
            ' generic_operator.test_format_signature_qtype.op._2'
            ' [x: INT32, y, *z: (INT32, -, INT32)]'
        ),
    ):
      _ = op(
          scalar_qtype.int32(1),
          p_x,
          scalar_qtype.int32(2),
          p_x,
          scalar_qtype.int32(3),
      )

  def test_status_not_ok_in_constructor(self):
    with self.assertRaises(ValueError):
      _ = generic_operator_qvalue.GenericOperator(
          '<invalid-namespace-name>', signature='x'
      )


if __name__ == '__main__':
  absltest.main()
