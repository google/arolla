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

"""Tests for arolla.types.qvalue.generic_operator_qvalues."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import boxing
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes
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


class PrepareOverloadConditionExprTest(parameterized.TestCase):

  @parameterized.parameters(
      ('', optional_qtypes.present(), [], True),
      ('', optional_qtypes.missing(), [], False),
      ('', optional_qtypes.present(), [UNIT], False),
      ('x', optional_qtypes.present(), [], False),
      ('x', optional_qtypes.present(), [NOTHING], True),
      ('x', optional_qtypes.present(), [UNIT], True),
      ('x', optional_qtypes.present(), [UNIT, UNIT], False),
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
      ('x, *z', optional_qtypes.present(), [], False),
      ('x, *z', optional_qtypes.present(), [NOTHING], True),
      ('x, *z', optional_qtypes.present(), [NOTHING, NOTHING], True),
      ('x, *z', equal(p_x, p_x), [], False),
      ('x, *z', equal(p_x, p_x), [NOTHING], False),
      ('x, *z', equal(p_x, p_x), [UNIT], True),
      ('x, *z', equal(p_x, p_x), [UNIT, NOTHING], True),
      ('x, *z', equal(p_x, p_x), [UNIT, UNIT], True),
  )
  def test_eval(self, sig_spec, condition_expr, input_qtypes, expected_result):
    prepared_conditional_expr = (
        generic_operator_qvalues._prepare_generic_overload_condition_expr(
            arolla_abc.make_operator_signature(sig_spec),
            boxing.as_expr(condition_expr),
        )
    )
    actual_result = arolla_abc.eval_expr(
        prepared_conditional_expr,
        input_tuple_qtype=tuple_qtypes.make_tuple_qtype(*input_qtypes),
    )
    expected_result = optional_qtypes.optional_unit(expected_result or None)
    self.assertEqual(
        arolla_abc.eval_expr(
            prepared_conditional_expr,
            input_tuple_qtype=tuple_qtypes.make_tuple_qtype(*input_qtypes),
        ).fingerprint,
        expected_result.fingerprint,
        f'{actual_result} != {expected_result}: {prepared_conditional_expr}',
    )

  def test_error_signature_with_default(self):
    sig = arolla_abc.make_operator_signature(
        ('x, b=, a=', arolla_abc.unspecified(), NOTHING)
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'operator overloads do not support parameters with default values:'
        ' b=unspecified, a=NOTHING',
    ):
      generic_operator_qvalues._prepare_generic_overload_condition_expr(
          sig, p_x
      )

  def test_error_condition_with_leaves(self):
    sig = arolla_abc.make_operator_signature('x, y, z')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'condition cannot contain leaves: L.x, L.y; did you mean to use'
        ' placeholders?',
    ):
      generic_operator_qvalues._prepare_generic_overload_condition_expr(
          sig,
          arolla_abc.bind_op(
              'core.make_tuple',
              arolla_abc.leaf('y'),
              arolla_abc.leaf('x'),
              arolla_abc.placeholder('z'),
              arolla_abc.leaf('input_tuple_qtype'),
          ),
      )

  def test_error_unexpected_parameters(self):
    sig = arolla_abc.make_operator_signature('x, y, z')
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'condition contains unexpected parameters: P.u, P.w'
    ):
      generic_operator_qvalues._prepare_generic_overload_condition_expr(
          sig,
          arolla_abc.bind_op(
              'core.make_tuple',
              arolla_abc.placeholder('w'),
              arolla_abc.placeholder('z'),
              arolla_abc.placeholder('u'),
              arolla_abc.leaf('input_tuple_qtype'),
          ),
      )

  def test_error_unexpected_output_qtype(self):
    sig = arolla_abc.make_operator_signature('x')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected output for the overload condition is OPTIONAL_UNIT,'
        ' the actual output is QTYPE',
    ):
      generic_operator_qvalues._prepare_generic_overload_condition_expr(
          sig,
          arolla_abc.literal(arolla_abc.NOTHING),
      )


class GenericOperatorOverloadTest(parameterized.TestCase):

  def test_type(self):
    op = generic_operator_qvalues.GenericOperatorOverload(
        lambda_operator_qvalues.LambdaOperator(p_x),
        equal(p_x, arolla_abc.QTYPE),
    )
    self.assertIsInstance(op, generic_operator_qvalues.GenericOperatorOverload)

  def test_error_unexpected_parameters(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'condition contains unexpected parameters: P.y'
    ):
      _ = generic_operator_qvalues.GenericOperatorOverload(
          lambda_operator_qvalues.LambdaOperator(p_x),
          equal(p_y, arolla_abc.QTYPE),
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
    overload = generic_operator_qvalues.GenericOperatorOverload(
        lambda_operator_qvalues.LambdaOperator(
            'x, *y',
            p_x,
            name='generic_operator.test_format_signature_qtype.op_n',
        ),
        equal(p_x, p_x),
    )
    arolla_abc.register_operator(
        'generic_operator.test_format_signature_qtype.op._1', overload
    )
    arolla_abc.register_operator(
        'generic_operator.test_format_signature_qtype.op._2', overload
    )
    op = generic_operator_qvalues.GenericOperator(
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
          scalar_qtypes.int32(1),
          p_x,
          scalar_qtypes.int32(2),
          p_x,
          scalar_qtypes.int32(3),
      )

  def test_status_not_ok_in_constructor(self):
    with self.assertRaises(ValueError):
      _ = generic_operator_qvalues.GenericOperator(
          '<invalid-namespace-name>', signature='x'
      )


if __name__ == '__main__':
  absltest.main()
