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
import warnings

from absl.testing import absltest
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.operators import operators_clib as _
from arolla.optools import decorators
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types

L = arolla_expr.LeafContainer()
M = arolla_expr.OperatorsContainer()
P = arolla_expr.PlaceholderContainer()

is_integral_scalar_qtype = arolla_types.LambdaOperator(
    M.qtype.common_qtype(P.x, arolla_types.INT64) == arolla_types.INT64,
    name='is_integral_scalar_qtype',
)

is_floating_point_scalar_qtype = arolla_types.LambdaOperator(
    M.qtype.common_qtype(P.x, arolla_types.FLOAT64) == arolla_types.FLOAT64,
    name='is_floating_point_scalar_qtype',
)

is_numeric_qtype = arolla_types.LambdaOperator(
    is_integral_scalar_qtype(M.qtype.get_scalar_qtype(P.x))
    | is_floating_point_scalar_qtype(M.qtype.get_scalar_qtype(P.x)),
    name='is_numeric_qtype',
)

core_presence_and = arolla_types.BackendOperator(
    'core.presence_and',
    'x, y',
    qtype_inference_expr=M.qtype.broadcast_qtype_like(P.y, P.x),
    qtype_constraints=[
        (
            M.qtype.get_scalar_qtype(P.x) != arolla_abc.NOTHING,
            'expected `x` to be a scalar based type, got {x}',
        ),
        (
            M.qtype.get_scalar_qtype(P.y) == arolla_types.UNIT,
            'expected `y` to be a unit based type, got {y}',
        ),
        (
            M.qtype.broadcast_qtype_like(P.y, P.x) != arolla_abc.NOTHING,
            'types `x` and `y` are not compatible: {x}, {y}',
        ),
    ],
)


@decorators.as_backend_operator(
    'math.add',
    qtype_inference_expr=M.qtype.common_qtype(P.x, P.y),
    qtype_constraints=[
        (is_numeric_qtype(P.x), '`x` needs to be a numeric type, got {x}'),
        (is_numeric_qtype(P.y), '`y` needs to be a numeric type, got {y}'),
        (
            M.qtype.common_qtype(P.x, P.y) != arolla_abc.NOTHING,
            'no common type for `x:{x}` and `y:{y}`',
        ),
    ],
)
def math_add(x, y):
  """Addition operator."""
  raise NotImplementedError  # implemented in backend


@decorators.as_backend_operator(
    'core.to_optional._scalar',
    qtype_inference_expr=M.qtype.broadcast_qtype_like(
        arolla_types.OPTIONAL_UNIT, P.x
    ),
    qtype_constraints=[(M.qtype.is_scalar_qtype(P.x), 'unsupported type {x}')],
)
def core_to_optional_scalar(x):
  raise NotImplementedError  # implemented in backend


@decorators.as_lambda_operator(
    'core.to_optional',
    qtype_constraints=[(
        M.qtype.get_scalar_qtype(P.x) != arolla_abc.NOTHING,
        'unsupported type {x}',
    )],
)
def core_to_optional(x):
  """Conversion to an optional type."""
  return decorators.dispatch[core_to_optional_scalar, M.core.identity](x)


@decorators.add_to_registry_as_overloadable('add_or_concat')
def add_or_concat(x, y):
  """Returns a sum for numbers, or a concatenation for strings."""
  raise NotImplementedError('implemented in overloads')


@decorators.add_to_registry_as_overload(
    overload_condition_expr=M.qtype.is_numeric_qtype(P.x)
)
@decorators.as_lambda_operator('add_or_concat.for_numerics')
def add_or_concat_for_numerics(x, y):
  return M.math.add(x, y)


@decorators.add_to_registry_as_overload(
    overload_condition_expr=(M.qtype.get_scalar_qtype(P.x) == arolla_types.TEXT)
)
@decorators.as_lambda_operator('add_or_concat.for_strings')
def add_or_concat_for_strings(x, y):
  return M.strings.join(x, y)


class DecoratorsTest(absltest.TestCase):

  def test_qtype_inference(self):
    with self.subTest('core_presence_and'):
      qtype_signatures = set(
          arolla_testing.detect_qtype_signatures(core_presence_and)
      )
      self.assertNotIn(
          (arolla_abc.QTYPE, arolla_types.UNIT, arolla_abc.QTYPE),
          qtype_signatures,
      )
      self.assertIn(
          (arolla_types.FLOAT32, arolla_types.UNIT, arolla_types.FLOAT32),
          qtype_signatures,
      )
      self.assertIn(
          (
              arolla_types.FLOAT32,
              arolla_types.OPTIONAL_UNIT,
              arolla_types.OPTIONAL_FLOAT32,
          ),
          qtype_signatures,
      )
      self.assertIn(
          (
              arolla_types.FLOAT32,
              arolla_types.ARRAY_UNIT,
              arolla_types.ARRAY_FLOAT32,
          ),
          qtype_signatures,
      )

    with self.subTest('math_add'):
      qtype_signatures = set(arolla_testing.detect_qtype_signatures(math_add))
      self.assertIn(
          (arolla_types.FLOAT32, arolla_types.FLOAT64, arolla_types.FLOAT64),
          qtype_signatures,
      )
      self.assertIn(
          (
              arolla_types.FLOAT32,
              arolla_types.OPTIONAL_FLOAT64,
              arolla_types.OPTIONAL_FLOAT64,
          ),
          qtype_signatures,
      )
      self.assertIn(
          (
              arolla_types.FLOAT32,
              arolla_types.ARRAY_FLOAT64,
              arolla_types.ARRAY_FLOAT64,
          ),
          qtype_signatures,
      )
      self.assertNotIn(
          (
              arolla_types.FLOAT32,
              arolla_types.INT32,
              arolla_types.ARRAY_FLOAT32,
          ),
          qtype_signatures,
      )
      self.assertNotIn(
          (
              arolla_types.FLOAT32,
              arolla_types.INT32,
              arolla_types.ARRAY_FLOAT64,
          ),
          qtype_signatures,
      )
      self.assertEqual(math_add.getdoc(), 'Addition operator.')

    with self.subTest('core_to_optional'):
      qtype_signatures = set(
          arolla_testing.detect_qtype_signatures(core_to_optional)
      )
      self.assertIn(
          (arolla_types.FLOAT32, arolla_types.OPTIONAL_FLOAT32),
          qtype_signatures,
      )
      self.assertIn(
          (arolla_types.OPTIONAL_FLOAT32, arolla_types.OPTIONAL_FLOAT32),
          qtype_signatures,
      )
      self.assertIn(
          (arolla_types.ARRAY_FLOAT32, arolla_types.ARRAY_FLOAT32),
          qtype_signatures,
      )
      self.assertNotIn((arolla_abc.QTYPE, arolla_abc.QTYPE), qtype_signatures)
      self.assertEqual(
          core_to_optional.getdoc(), 'Conversion to an optional type.'
      )

    with self.subTest('add_or_concat'):
      qtype_signatures = set(
          arolla_testing.detect_qtype_signatures(add_or_concat)
      )
      self.assertIn(
          (
              arolla_types.OPTIONAL_FLOAT32,
              arolla_types.FLOAT64,
              arolla_types.OPTIONAL_FLOAT64,
          ),
          qtype_signatures,
      )
      self.assertIn(
          (arolla_types.TEXT, arolla_types.ARRAY_TEXT, arolla_types.ARRAY_TEXT),
          qtype_signatures,
      )
      self.assertEqual(
          add_or_concat.getdoc(),
          'Returns a sum for numbers, or a concatenation for strings.',
      )

  def test_eval(self):
    with self.subTest('core_presence_and'):
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(
              core_presence_and(
                  arolla_types.float32(5.0), arolla_types.missing_unit()
              )
          ),
          arolla_types.optional_float32(None),
      )
    with self.subTest('math_add'):
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(
              math_add(
                  arolla_types.float32(5.0), arolla_types.optional_float32(1.0)
              )
          ),
          arolla_types.optional_float32(6.0),
      )
    with self.subTest('core_to_optional'):
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(core_to_optional(arolla_types.float32(5.0))),
          arolla_types.optional_float32(5.0),
      )
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(core_to_optional(arolla_types.array([5.0]))),
          arolla_types.array([5.0]),
      )
    with self.subTest('core_to_optional'):
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(core_to_optional(arolla_types.float32(5.0))),
          arolla_types.optional_float32(5.0),
      )
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(core_to_optional(arolla_types.array([5.0]))),
          arolla_types.array([5.0]),
      )
    with self.subTest('add_or_concat'):
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(
              add_or_concat(
                  arolla_types.optional_float32(5.0),
                  arolla_types.optional_float64(4.0),
              )
          ),
          arolla_types.optional_float64(9.0),
      )
      arolla_testing.assert_qvalue_allequal(
          arolla_types.eval_(
              add_or_concat(
                  arolla_types.text('left'), arolla_types.text('right')
              )
          ),
          arolla_types.text('leftright'),
      )

  def test_to_lower(self):
    with self.subTest('core_to_optional'):
      self.assertFalse(
          arolla_abc.to_lowest(
              core_to_optional(arolla_types.float32(5.0))
          ).is_literal
      )
      self.assertTrue(
          arolla_abc.to_lowest(
              core_to_optional(arolla_types.optional_float32(5.0))
          ).is_literal
      )

  def test_error_message(self):
    with self.subTest('core_presence_and'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape('expected `x` to be a scalar based type, got tuple<>'),
      ):
        _ = core_presence_and(arolla_types.tuple_(), arolla_types.tuple_())
    with self.subTest('math_add'):
      with self.assertRaisesRegex(
          ValueError, re.escape('`y` needs to be a numeric type, got BYTES')
      ):
        _ = math_add(arolla_types.int32(0), arolla_types.bytes_(b'foo'))

  def test_as_lambda_operator_signature_as_qvalue_regression(self):
    @decorators.as_lambda_operator(
        'test.lambda_operator_signature_as_qvalue_regression'
    )
    def op(result=3.1415):
      return result

    arolla_testing.assert_qvalue_allequal(
        arolla_types.eval_(op()), arolla_types.float32(3.1415)
    )

  def test_as_lambda_operator__accidental_placeholder_usage(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'unexpected placeholders in lambda operator definition: P.x, P.y',
    ):

      @decorators.as_lambda_operator('test.op')
      def _op(x, y):  # pylint: disable=unused-argument
        return P.x + P.y

  def test_as_lambda_operator__accidental_param_usage(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'P._as_lambda_operator_argument_x is missing in the list of lambda'
            ' parameters'
        ),
    ):

      @decorators.as_lambda_operator('test.op')
      def _op(x, y):
        return arolla_types.LambdaOperator('x, y', x + y)(x, y)

  def test_as_lambda_operator__unused_parameter_warning(self):
    def fn(_a, z, y, unused_b, *x):  # pylint: disable=invalid-name
      del _a, z, unused_b, x
      return y

    with self.assertWarnsRegex(
        decorators.LambdaUnusedParameterWarning,
        re.escape(
            "arolla.optools.as_lambda_operator('test.op', ...) a lambda"
            ' operator not using some of its parameters: x, z'
        ),
    ):
      decorators.as_lambda_operator('test.op')(fn)

    with warnings.catch_warnings():
      warnings.simplefilter('error')
      decorators.as_lambda_operator(
          'test.op', suppress_unused_parameter_warning=True
      )(fn)
      # ok if no errors

  def test_add_to_registry(self):
    @decorators.add_to_registry()
    @decorators.as_lambda_operator('decorator_test.add_to_registry.op1')
    def op1(x):
      return x

    @decorators.add_to_registry('decorator_test.add_to_registry.op2')
    @decorators.as_lambda_operator('foo.bar')
    def op2(x):
      return x

    self.assertIsInstance(op1, arolla_abc.RegisteredOperator)
    self.assertIsInstance(op2, arolla_abc.RegisteredOperator)
    self.assertEqual(op1.display_name, 'decorator_test.add_to_registry.op1')
    self.assertEqual(op2.display_name, 'decorator_test.add_to_registry.op2')

  def test_add_to_registry_unsafe_override_false(self):
    @decorators.as_lambda_operator('op')
    def op(x, unused_y):
      return x

    with warnings.catch_warnings():
      warnings.simplefilter('error')
      _ = decorators.add_to_registry(
          'decorator_test.add_to_registry_unsafe_override_false.op',
      )(op)
      with self.assertRaisesRegex(ValueError, re.escape('already exists')):
        _ = decorators.add_to_registry(
            'decorator_test.add_to_registry_unsafe_override_false.op',
        )(op)
      with self.assertRaisesRegex(ValueError, re.escape('already exists')):
        _ = decorators.add_to_registry(
            'decorator_test.add_to_registry_unsafe_override_false.op',
            unsafe_override=False,
        )(op)

  def test_add_to_registry_unsafe_override_true(self):
    @decorators.as_lambda_operator('op1')
    def op1(x, unused_y):
      return x

    @decorators.as_lambda_operator('op2')
    def op2(unused_x, y):
      return y

    with warnings.catch_warnings():
      warnings.simplefilter('error')
      expr = decorators.add_to_registry(
          'decorator_test.add_to_registry_unsafe_override.op',
          unsafe_override=True,
      )(op1)(L.x, L.y)
    self.assertEqual(
        arolla_types.eval_(expr, x=arolla_abc.QTYPE, y=arolla_abc.NOTHING),
        arolla_abc.QTYPE,
    )

    with warnings.catch_warnings():
      warnings.simplefilter('error')
      decorators.add_to_registry(
          'decorator_test.add_to_registry_unsafe_override.op',
          unsafe_override=True,
      )(op1)

    with self.assertWarnsRegex(
        RuntimeWarning,
        re.escape(
            'expr operator implementation was replaced in the registry:'
            ' decorator_test.add_to_registry_unsafe_override.op'
        ),
    ):
      decorators.add_to_registry(
          'decorator_test.add_to_registry_unsafe_override.op',
          unsafe_override=True,
      )(op2)
    self.assertEqual(
        arolla_types.eval_(expr, x=arolla_abc.QTYPE, y=arolla_abc.NOTHING),
        arolla_abc.NOTHING,
    )

  def test_as_lambda_operator_cleandoc(self):
    @decorators.as_lambda_operator('op')
    def op():
      # pylint: disable=g-doc-return-or-yield
      """Line1.

      Line2.
      """
      return arolla_types.unit()

    self.assertEqual(op.getdoc(), 'Line1.\n\nLine2.')

  def test_as_backend_operator_cleandoc(self):
    @decorators.as_backend_operator(
        'op', qtype_inference_expr=arolla_types.UNIT
    )
    def op():
      # pylint: disable=g-doc-return-or-yield
      """Line1.

      Line2.
      """
      pass

    self.assertEqual(op.getdoc(), 'Line1.\n\nLine2.')

  def test_add_to_registry_as_overloadable_cleandoc(self):
    @decorators.add_to_registry_as_overloadable(
        'decorator_test.test_add_to_registry_as_overloadable_cleandoc'
    )
    def op():
      # pylint: disable=g-doc-return-or-yield
      """Line1.

      Line2.
      """
      pass

    self.assertEqual(op.getdoc(), 'Line1.\n\nLine2.')

  def test_add_registery_as_overload_error_no_base_operator(self):
    @decorators.as_lambda_operator('op')
    def op(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        LookupError, "found no corresponding generic operator: ''"
    ):
      decorators.add_to_registry_as_overload(
          'test_no_base_operator', overload_condition_expr=P.x
      )(op)

    with self.assertRaisesWithLiteralMatch(
        LookupError,
        "found no corresponding generic operator: 'test_no_base_operator'",
    ):
      decorators.add_to_registry_as_overload(
          'test_no_base_operator.op', overload_condition_expr=P.x
      )(op)

  def test_add_registery_as_overload_error_base_operator_non_generic(self):
    @decorators.as_lambda_operator('op')
    def op(x):
      return x

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected M.qtype.is_scalar_qtype to be a generic operator, found'
        ' arolla.types.qvalue.lambda_operator_qvalues.LambdaOperator',
    ):
      decorators.add_to_registry_as_overload(
          'qtype.is_scalar_qtype._2', overload_condition_expr=P.x
      )(arolla_types.LambdaOperator(P.x))

  def test_add_registery_as_overload_error_base_operator_name_mismatch(self):
    arolla_abc.register_operator(
        'decorator_test.test_add_registery_as_overload_error_base_operator'
        '_name_mismatch.op',
        arolla_types.GenericOperator('foo.bar', signature='x'),
    )

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "expected a generic operator with name 'decorator_test.test"
        "_add_registery_as_overload_error_base_operator_name_mismatch.op',"
        " found: 'foo.bar'",
    ):
      decorators.add_to_registry_as_overload(
          'decorator_test.test_add_registery_as_overload_error_base_operator'
          '_name_mismatch.op.overload',
          overload_condition_expr=P.x,
      )(arolla_types.LambdaOperator(P.x))

  def test_as_plain_lambda(self):
    @decorators.as_lambda_operator(
        'op', qtype_constraints=[(P.x == P.x, 'true')]
    )
    def op(x):
      return x

    @decorators.as_lambda_operator('plain_op')
    def plain_op(x):
      return x

    self.assertIsInstance(op, arolla_types.RestrictedLambdaOperator)
    self.assertNotIsInstance(plain_op, arolla_types.RestrictedLambdaOperator)
    self.assertIsInstance(plain_op, arolla_types.LambdaOperator)

  def test_as_lambda_operator_signature_with_experimental_aux_policy(self):
    @decorators.as_lambda_operator('op1')
    def op1():
      return None

    @decorators.as_lambda_operator('op2', experimental_aux_policy='policy-name')
    def op2():
      return None

    self.assertEqual(arolla_abc.get_operator_signature(op1).aux_policy, '')
    self.assertEqual(
        arolla_abc.get_operator_signature(op2).aux_policy, 'policy-name'
    )

  def test_as_backend_operator_signature_with_experimental_aux_policy(self):
    @decorators.as_backend_operator(
        'op1', qtype_inference_expr=arolla_abc.QTYPE
    )
    def op1():
      return None

    @decorators.as_backend_operator(
        'op2',
        qtype_inference_expr=arolla_abc.QTYPE,
        experimental_aux_policy='policy-name',
    )
    def op2():
      return None

    self.assertEqual(arolla_abc.get_operator_signature(op1).aux_policy, '')
    self.assertEqual(
        arolla_abc.get_operator_signature(op2).aux_policy, 'policy-name'
    )

  def test_add_to_registry_as_overloadable_signature_with_experimental_aux_policy(
      self,
  ):
    @decorators.add_to_registry_as_overloadable(
        'decorator_test.test_add_to_registry_as_overloadable_signature_with_aux_policy.op1'
    )
    def op1():
      return None

    @decorators.add_to_registry_as_overloadable(
        'decorator_test.test_add_to_registry_as_overloadable_signature_with_aux_policy.op2',
        experimental_aux_policy='policy-name',
    )
    def op2():
      return None

    self.assertEqual(arolla_abc.get_operator_signature(op1).aux_policy, '')
    self.assertEqual(
        arolla_abc.get_operator_signature(op2).aux_policy, 'policy-name'
    )

  def test_doc_is_correct_when_decorating_operator_class(self):
    @decorators.add_to_registry_as_overloadable('overloadable_operator_class')
    @decorators.as_backend_operator('operator_class', qtype_inference_expr=P.x)
    def overloadable_backend_operator(x, y):
      """Here is a docstring."""
      del x, y
      raise NotImplementedError('implemented in backend')

    self.assertEqual(
        overloadable_backend_operator.getdoc(),
        'Here is a docstring.',
    )

if __name__ == '__main__':
  absltest.main()
