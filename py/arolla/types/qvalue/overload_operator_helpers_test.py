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

"""Tests for arolla.types.qvalue.helpers."""

import functools

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.testing import testing
from arolla.types.qtype import boxing
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes
from arolla.types.qvalue import overload_operator_helpers


p_x = arolla_abc.placeholder('x')
p_y = arolla_abc.placeholder('y')
p_z = arolla_abc.placeholder('z')

NOTHING = arolla_abc.NOTHING
UNIT = scalar_qtypes.UNIT
OPTIONAL_UNIT = optional_qtypes.OPTIONAL_UNIT
INT32 = scalar_qtypes.INT32
int64 = scalar_qtypes.int64
FLOAT32 = scalar_qtypes.FLOAT32
QTYPE = arolla_abc.QTYPE

equal = arolla_abc.lookup_operator('core.equal')
get_field_count = arolla_abc.lookup_operator('qtype.get_field_count')
get_field_qtype = arolla_abc.lookup_operator('qtype.get_field_qtype')
leaf_a = arolla_abc.leaf('leaf_a')
leaf_b = arolla_abc.leaf('leaf_b')
input_tuple_qtype = arolla_abc.leaf('input_tuple_qtype')
make_tuple = arolla_abc.lookup_operator('core.make_tuple')
missing = optional_qtypes.missing()
placeholder_a = arolla_abc.placeholder('a')
placeholder_b = arolla_abc.placeholder('b')
presence_and = arolla_abc.lookup_operator('core.presence_and')
presence_or = arolla_abc.lookup_operator('core.presence_or')
present = optional_qtypes.present()
qtype_of = arolla_abc.lookup_operator('qtype.qtype_of')
slice_tuple_qtype = arolla_abc.lookup_operator('qtype.slice_tuple_qtype')


class GetInputTupleLengthValidationExprsTest(parameterized.TestCase):

  @parameterized.parameters(
      ('', (present, missing, missing, missing)),
      ('x', (missing, present, missing, missing)),
      ('x, y', (missing, missing, present, missing)),
      ('*args', (present, present, present, present)),
      ('x, *ys', (missing, present, present, present)),
  )
  def test_valid(self, str_signature, expected_output):
    signature = arolla_abc.make_operator_signature(
        str_signature, as_qvalue=boxing.as_qvalue
    )
    condition_expr = (
        overload_operator_helpers.get_input_tuple_length_validation_exprs(
            signature
        )
    )
    combined_expr = functools.reduce(
        presence_and, condition_expr, arolla_abc.literal(present)
    )
    for i, expected_output in enumerate(expected_output):
      annotated_combined_expr = arolla_abc.sub_leaves(
          combined_expr,
          input_tuple_qtype=arolla_abc.literal(
              tuple_qtypes.make_tuple_qtype(*[QTYPE for _ in range(i)])
          ),
      )
      actual_output = arolla_abc.eval_expr(annotated_combined_expr)
      testing.assert_qvalue_equal_by_fingerprint(actual_output, expected_output)


class CheckSignatureOfOverloadTest(parameterized.TestCase):

  @parameterized.parameters(
      '',
      'x, y, z',
      '*args',
      'x, *args',
  )
  def test_valid(self, signature):
    signature = arolla_abc.make_operator_signature(signature)
    _ = overload_operator_helpers.check_signature_of_overload(signature)

  @parameterized.parameters(
      (('x=', present),),
      (('x, y=, z=', missing, present),),
      (('x=, *args', present),),
  )
  def test_has_defaults(self, signature):
    signature = arolla_abc.make_operator_signature(signature)
    with self.assertRaisesRegex(
        ValueError,
        'operator overloads do not support parameters with default values',
    ):
      _ = overload_operator_helpers.check_signature_of_overload(signature)


class SubstitutePlaceholdersInConditionExprTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          'a',
          equal(placeholder_a, INT32),
          equal(get_field_qtype(input_tuple_qtype, int64(0)), INT32),
      ),
      (
          'a, *b',
          equal(get_field_count(placeholder_b), 1),
          equal(
              get_field_count(
                  slice_tuple_qtype(input_tuple_qtype, int64(1), int64(-1))
              ),
              1,
          ),
      ),
  )
  def test_substitute(self, signature, condition_expr, expected_expr):
    signature = arolla_abc.make_operator_signature(signature)
    actual_expr, _ = (
        overload_operator_helpers.substitute_placeholders_in_condition_expr(
            signature, condition_expr
        )
    )
    testing.assert_expr_equal_by_fingerprint(actual_expr, expected_expr)

  @parameterized.parameters(
      ('', optional_qtypes.present(), [], True, []),
      ('', optional_qtypes.missing(), [], False, []),
      ('', optional_qtypes.present(), [UNIT], True, []),
      ('x', optional_qtypes.present(), [], True, []),
      ('x', optional_qtypes.present(), [NOTHING], True, []),
      ('x', optional_qtypes.present(), [UNIT], True, []),
      ('x', optional_qtypes.present(), [UNIT, UNIT], True, []),
      ('x', equal(p_x, p_x), [], True, [0]),
      ('x', equal(p_x, p_x), [NOTHING], True, [0]),
      ('x', equal(p_x, p_x), [UNIT], True, [0]),
      ('x', equal(p_x, p_x), [UNIT, UNIT], True, [0]),
      ('x, y', equal(p_x, p_x), [], True, [0]),
      ('x, y', equal(p_x, p_x), [NOTHING, NOTHING], True, [0]),
      ('x, y', equal(p_x, p_x), [UNIT, UNIT], True, [0]),
      ('x, y', equal(p_x, p_x), [UNIT, UNIT, UNIT], True, [0]),
      ('x, y', equal(p_x, p_y), [], True, [0, 1]),
      ('x, y', equal(p_x, p_y), [UNIT], False, [0, 1]),
      ('x, y', equal(p_x, p_y), [UNIT, UNIT], True, [0, 1]),
      ('x, y', equal(p_x, p_y), [UNIT, INT32], False, [0, 1]),
      ('x, y', equal(p_x, p_y), [UNIT, UNIT, UNIT], True, [0, 1]),
      ('x, y', equal(p_x, p_y), [UNIT, INT32, UNIT], False, [0, 1]),
      ('*z', equal(p_z, p_z), [], True, [0]),
      ('*z', equal(p_z, p_z), [NOTHING], True, [0]),
      ('*z', equal(p_z, p_z), [UNIT, NOTHING], True, [0]),
  )
  def test_eval(
      self,
      sig_spec,
      condition_expr,
      input_qtypes,
      expected_result,
      expected_used_params,
  ):
    prepared_conditional_expr, used_params = (
        overload_operator_helpers.substitute_placeholders_in_condition_expr(
            arolla_abc.make_operator_signature(sig_spec),
            boxing.as_expr(condition_expr),
        )
    )
    actual_result = arolla_abc.eval_expr(
        prepared_conditional_expr,
        input_tuple_qtype=tuple_qtypes.make_tuple_qtype(*input_qtypes),
    )
    expected_result = optional_qtypes.optional_unit(expected_result or None)
    testing.assert_qvalue_equal_by_fingerprint(actual_result, expected_result)
    self.assertEqual(used_params, expected_used_params)

  def test_error_condition_with_leaves(self):
    sig = arolla_abc.make_operator_signature('x, y, z')
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'condition cannot contain leaves: L.x, L.y; did you mean to use'
        ' placeholders?',
    ):
      overload_operator_helpers.substitute_placeholders_in_condition_expr(
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
      overload_operator_helpers.substitute_placeholders_in_condition_expr(
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
      overload_operator_helpers.substitute_placeholders_in_condition_expr(
          sig,
          arolla_abc.literal(arolla_abc.NOTHING),
      )


class GetOverloadConditionReadinessExprTest(parameterized.TestCase):

  @parameterized.parameters(
      ('', [], (), present),
      ('a', [], (), present),
      ('*a', [], (), present),
      ('a, b', [0], (INT32,), present),
      ('a, b', [0], (INT32, NOTHING), present),
      ('a, b', [0], (NOTHING, INT32), missing),
      ('a, b', [0], (INT32, INT32, INT32), present),
      ('a, *bs', [0], (NOTHING, INT32), missing),
      ('a, *bs', [0], (INT32, INT32), present),
      ('a, *bs', [1], (NOTHING, INT32), present),
      ('a, *bs', [1], (NOTHING, INT32, NOTHING), missing),
      ('a, *bs', [1], (NOTHING, INT32, INT32), present),
      ('a, *bs', [0, 1], (INT32, INT32, INT32), present),
      ('a, *bs', [0, 1], (INT32, INT32, NOTHING), missing),
      ('a, *bs', [0, 1], (NOTHING, INT32, INT32), missing),
  )
  def test_eval(self, signature, required_args, input_qtypes, expected_output):
    signature = arolla_abc.make_operator_signature(signature)
    readiness_expr = (
        overload_operator_helpers.get_overload_condition_readiness_expr(
            signature, required_args
        )
    )
    readiness_expr = arolla_abc.sub_leaves(
        readiness_expr,
        **{
            input_tuple_qtype.leaf_key: arolla_abc.literal(
                tuple_qtypes.make_tuple_qtype(*input_qtypes)
            )
        },
    )
    actual_output = arolla_abc.eval_expr(readiness_expr)
    testing.assert_qvalue_equal_by_fingerprint(expected_output, actual_output)


if __name__ == '__main__':
  absltest.main()
