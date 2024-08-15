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

"""Tests for M.core.map."""

import re
from typing import Iterable

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

L = arolla.L
M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@arolla.optools.as_lambda_operator(
    'test.scalar_add',
    qtype_constraints=[
        constraints.expect_scalar(P.x),
        constraints.expect_scalar(P.y),
    ],
)
def scalar_add(x, y):
  return M.math.add(x, y)


@arolla.optools.as_lambda_operator('test.map_scalar_add')
def map_scalar_add(x, y):
  return M.core.map(scalar_add, x, y)


@arolla.optools.as_lambda_operator(
    'test.scalar_or_optional_add',
    qtype_constraints=[
        constraints.expect_scalar_or_optional(P.x),
        constraints.expect_scalar_or_optional(P.y),
    ],
)
def scalar_or_optional_add(x, y):
  return M.math.add(x, y)


@arolla.optools.as_lambda_operator(
    'test.to_optional_float32',
)
def to_optional_float32(x):
  return M.core.cast(x, arolla.OPTIONAL_FLOAT32)


@arolla.optools.as_lambda_operator('test.cast_to_float32')
def cast_values_to_float32(x):
  return M.core.cast_values(x, arolla.FLOAT32)


@arolla.optools.as_lambda_operator(
    'test.add_n',
)
def add_n(*args):
  return M.core.reduce_tuple(M.math.add, *args)


@arolla.optools.as_lambda_operator('test.is_scalar_or_optional_qtype')
def is_scalar_or_optional_qtype(qtype):
  return M.qtype.is_scalar_qtype(qtype) | M.qtype.is_optional_qtype(qtype)


@arolla.optools.as_lambda_operator(
    'test.optional_scalar_add_n',
    qtype_constraints=[(
        M.seq.all(
            M.seq.map(
                is_scalar_or_optional_qtype,
                M.qtype.get_field_qtypes(P.args),
            )
        ),
        'only optionals are supported',
    )],
)
def optional_scalar_add_n(*args):
  return M.core.apply_varargs(add_n, *args)


def has_array(qtypes: Iterable[arolla.QType]) -> bool:
  return any(arolla.types.is_array_qtype(x) for x in qtypes)


class CoreMapTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('operator_on_optionals', scalar_or_optional_add, M.math.add),
      (
          'lambda_with_literal_inside',
          to_optional_float32,
          cast_values_to_float32,
      ),
      (
          'variadic_operator',
          optional_scalar_add_n,
          add_n,
      ),
  )
  def test_qtype_signatures(self, mapper, reference_operator):

    @arolla.optools.as_lambda_operator('test.map_scalar_or_optional_add')
    def wrapped_map(*args):
      return M.core.apply_varargs(M.core.map, mapper, *args)

    self.assertEqual(
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(
                wrapped_map, max_arity=3
            )
        ),
        frozenset(
            filter(
                has_array,
                pointwise_test_utils.detect_qtype_signatures(
                    reference_operator, max_arity=3
                ),
            )
        ),
    )

  def test_qtype_errors(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('at least one array required, got (INT32)')
    ):
      M.core.map(M.math.neg, 1)

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected an EXPR_OPERATOR, got op: tuple<INT32,INT32>'),
    ):
      M.core.map((1, 2), M.math.neg)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'incorrect number of dependencies passed to an operator node:'
            ' expected 2 but got 1'
        )
        + '.*'
        + re.escape(
            'while deducing output type for math.add in core.map operator'
        ),
    ):
      M.core.map(M.math.add, arolla.array([1, 2]))

    with self.assertRaisesRegex(ValueError, 'op must be a literal'):
      arolla.eval(M.core.map(L.op, arolla.array([1, 2])), op=M.math.neg)

    # Mappers on scalars are currently not supported on Expr level.
    with self.assertRaisesRegex(
        ValueError, 'found no supported qtype signatures'
    ):
      frozenset(pointwise_test_utils.detect_qtype_signatures(map_scalar_add))

    # Mixing arrays and dense_arrays is currently prohibited. Note, that the
    # underlying implementation allows to support them easily.
    with self.assertRaisesRegex(
        ValueError,
        (
            'all array arguments must have compatible shapes, got ARRAY_INT32'
            ' and DENSE_ARRAY_INT32'
        ),
    ):
      M.core.map(M.math.add, arolla.dense_array([1, 2]), arolla.array([1, 2]))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_binary_op(self, array_factory):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(
                scalar_or_optional_add,
                array_factory([56, 55, 54]),
                array_factory([1, 2, None]),
            )
        ),
        array_factory([57, 57, None]),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_broadcast_scalar(self, array_factory):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(
                scalar_or_optional_add,
                56,
                array_factory([1, 2, None]),
            )
        ),
        array_factory([57, 58, None]),
    )
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(scalar_or_optional_add, array_factory([56, 55, 54]), 1)
        ),
        array_factory([57, 56, 55]),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_broadcast_missing_scalar(self, array_factory):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(
                scalar_or_optional_add,
                array_factory([56, 55, 54]),
                arolla.optional_int32(None),
            )
        ),
        array_factory([None, None, None], value_qtype=arolla.INT32),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_op_with_literal_argument(self, array_factory):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(
                M.core.cast,
                array_factory([1, 2, 3]),
                arolla.OPTIONAL_FLOAT32,
                # TODO: Support default arg values.
                # implicit_only=
                False,
            )
        ),
        array_factory([1.0, 2.0, 3.0]),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_variadic_op(self, array_factory):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(
                M.strings.join,
                'The quick ',
                array_factory(['brown', None, 'pink']),
                ' ',
                L.object,
                ' jumps over ',
                L.obstacle,
            ),
            object=array_factory(['fox', 'cat', 'panther']),
            obstacle='the lazy dog',
        ),
        array_factory([
            'The quick brown fox jumps over the lazy dog',
            None,
            'The quick pink panther jumps over the lazy dog',
        ]),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_op_with_default_args(self, array_factory):

    @arolla.optools.as_lambda_operator(
        'test.add_3_with_default',
        qtype_constraints=[
            constraints.expect_scalar_or_optional(P.x),
            constraints.expect_scalar_or_optional(P.y),
            constraints.expect_scalar_or_optional(P.z),
        ],
    )
    def add_3_with_default(x, y, z=1):
      return M.math.add(M.math.add(x, y), z)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(
            M.core.map(
                add_3_with_default,
                array_factory([1, 2, 3]),
                array_factory([1, 2, None]),
                1,
            ),
        ),
        array_factory([3, 5, None]),
    )

    # TODO: Support default arg values.
    with self.assertRaisesRegex(
        ValueError,
        (
            'incorrect number of dependencies passed to an operator node:'
            ' expected 3 but got 2'
        ),
    ):
      M.core.map(
          add_3_with_default,
          array_factory([1, 2, 3]),
          array_factory([1, 2, None]),
      )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_runtime_errors(self, array_factory):
    with self.assertRaisesRegex(ValueError, "array size doesn't match: 3 vs 2"):
      arolla.eval(
          M.core.map(
              M.math.add, array_factory([1, 2, 3]), array_factory([1, 2])
          ),
      )

    # TODO: Should we support M.core.with_assertion directly,
    # without need to wrap it?
    @arolla.optools.as_lambda_operator('test.with_optional_assertion')
    def with_optional_assertion(value, condition, message):
      """M.core.with_assertion that accepts optional error message."""
      return M.core.with_assertion(value, condition, message | 'error')

    with self.assertRaisesRegex(ValueError, 'error in the second row'):
      arolla.eval(
          M.core.map(
              with_optional_assertion,
              array_factory([1, 2, 3]),
              array_factory([True, None, None], value_qtype=arolla.UNIT),
              array_factory([
                  'error in the first row',
                  'error in the second row',
                  'error in the third row',
              ]),
          ),
      )

    with self.assertRaisesRegex(ValueError, 'error somewhere'):
      arolla.eval(
          M.core.map(
              with_optional_assertion,
              array_factory([1, 2, 3]),
              array_factory([True, None, None], value_qtype=arolla.UNIT),
              'error somewhere',
          ),
      )


if __name__ == '__main__':
  absltest.main()
