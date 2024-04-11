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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.optools import optools as arolla_optools
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types

constraints = arolla_optools.constraints

M = arolla_expr.OperatorsContainer()
P = arolla_expr.PlaceholderContainer()


class ConstraintsTest(parameterized.TestCase):

  def test_name_type_msg(self):
    self.assertEqual(constraints.name_type_msg(P.abc), 'abc: {abc}')

  def test_variadic_name_type_msg(self):
    self.assertEqual(constraints.variadic_name_type_msg(P.abc), '*abc: {*abc}')

  def test_common_qtype_expr(self):
    arolla_testing.assert_expr_equal_by_fingerprint(
        constraints.common_qtype_expr(P.abc, P.ijk),
        M.qtype.common_qtype(P.abc, P.ijk),
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        constraints.common_qtype_expr(P.abc, P.ijk, P.uvw, P.xyz),
        M.qtype.common_qtype(
            M.qtype.common_qtype(M.qtype.common_qtype(P.abc, P.ijk), P.uvw),
            P.xyz,
        ),
    )

  def test_broadcast_qtype_expr(self):
    arolla_testing.assert_expr_equal_by_fingerprint(
        constraints.broadcast_qtype_expr([P.abc], P.ijk),
        M.qtype.broadcast_qtype_like(P.abc, P.ijk),
    )
    arolla_testing.assert_expr_equal_by_fingerprint(
        constraints.broadcast_qtype_expr([P.abc, P.ijk, P.uvw], P.xyz),
        M.qtype.broadcast_qtype_like(
            M.qtype.broadcast_qtype_like(
                M.qtype.broadcast_qtype_like(P.abc, P.ijk), P.uvw
            ),
            P.xyz,
        ),
    )

  @parameterized.parameters(
      (constraints.expect_qtype, arolla_types.INT32),
      (constraints.expect_qtype, arolla_types.ARRAY_INT32),
      (constraints.expect_shape, arolla_types.ScalarShape()),
      (constraints.expect_shape, arolla_types.OptionalScalarShape()),
      (constraints.expect_shape, arolla_types.ArrayShape(0)),
      (constraints.has_presence_type, arolla_types.int32(1)),
      (constraints.has_presence_type, arolla_types.array_int32([1])),
      (constraints.has_scalar_type, arolla_types.int32(1)),
      (constraints.has_scalar_type, arolla_types.array_int32([1])),
      (constraints.expect_scalar, arolla_types.int32(1)),
      (constraints.expect_scalar_or_optional, arolla_types.int32(1)),
      (constraints.expect_scalar_or_optional, arolla_types.optional_float64(1)),
      (constraints.expect_array, arolla_types.array_int32([1])),
      (constraints.expect_array_scalar_or_optional, arolla_types.int32(1)),
      (
          constraints.expect_array_scalar_or_optional,
          arolla_types.optional_float64(1),
      ),
      (
          constraints.expect_array_scalar_or_optional,
          arolla_types.array_int32([1]),
      ),
      (constraints.expect_array_or_unspecified, arolla_types.array_int32([1])),
      (constraints.expect_array_or_unspecified, arolla_abc.unspecified()),
      (constraints.expect_array_shape, arolla_types.DenseArrayShape(0)),
      (constraints.expect_array_shape, arolla_types.ArrayShape(0)),
      (constraints.expect_dense_array, arolla_types.dense_array_int32([1])),
      (constraints.expect_sequence, arolla_types.Sequence(1)),
      (constraints.expect_scalar_boolean, arolla_types.boolean(True)),
      (constraints.expect_scalar_bytes, arolla_types.bytes_(b'foo')),
      (constraints.expect_scalar_text, arolla_types.text('bar')),
      (constraints.expect_scalar_integer, arolla_types.int32(1)),
      (constraints.expect_scalar_integer, arolla_types.int64(1)),
      (constraints.expect_scalar_float, arolla_types.float32(1)),
      (constraints.expect_scalar_float, arolla_types.float64(1)),
      (constraints.expect_unit, arolla_types.unit()),
      (constraints.expect_unit, arolla_types.missing()),
      (constraints.expect_boolean, arolla_types.boolean(True)),
      (constraints.expect_boolean, arolla_types.optional_boolean(None)),
      (constraints.expect_integer, arolla_types.int32(1)),
      (constraints.expect_integer, arolla_types.optional_int32(None)),
      (constraints.expect_units, arolla_types.unit()),
      (constraints.expect_units, arolla_types.missing()),
      (constraints.expect_units, arolla_types.array_unit([None, True])),
      (constraints.expect_booleans, arolla_types.array_boolean([None, True])),
      (constraints.expect_byteses, arolla_types.array_bytes([None, b'foo'])),
      (constraints.expect_texts, arolla_types.array_text([None, 'bar'])),
      (
          constraints.expect_texts_or_byteses,
          arolla_types.array_text([None, 'bar']),
      ),
      (
          constraints.expect_texts_or_byteses,
          arolla_types.array_bytes([None, b'foo']),
      ),
      (constraints.expect_integers, arolla_types.array_int32([None, 1])),
      (constraints.expect_floats, arolla_types.array_float32([None, 1.5])),
      (constraints.expect_numerics, arolla_types.array_int32([None, 1])),
      (constraints.expect_numerics, arolla_types.array_float32([None, 1.5])),
      (constraints.expect_dict_key_types, arolla_types.array_int32([None, 1])),
      (constraints.expect_dict_key_types, arolla_types.array_uint64([None, 1])),
      (
          constraints.expect_dict_key_types,
          arolla_types.array_text([None, 'bar']),
      ),
      (
          constraints.expect_dict_key_types,
          arolla_types.array_bytes([None, b'foo']),
      ),
      (
          constraints.expect_dict_key_types,
          arolla_types.array_boolean([None, True]),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_types.array_int32([None, 1]),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_types.array_uint64([None, 1]),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_types.array_text([None, 'bar']),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_types.array_bytes([None, b'foo']),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_types.array_boolean([None, True]),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_abc.unspecified(),
      ),
  )
  def test_unary_predicate_accept(self, unary_predicate_factory, value):
    @arolla_optools.as_lambda_operator(
        'op.name', qtype_constraints=[unary_predicate_factory(P.arg)]
    )
    def _op(arg):
      return arg

    _ = _op(value)  # no exception

  @parameterized.parameters(
      (
          constraints.expect_qtype,
          arolla_types.int32(1),
          'expected QTYPE, got arg: INT32',
      ),
      (
          constraints.expect_shape,
          arolla_types.int32(1),
          'expected a shape, got arg: INT32',
      ),
      (
          constraints.has_presence_type,
          arolla_types.tuple_(),
          'no presence type for arg: tuple<>',
      ),
      (
          constraints.has_scalar_type,
          arolla_types.tuple_(),
          'expected a type storing scalar(s), got arg: tuple<>',
      ),
      (
          constraints.expect_scalar,
          arolla_types.optional_int32(1),
          'expected a scalar type, got arg: OPTIONAL_INT32',
      ),
      (
          constraints.expect_scalar_or_optional,
          arolla_types.dense_array_float32([1.0]),
          (
              'expected a scalar or optional scalar type, got arg: '
              'DENSE_ARRAY_FLOAT32'
          ),
      ),
      (
          constraints.expect_array,
          arolla_types.int32(1),
          'expected an array type, got arg: INT32',
      ),
      (
          constraints.expect_array_scalar_or_optional,
          arolla_types.tuple_(),
          'expected an array, scalar or optional type, got arg: tuple<>',
      ),
      (
          constraints.expect_array_or_unspecified,
          arolla_types.optional_unit(True),
          'expected an array type, got arg: OPTIONAL_UNIT',
      ),
      (
          constraints.expect_array_shape,
          arolla_types.eval_(arolla_types.ScalarShape()),
          'expected an array shape, got arg: SCALAR_SHAPE',
      ),
      (
          constraints.expect_dense_array,
          arolla_types.array_int32([1]),
          'expected a dense_array type, got arg: ARRAY_INT32',
      ),
      (
          constraints.expect_sequence,
          arolla_types.dense_array_float32([1]),
          'expected a sequence type, got arg: DENSE_ARRAY_FLOAT32',
      ),
      (
          constraints.expect_scalar_boolean,
          arolla_types.optional_boolean(True),
          'expected a boolean scalar, got arg: OPTIONAL_BOOLEAN',
      ),
      (
          constraints.expect_scalar_bytes,
          arolla_types.text('foo'),
          'expected a bytes scalar, got arg: TEXT',
      ),
      (
          constraints.expect_scalar_text,
          arolla_types.bytes_(b'bar'),
          'expected a text scalar, got arg: BYTES',
      ),
      (
          constraints.expect_scalar_integer,
          arolla_types.float32(1.5),
          'expected an integer scalar, got arg: FLOAT32',
      ),
      (
          constraints.expect_scalar_float,
          arolla_types.int32(1),
          'expected a floating-point scalar, got arg: INT32',
      ),
      (
          constraints.expect_unit,
          arolla_types.boolean(True),
          'expected a unit scalar or optional, got arg: BOOLEAN',
      ),
      (
          constraints.expect_boolean,
          arolla_types.array_boolean([True]),
          'expected a boolean scalar or optional, got arg: ARRAY_BOOLEAN',
      ),
      (
          constraints.expect_integer,
          arolla_types.array_int32([1]),
          'expected an integer scalar or optional, got arg: ARRAY_INT32',
      ),
      (
          constraints.expect_units,
          arolla_types.int32(1),
          'expected units, got arg: INT32',
      ),
      (
          constraints.expect_booleans,
          arolla_types.int32(1),
          'expected booleans, got arg: INT32',
      ),
      (
          constraints.expect_byteses,
          arolla_types.array_text(['foo']),
          'expected bytes or array of bytes, got arg: ARRAY_TEXT',
      ),
      (
          constraints.expect_texts,
          arolla_types.array_bytes([b'bar']),
          'expected texts or array of texts, got arg: ARRAY_BYTES',
      ),
      (
          constraints.expect_texts_or_byteses,
          arolla_types.array_int32([15]),
          'expected texts/byteses or corresponding array, '
          + 'got arg: ARRAY_INT32',
      ),
      (
          constraints.expect_integers,
          arolla_types.array_float32([1.5]),
          'expected integers, got arg: ARRAY_FLOAT32',
      ),
      (
          constraints.expect_floats,
          arolla_types.array_int32([1]),
          'expected floating-points, got arg: ARRAY_INT32',
      ),
      (
          constraints.expect_numerics,
          arolla_types.array_bytes([b'bar']),
          'expected numerics, got arg: ARRAY_BYTES',
      ),
      (
          constraints.expect_dict_key_types,
          arolla_types.array_float32([1.5]),
          (
              'expect value type to be compatible with dict keys, got arg:'
              ' ARRAY_FLOAT32'
          ),
      ),
      (
          constraints.expect_dict_key_types_or_unspecified,
          arolla_types.array_float32([1.5]),
          (
              'expect value type to be compatible with dict keys, got arg:'
              ' ARRAY_FLOAT32'
          ),
      ),
  )
  def test_unary_predicate_reject(
      self, unary_predicate_factory, value, error_message
  ):
    @arolla_optools.as_lambda_operator(
        'op.name', qtype_constraints=[unary_predicate_factory(P.arg)]
    )
    def _op(arg):
      return arg

    with self.assertRaisesRegex(ValueError, re.escape(error_message)):
      _ = _op(value)

  def test_expect_implicit_cast_compatible(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            constraints.expect_implicit_cast_compatible(P.a, P.b)
        ],
    )
    def _op2(a, b):
      return M.core.make_tuple(a, b)

    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            constraints.expect_implicit_cast_compatible(P.a, P.b, P.c)
        ],
    )
    def _op3(a, b, c):
      return M.core.make_tuple(a, b, c)

    with self.subTest('accept'):
      _ = _op2(
          arolla_types.int32(1), arolla_types.array_int32([1])
      )  # no exception
      _ = _op3(
          arolla_types.float32(1.5),
          arolla_types.array_float32([1.5]),
          arolla_types.optional_float32(None),
      )  # no exception

    with self.subTest('reject'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape('incompatible types a: INT32 and b: ARRAY_FLOAT32'),
      ):
        _ = _op2(arolla_types.int32(1), arolla_types.array_float32([1.5]))
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'incompatible types a: INT32, b: ARRAY_FLOAT32, and c: UNIT'
          ),
      ):
        _ = _op3(
            arolla_types.int32(1),
            arolla_types.array_float32([1.5]),
            arolla_types.unit(),
        )

  def test_expect_broadcast_compatible(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[constraints.expect_broadcast_compatible(P.a, P.b)],
    )
    def _op2(a, b):
      return M.core.make_tuple(a, b)

    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            constraints.expect_broadcast_compatible(P.a, P.b, P.c)
        ],
    )
    def _op3(a, b, c):
      return M.core.make_tuple(a, b, c)

    with self.subTest('accept'):
      _ = _op2(
          arolla_types.int32(1), arolla_types.array_float32([1.5])
      )  # no exception
      _ = _op3(
          arolla_types.int32(1),
          arolla_types.array_float32([1.5]),
          arolla_types.optional_bytes(None),
      )  # no exception

    with self.subTest('reject'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape('broadcast incompatible types a: INT32 and b: tuple<>'),
      ):
        _ = _op2(arolla_types.int32(1), arolla_types.tuple_())
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'broadcast incompatible types a: INT32, b: ARRAY_FLOAT32, '
              'and c: DENSE_ARRAY_INT32'
          ),
      ):
        _ = _op3(
            arolla_types.int32(1),
            arolla_types.array_float32([1.5]),
            arolla_types.dense_array_int32([1]),
        )

  def test_expect_shape_compatible(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[constraints.expect_shape_compatible(P.a, P.b)],
    )
    def _op2(a, b):
      return M.core.make_tuple(a, b)

    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[constraints.expect_shape_compatible(P.a, P.b, P.c)],
    )
    def _op3(a, b, c):
      return M.core.make_tuple(a, b, c)

    with self.subTest('accept'):
      _ = _op2(
          arolla_types.array_int32([1, 2]),
          arolla_types.array_float32([1.5, None, 3.5]),
      )  # no exception
      _ = _op2(arolla_types.int32(1), arolla_types.int32(2))  # no exception
      _ = _op2(arolla_types.int32(1), arolla_types.float32(2.0))  # no exception
      _ = _op2(
          arolla_types.optional_int32(1), arolla_types.optional_int32(2)
      )  # no exception
      _ = _op2(
          arolla_types.dense_array_int32([1, 2]),
          arolla_types.dense_array_float32([1.5, None, 3.5]),
      )  # no exception
      _ = _op3(
          arolla_types.array_int32([1, 2]),
          arolla_types.array_float32([1.5, None, 3.5]),
          arolla_types.array_float32([1.5, None, 3.5]),
      )  # no exception

    with self.subTest('reject'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'expected types with compatible shape, got a: INT32 and b:'
              ' ARRAY_INT32'
          ),
      ):
        _ = _op2(arolla_types.int32(1), arolla_types.array_int32([1]))
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'expected types with compatible shape, got a: ARRAY_INT32, b:'
              ' ARRAY_INT32, and c: DENSE_ARRAY_INT32'
          ),
      ):
        _ = _op3(
            arolla_types.array_int32([1]),
            arolla_types.array_int32([1]),
            arolla_types.dense_array_int32([1]),
        )

  def test_expect_edge(self):
    @arolla_optools.as_lambda_operator(
        'op.name', qtype_constraints=[*constraints.expect_edge(P.edge)]
    )
    def _op(edge):
      return edge

    with self.subTest('accepts_index_edge'):
      _ = _op(arolla_types.ArrayEdge.from_mapping([1], 2))  # no exception

    with self.subTest('rejects_wrong_type'):
      with self.assertRaisesRegex(
          ValueError, re.escape('expected an edge, got edge: ARRAY_INT32')
      ):
        _ = _op(arolla_types.array_int32([1, 2]))

  def test_expect_edge_child_side_compatible(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            *constraints.expect_edge(P.edge, child_side_param=P.child)
        ],
    )
    def _op(edge, child):
      return M.core.make_tuple(edge, child)

    with self.subTest('accepts_proper_type'):
      # no exception.
      _ = _op(
          arolla_types.ArrayEdge.from_mapping([1], 2),
          arolla_types.array_int32([1, 2]),
      )
      _ = _op(
          arolla_types.DenseArrayEdge.from_mapping([1], 2),
          arolla_types.dense_array_int32([1, 2]),
      )
      _ = _op(
          arolla_types.ArrayToScalarEdge(1), arolla_types.array_int32([1, 2])
      )
      _ = _op(arolla_types.ScalarToScalarEdge(), arolla_types.optional_int32(2))
      _ = _op(arolla_types.ScalarToScalarEdge(), arolla_types.int32(2))

    with self.subTest('rejects_wrong_type'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'child: ARRAY_INT32 is not compatible with child-side '
              'of edge: DENSE_ARRAY_EDGE'
          ),
      ):
        _ = _op(
            arolla_types.DenseArrayEdge.from_mapping([1], 2),
            arolla_types.array_int32([1, 2]),
        )

  def test_expect_edge_parent_side_compatible(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            *constraints.expect_edge(P.edge, parent_side_param=P.parent)
        ],
    )
    def _op(edge, parent):
      return M.core.make_tuple(edge, parent)

    with self.subTest('accepts_proper_type'):
      # no exception.
      _ = _op(
          arolla_types.ArrayEdge.from_mapping([1], 2),
          arolla_types.array_int32([1]),
      )
      _ = _op(
          arolla_types.DenseArrayEdge.from_mapping([1], 2),
          arolla_types.dense_array_int32([1]),
      )
      _ = _op(arolla_types.ArrayToScalarEdge(1), arolla_types.optional_int32(1))
      _ = _op(arolla_types.ArrayToScalarEdge(1), arolla_types.int32(1))
      _ = _op(arolla_types.ScalarToScalarEdge(), arolla_types.optional_int32(1))
      _ = _op(arolla_types.ScalarToScalarEdge(), arolla_types.int32(1))

    with self.subTest('rejects_wrong_type'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'parent: ARRAY_INT32 is not compatible with parent-side '
              'of edge: ARRAY_TO_SCALAR_EDGE'
          ),
      ):
        _ = _op(
            arolla_types.ArrayToScalarEdge(1), arolla_types.array_int32([1])
        )

  def test_expect_key_to_row_dict_and_compatible_keys(self):
    @arolla_optools.as_lambda_operator(
        'op_keys.name',
        qtype_constraints=[
            *constraints.expect_key_to_row_dict(
                P.key_to_row_dict, keys_compatible_with_param=P.keys
            )
        ],
    )
    def _op_keys(key_to_row_dict, keys):
      return M.core.make_tuple(key_to_row_dict, keys)

    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            *constraints.expect_key_to_row_dict(P.key_to_row_dict)
        ],
    )
    def _op(key_to_row_dict):
      return M.core.make_tuple(key_to_row_dict)

    with self.subTest('accepts_proper_type'):
      # no exception
      _ = _op(
          M.dict._make_key_to_row_dict(arolla_types.dense_array_text(['bar']))
      )
      _ = _op(M.dict._make_key_to_row_dict(arolla_types.dense_array_int32([1])))
      _ = _op_keys(
          M.dict._make_key_to_row_dict(arolla_types.dense_array_int32([1])),
          arolla_types.int32(1),
      )
      _ = _op_keys(
          M.dict._make_key_to_row_dict(arolla_types.dense_array_int32([1])),
          arolla_types.array_int32([1]),
      )

    with self.subTest('rejects_wrong_type'):
      with self.assertRaisesRegex(
          ValueError,
          (
              'expected a key type compatible with key_to_row_dict: DICT_INT32,'
              ' got keys: INT64'
          ),
      ):
        _ = _op_keys(
            M.dict._make_key_to_row_dict(arolla_types.dense_array_int32([1])),
            arolla_types.int64(1),
        )

      with self.assertRaisesRegex(
          ValueError,
          (
              'expected a key type compatible with key_to_row_dict: DICT_INT32,'
              ' got keys: ARRAY_FLOAT32'
          ),
      ):
        _ = _op_keys(
            M.dict._make_key_to_row_dict(arolla_types.dense_array_int32([1])),
            arolla_types.array_float32([1.0]),
        )

      with self.assertRaisesRegex(
          ValueError,
          'expected a key to row dict, got key_to_row_dict: DENSE_ARRAY_INT32',
      ):
        _ = _op(arolla_types.dense_array_int32([1]))

      with self.assertRaisesRegex(
          ValueError, 'expected a key to row dict, got key_to_row_dict: INT32'
      ):
        _ = _op(arolla_types.int32(1))

      with self.assertRaisesRegex(
          ValueError,
          'expected a key to row dict, got key_to_row_dict: DENSE_ARRAY_INT32',
      ):
        _ = _op_keys(
            arolla_types.dense_array_int32([1]),
            arolla_types.array_float32([1.0]),
        )

  def test_expect_dict(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            *constraints.expect_dict(P.d, keys_compatible_with_param=P.keys)
        ],
    )
    def _op(d, keys):
      return M.core.make_tuple(d, keys)

    with self.subTest('accepts_proper_type'):
      # no exception
      _ = _op(
          M.dict.make(
              arolla_types.dense_array_text('foo'),
              arolla_types.dense_array_text('bar'),
          ),
          'foo',
      )
      # even with casting
      _ = _op(
          M.dict.make(
              arolla_types.dense_array_int64([1]),
              arolla_types.dense_array_text(['bar']),
          ),
          arolla_types.array_int32([1]),
      )

    with self.subTest('rejects_wrong_type'):
      with self.assertRaisesRegex(
          ValueError,
          'expect to be a dict, got d: INT64',
      ):
        _ = _op(
            arolla_types.int64(1),
            M.dict.make(
                arolla_types.dense_array_int32([1]),
                arolla_types.dense_array_text(['bar']),
            ),
        )

      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'expected a key type compatible with d: Dict<INT32,TEXT> keys,'
              ' got keys: INT64'
          ),
      ):
        _ = _op(
            M.dict.make(
                arolla_types.dense_array_int32([1]),
                arolla_types.dense_array_text(['bar']),
            ),
            arolla_types.int64(1),
        )

  def test_expect_qtype_in(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            constraints.expect_qtype_in(
                P.a,
                (
                    arolla_types.INT32,
                    arolla_types.ARRAY_FLOAT32,
                ),
            )
        ],
    )
    def _op(a):
      return a

    with self.subTest('accepts'):
      _ = _op(arolla_types.int32(1))  # no exception
      _ = _op(arolla_types.array_float32([1]))  # no exception

    with self.subTest('rejects'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'expected one of [ARRAY_FLOAT32, INT32], got a: DENSE_ARRAY_INT64'
          ),
      ):
        _ = _op(arolla_types.dense_array_int64([1]))

  def test_expect_scalar_qtype_in(self):
    @arolla_optools.as_lambda_operator(
        'op.name',
        qtype_constraints=[
            constraints.expect_scalar_qtype_in(
                P.a,
                (
                    arolla_types.INT32,
                    arolla_types.FLOAT32,
                ),
            )
        ],
    )
    def _op(a):
      return a

    with self.subTest('accepts'):
      _ = _op(arolla_types.int32(1))  # no exception
      _ = _op(arolla_types.dense_array_float32([1]))  # no exception

    with self.subTest('rejects'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'expected the scalar qtype to be one of [FLOAT32, INT32], got a:'
              ' DENSE_ARRAY_TEXT'
          ),
      ):
        _ = _op(arolla_types.dense_array_text(['foo']))


if __name__ == '__main__':
  absltest.main()
