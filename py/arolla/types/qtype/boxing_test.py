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

"""Tests for arolla.types.qtype.boxing."""

import inspect
import itertools
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import boxing
from arolla.types.qtype import dense_array_qtypes
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes
import numpy


def iter_range(n):
  """Returns a iterable object that can be scanned only once."""
  return iter(range(n))


class BoxingTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, scalar_qtypes.int32(1)),
      (1.0, scalar_qtypes.float32(1)),
      ((), tuple_qtypes.make_tuple_qvalue()),
      (
          (1, 2.0, True),
          tuple_qtypes.make_tuple_qvalue(
              scalar_qtypes.int32(1),
              scalar_qtypes.float32(2.0),
              scalar_qtypes.boolean(True),
          ),
      ),
      ([1, 2.0, 3], array_qtypes.array_float32([1.0, 2.0, 3.0])),
      ([None, True, False], array_qtypes.array_boolean([None, True, False])),
      (None, arolla_abc.unspecified()),
      (arolla_abc.QTYPE, arolla_abc.QTYPE),
      (numpy.bool_(True), scalar_qtypes.boolean(True)),
      (numpy.int32(1), scalar_qtypes.int32(1)),
      (numpy.int64(1), scalar_qtypes.int64(1)),
      (numpy.uint64(1), scalar_qtypes.uint64(1)),
      (numpy.float32(1), scalar_qtypes.float32(1)),
      (numpy.float64(1), scalar_qtypes.float64(1)),
      (numpy.bytes_(b'foo'), scalar_qtypes.bytes_(b'foo')),
      (numpy.str_('bar'), scalar_qtypes.text('bar')),
      (
          numpy.array([1, 2, 3], numpy.float64),
          array_qtypes.array_float64([1, 2, 3]),
      ),
  )
  def testAsQValue(self, py_value, expected_qvalue):
    qvalue = boxing.as_qvalue(py_value)
    self.assertEqual(
        qvalue.fingerprint,
        expected_qvalue.fingerprint,
        f'{qvalue} != {expected_qvalue}',
    )

  def testAsQValue_TypeError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'unsupported type: object'
    ):
      _ = boxing.as_qvalue(object())

  @parameterized.parameters(
      (1, scalar_qtypes.int32(1)),
      (1.0, scalar_qtypes.float32(1)),
      ((), tuple_qtypes.make_tuple_qvalue()),
      (
          ((1, 2.0), arolla_abc.leaf('x')),
          arolla_abc.bind_op(
              'core.make_tuple',
              tuple_qtypes.make_tuple_qvalue(
                  scalar_qtypes.int32(1),
                  scalar_qtypes.float32(2.0),
              ),
              arolla_abc.leaf('x'),
          ),
      ),
      ([None, True, False], array_qtypes.array_boolean([None, True, False])),
      (None, arolla_abc.unspecified()),
      (arolla_abc.QTYPE, arolla_abc.QTYPE),
      (
          numpy.array([1, 2, 3], numpy.float64),
          array_qtypes.array_float64([1, 2, 3]),
      ),
      (numpy.bool_(True), scalar_qtypes.boolean(True)),
      (numpy.int32(1), scalar_qtypes.int32(1)),
      (numpy.int64(1), scalar_qtypes.int64(1)),
      (numpy.uint64(1), scalar_qtypes.uint64(1)),
      (numpy.float32(1), scalar_qtypes.float32(1)),
      (numpy.float64(1), scalar_qtypes.float64(1)),
      (numpy.bytes_(b'foo'), scalar_qtypes.bytes_(b'foo')),
      (numpy.str_('bar'), scalar_qtypes.text('bar')),
  )
  def testAsQValueOrExpr(self, py_value, expected_qvalue_or_expr):
    qvalue = boxing.as_qvalue_or_expr(py_value)
    self.assertEqual(
        qvalue.fingerprint,
        expected_qvalue_or_expr.fingerprint,
        f'{qvalue} != {expected_qvalue_or_expr}',
    )

  def testAsQValueOrExpr_TypeError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'unsupported type: object'
    ):
      _ = boxing.as_qvalue_or_expr(object())

  @parameterized.parameters(
      (1, arolla_abc.literal(scalar_qtypes.int32(1))),
      (1.0, arolla_abc.literal(scalar_qtypes.float32(1))),
      ((), arolla_abc.literal(tuple_qtypes.make_tuple_qvalue())),
      (
          ((1, 2.0), arolla_abc.leaf('x')),
          arolla_abc.bind_op(
              'core.make_tuple',
              tuple_qtypes.make_tuple_qvalue(
                  scalar_qtypes.int32(1),
                  scalar_qtypes.float32(2.0),
              ),
              arolla_abc.leaf('x'),
          ),
      ),
      (
          [None, True, False],
          arolla_abc.literal(array_qtypes.array_boolean([None, True, False])),
      ),
      (None, arolla_abc.literal(arolla_abc.unspecified())),
      (arolla_abc.QTYPE, arolla_abc.literal(arolla_abc.QTYPE)),
      (
          numpy.array([1, 2, 3], numpy.float64),
          arolla_abc.literal(array_qtypes.array_float64([1, 2, 3])),
      ),
  )
  def testAsExpr(self, py_value, expected_expr):
    expr = boxing.as_expr(py_value)
    self.assertEqual(
        expr.fingerprint,
        expected_expr.fingerprint,
        f'{expr} != {expected_expr}',
    )

  def testAsExpr_TypeError(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('unable to create a literal from: [L.y]')
    ):
      _ = boxing.as_expr([arolla_abc.leaf('y')])

  def testLiteral(self):
    x = boxing.literal(1)
    self.assertTrue(x.is_literal)
    self.assertEqual(str(x), '1')
    self.assertEqual(repr(x), '1')
    assert x.qvalue is not None
    self.assertEqual(x.qvalue.fingerprint, boxing.as_qvalue(1).fingerprint)

  def testBindOp(self):
    self.assertEqual(
        arolla_abc.aux_bind_op(
            'core.identity', (arolla_abc.leaf('y'), 2.5, ['foo'])
        ).fingerprint,
        arolla_abc.bind_op(
            'core.identity',
            arolla_abc.bind_op(
                'core.make_tuple',
                arolla_abc.leaf('y'),
                scalar_qtypes.float32(2.5),
                array_qtypes.array_text(['foo']),
            ),
        ).fingerprint,
    )

  def testBindKwargs(self):
    self.assertTrue(
        arolla_abc.aux_bind_op('math.add', 1, y=2).equals(
            arolla_abc.bind_op(
                'math.add', scalar_qtypes.int32(1), scalar_qtypes.int32(2)
            )
        )
    )

  def testEval(self):
    self.assertEqual(
        boxing.eval_((arolla_abc.leaf('y'), (2.5, ['foo'])), y=1).fingerprint,
        boxing.tuple_(1, (2.5, ['foo'])).fingerprint,
    )

  def testEvalIntermediateResultsNotAllInMemory(self):
    """Will OOM if all intermediate results will be materialized."""

    node = arolla_abc.leaf('a')
    num_ops = 1_000
    array_size = 1_000
    for _ in range(num_ops - 1):
      node = arolla_abc.bind_op('math.add', node, arolla_abc.leaf('a'))
    a = boxing.array([5.0] * array_size, value_qtype=scalar_qtypes.FLOAT64)
    _ = boxing.eval_(node, a=a)

  @parameterized.parameters(
      # Builtin types.
      (False, scalar_qtypes.BOOLEAN),
      (True, scalar_qtypes.BOOLEAN),
      (b'', scalar_qtypes.BYTES),
      (b'foo', scalar_qtypes.BYTES),
      ('', scalar_qtypes.TEXT),
      ('bar', scalar_qtypes.TEXT),
      (0, scalar_qtypes.INT32),
      (1, scalar_qtypes.INT32),
      (0.0, scalar_qtypes.FLOAT32),
      (3.14, scalar_qtypes.FLOAT32),
      # Numpy types.
      (numpy.bool_(False), scalar_qtypes.BOOLEAN),
      (numpy.int32(1), scalar_qtypes.INT32),
      (numpy.int64(1), scalar_qtypes.INT64),
      (numpy.uint64(1), scalar_qtypes.UINT64),
      (numpy.float32(1.0), scalar_qtypes.FLOAT32),
      (numpy.float64(1.0), scalar_qtypes.FLOAT64),
      (numpy.bytes_(b'foo'), scalar_qtypes.BYTES),
      (numpy.str_('foo'), scalar_qtypes.TEXT),
      # QValues.
      (scalar_qtypes.float64(1.0), scalar_qtypes.FLOAT64),
  )
  def testDeduceScalarQType(self, value, expected_value_type):
    actual_value_qtype = boxing._deduce_scalar_qtype_from_value(value)
    self.assertEqual(actual_value_qtype, expected_value_type)

  @parameterized.parameters(
      arolla_abc.leaf('x'),
      arolla_abc.literal(scalar_qtypes.int32(1)),
  )
  def testDeduceScalarQType_TypeError(self, value):
    with self.assertRaises(TypeError):
      _ = boxing._deduce_scalar_qtype_from_value(value)

  @parameterized.parameters(
      # Builtin types.
      ([None, True], scalar_qtypes.BOOLEAN),
      ([None, b'foo'], scalar_qtypes.BYTES),
      ([None, 'bar'], scalar_qtypes.TEXT),
      ([None, 1], scalar_qtypes.INT32),
      ([None, 3.14], scalar_qtypes.FLOAT32),
      ([None, 1, 3.14], scalar_qtypes.FLOAT32),
      # Numpy scalar types.
      ([None, numpy.bool_(False)], scalar_qtypes.BOOLEAN),
      ([None, numpy.int32(1)], scalar_qtypes.INT32),
      ([None, numpy.int64(1)], scalar_qtypes.INT64),
      ([None, numpy.uint64(1)], scalar_qtypes.UINT64),
      ([None, numpy.float32(1.0)], scalar_qtypes.FLOAT32),
      ([None, numpy.float64(1.0)], scalar_qtypes.FLOAT64),
      ([None, numpy.bytes_(b'foo')], scalar_qtypes.BYTES),
      ([None, numpy.str_('foo')], scalar_qtypes.TEXT),
      # Numpy arrays.
      (numpy.array([False], numpy.bool_), scalar_qtypes.BOOLEAN),
      (numpy.array([1], numpy.int32), scalar_qtypes.INT32),
      (numpy.array([1], numpy.int64), scalar_qtypes.INT64),
      (numpy.array([1], numpy.uint64), scalar_qtypes.UINT64),
      (numpy.array([1], numpy.float32), scalar_qtypes.FLOAT32),
      (numpy.array([1], numpy.float64), scalar_qtypes.FLOAT64),
      (numpy.array([b'foo'], numpy.bytes_), scalar_qtypes.BYTES),
      (numpy.array(['foo'], numpy.str_), scalar_qtypes.TEXT),
      # QValues.
      ([None, scalar_qtypes.int64(2**34)], scalar_qtypes.INT64),
      ([None, scalar_qtypes.float64(1.0)], scalar_qtypes.FLOAT64),
      # Implicit cast.
      (
          [None, scalar_qtypes.float64(1), scalar_qtypes.float32(1)],
          scalar_qtypes.FLOAT64,
      ),
  )
  def testDeduceQTypeFromValues(self, values, expected_value_qtype):
    actual_value_qtype = boxing._deduce_value_qtype_from_values(values)
    self.assertEqual(actual_value_qtype, expected_value_qtype)

  @parameterized.parameters(
      [[]],
      [[None]],
      [[False, b'']],
  )
  def testDeduceValueQType_ValueError(self, values):
    with self.assertRaises(ValueError):
      _ = boxing._deduce_value_qtype_from_values(values)

  @parameterized.parameters(
      [[[]]],
      [[object()]],
      [[arolla_abc.leaf('x')]],
      [[arolla_abc.literal(scalar_qtypes.int32(1))]],
  )
  def testDeduceValueQType_TypeError(self, values):
    with self.assertRaises(TypeError):
      _ = boxing._deduce_value_qtype_from_values(values)

  @parameterized.parameters(*dense_array_qtypes.DENSE_ARRAY_QTYPES)
  def testDenseArrayFromValues_QType(self, dense_array_qtype):
    self.assertEqual(
        boxing.dense_array([], dense_array_qtype.value_qtype).qtype,
        dense_array_qtype,
    )
    self.assertEqual(
        boxing.dense_array(
            [],
            optional_qtypes.make_optional_qtype(dense_array_qtype.value_qtype),
        ).qtype,
        dense_array_qtype,
    )

  def testDenseArrayFromValues_WithGenerator(self):
    self.assertEqual(
        boxing.dense_array(iter_range(2)).fingerprint,
        dense_array_qtypes.dense_array_int32([0, 1]).fingerprint,
    )
    self.assertEqual(
        boxing.dense_array(iter_range(2), scalar_qtypes.INT64).fingerprint,
        dense_array_qtypes.dense_array_int64([0, 1]).fingerprint,
    )

  @parameterized.parameters(*dense_array_qtypes.DENSE_ARRAY_QTYPES)
  def testDenseArrayFromDenseArray_SameQType(self, dense_array_qtype):
    da = boxing.dense_array([], dense_array_qtype.value_qtype)
    self.assertEqual(boxing.dense_array(da).qtype, dense_array_qtype)
    self.assertEqual(
        boxing.dense_array(da, dense_array_qtype.value_qtype).qtype,
        dense_array_qtype,
    )

  @parameterized.parameters(
      *itertools.combinations(
          [
              dense_array_qtypes.DENSE_ARRAY_INT32,
              dense_array_qtypes.DENSE_ARRAY_INT64,
          ],
          2,
      ),
      *itertools.combinations(
          [
              dense_array_qtypes.DENSE_ARRAY_WEAK_FLOAT,
              dense_array_qtypes.DENSE_ARRAY_FLOAT32,
              dense_array_qtypes.DENSE_ARRAY_FLOAT64,
          ],
          2,
      ),
  )
  def testDenseArrayFromDenseArray_ImplicitCasting(self, from_qtype, to_qtype):
    da = boxing.dense_array([], from_qtype.value_qtype)
    self.assertEqual(
        boxing.dense_array(da, to_qtype.value_qtype).qtype, to_qtype
    )

  def testDenseArrayFromValues_NumpyArrayPerformance(self):
    self.assertEqual(
        boxing.dense_array(numpy.arange(10**6, dtype=numpy.int32)).qtype,
        dense_array_qtypes.DENSE_ARRAY_INT32,
    )
    self.assertEqual(
        boxing.dense_array(
            numpy.arange(10**6, dtype=numpy.int32), scalar_qtypes.INT64
        ).qtype,
        dense_array_qtypes.DENSE_ARRAY_INT64,
    )

  def testDenseArrayFromValues_DenseArrayPerformance(self):
    da_int32 = boxing.dense_array(numpy.arange(10**6, dtype=numpy.int32))
    self.assertEqual(
        boxing.dense_array(da_int32).qtype,
        dense_array_qtypes.DENSE_ARRAY_INT32,
    )
    self.assertEqual(
        boxing.dense_array(da_int32, scalar_qtypes.INT64).qtype,
        dense_array_qtypes.DENSE_ARRAY_INT64,
    )

  @parameterized.parameters(*array_qtypes.ARRAY_QTYPES)
  def testArrayFromValues_QType(self, array_qtype):
    self.assertEqual(
        boxing.array([], array_qtype.value_qtype).qtype, array_qtype
    )
    self.assertEqual(
        boxing.array(
            [], optional_qtypes.make_optional_qtype(array_qtype.value_qtype)
        ).qtype,
        array_qtype,
    )

  def testArrayFromValues_WithGenerator(self):
    self.assertEqual(
        boxing.array(iter_range(2)).fingerprint,
        array_qtypes.array_int32([0, 1]).fingerprint,
    )
    self.assertEqual(
        boxing.array(iter_range(2), scalar_qtypes.INT64).fingerprint,
        array_qtypes.array_int64([0, 1]).fingerprint,
    )

  @parameterized.parameters(*array_qtypes.ARRAY_QTYPES)
  def testArrayFromArray_SameQType(self, array_qtype):
    qb = boxing.array([], array_qtype.value_qtype)
    self.assertEqual(boxing.array(qb).qtype, array_qtype)
    self.assertEqual(
        boxing.array(qb, array_qtype.value_qtype).qtype, array_qtype
    )

  @parameterized.parameters(
      *itertools.combinations(
          [array_qtypes.ARRAY_INT32, array_qtypes.ARRAY_INT64], 2
      ),
      *itertools.combinations(
          [
              array_qtypes.ARRAY_WEAK_FLOAT,
              array_qtypes.ARRAY_FLOAT32,
              array_qtypes.ARRAY_FLOAT64,
          ],
          2,
      ),
  )
  def testArrayFromArray_ImplicitCasting(self, from_qtype, to_qtype):
    qb = boxing.array([], from_qtype.value_qtype)
    self.assertEqual(boxing.array(qb, to_qtype.value_qtype).qtype, to_qtype)

  def testArrayFromValues_NumpyArrayPerformance(self):
    self.assertEqual(
        boxing.array(numpy.arange(10**6, dtype=numpy.int32)).qtype,
        array_qtypes.ARRAY_INT32,
    )
    self.assertEqual(
        boxing.array(
            numpy.arange(10**6, dtype=numpy.int32), scalar_qtypes.INT64
        ).qtype,
        array_qtypes.ARRAY_INT64,
    )

  def testArrayFromValues_ArrayPerformance(self):
    qb_int32 = boxing.array(numpy.arange(10**6, dtype=numpy.int32))
    self.assertEqual(boxing.array(qb_int32).qtype, array_qtypes.ARRAY_INT32)
    self.assertEqual(
        boxing.array(qb_int32, scalar_qtypes.INT64).qtype,
        array_qtypes.ARRAY_INT64,
    )

  def testMakeTuple(self):
    self.assertEqual(
        boxing.tuple_(1, 2.0).fingerprint,
        tuple_qtypes.make_tuple_qvalue(
            boxing.as_qvalue(1), boxing.as_qvalue(2.0)
        ).fingerprint,
    )

  def testMakeNamedTuple(self):
    self.assertEqual(
        boxing.namedtuple(a=1, b=2.0).fingerprint,
        tuple_qtypes.make_namedtuple_qvalue(
            a=boxing.as_qvalue(1), b=boxing.as_qvalue(2.0)
        ).fingerprint,
    )


class KwArgsBindingPolicyTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.op = arolla_abc.make_lambda(
        'keys, *values|experimental_kwargs',
        boxing.as_expr(
            (arolla_abc.placeholder('keys'), arolla_abc.placeholder('values'))
        ),
    )

  def testSignature(self):
    expected_signature = inspect.signature(lambda *args, **keys: None)
    self.assertEqual(inspect.signature(self.op), expected_signature)

  def testClassicMode(self):
    self.assertTrue(
        self.op('').equals(
            arolla_abc.make_operator_node(self.op, (boxing.literal(''),)),
        )
    )
    self.assertTrue(
        self.op('x', 1).equals(
            arolla_abc.make_operator_node(
                self.op, (boxing.literal('x'), boxing.literal(1))
            ),
        )
    )
    self.assertTrue(
        self.op('x, y', 1, 2).equals(
            arolla_abc.make_operator_node(
                self.op,
                (
                    boxing.literal('x, y'),
                    boxing.literal(1),
                    boxing.literal(2),
                ),
            ),
        )
    )

  def testKwargsMode(self):
    self.assertTrue(
        self.op().equals(
            arolla_abc.make_operator_node(self.op, (boxing.literal(''),)),
        )
    )
    self.assertTrue(
        self.op(x=1).equals(
            arolla_abc.make_operator_node(
                self.op, (boxing.literal('x'), boxing.literal(1))
            ),
        )
    )
    self.assertTrue(
        self.op(x=1, y=2).equals(
            arolla_abc.make_operator_node(
                self.op,
                (
                    boxing.literal('x,y'),
                    boxing.literal(1),
                    boxing.literal(2),
                ),
            ),
        )
    )

  def testClassicAndKwargsModesConsistent(self):
    expr = self.op(x=1, y=2)
    classic_expr = self.op(*expr.node_deps)
    self.assertTrue(
        expr.equals(classic_expr),
        f'{expr} != {classic_expr}',
    )

  def testMixedPositionalAndKeywordsError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected only positional or only keyword arguments, got both'
        " (aux_policy='experimental_kwargs')",
    ):
      _ = self.op('x', 1, y=2)


class FormatArgsBindingPolicyTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.op = arolla_abc.make_lambda(
        'fmt, arg_names, *values|experimental_format_args',
        boxing.as_expr((
            arolla_abc.placeholder('fmt'),
            arolla_abc.placeholder('arg_names'),
            arolla_abc.placeholder('values'),
        )),
    )

  def testSignature(self):
    expected_signature = inspect.signature(
        lambda fmt, *args, **values: None
    )
    self.assertEqual(inspect.signature(self.op), expected_signature)

  def testClassicMode(self):
    self.assertTrue(
        self.op('').equals(
            arolla_abc.make_operator_node(
                self.op, (boxing.literal(''), boxing.literal(''))
            ),
        )
    )
    self.assertTrue(
        self.op('fmt', 'q', 1).equals(
            arolla_abc.make_operator_node(
                self.op,
                (boxing.literal('fmt'), boxing.literal('q'), boxing.literal(1)),
            ),
        )
    )
    self.assertTrue(
        self.op('fmt', 'x, y', 1, 2).equals(
            arolla_abc.make_operator_node(
                self.op,
                (
                    boxing.literal('fmt'),
                    boxing.literal('x, y'),
                    boxing.literal(1),
                    boxing.literal(2),
                ),
            ),
        )
    )

  def testKwargsMode(self):
    self.assertTrue(
        self.op('fmt').equals(
            arolla_abc.make_operator_node(
                self.op,
                (
                    boxing.literal('fmt'),
                    boxing.literal(''),
                ),
            ),
        )
    )
    self.assertTrue(
        self.op('fmt', x=1).equals(
            arolla_abc.make_operator_node(
                self.op,
                (boxing.literal('fmt'), boxing.literal('x'), boxing.literal(1)),
            ),
        )
    )
    self.assertTrue(
        self.op('fmt', x=1, y=2).equals(
            arolla_abc.make_operator_node(
                self.op,
                (
                    boxing.literal('fmt'),
                    boxing.literal('x,y'),
                    boxing.literal(1),
                    boxing.literal(2),
                ),
            ),
        )
    )

  def testClassicAndKwargsModesConsistent(self):
    expr = self.op('fmt', x=1, y=2)
    classic_expr = self.op(*expr.node_deps)
    self.assertTrue(
        expr.equals(classic_expr),
        f'{expr} != {classic_expr}',
    )

  def testMixedPositionalAndKeywordsError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected exactly one positional arg with keyword arguments, got 3'
        " (aux_policy='experimental_format_args')",
    ):
      _ = self.op('fmt', 'x', 1, y=2)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected exactly one positional arg with keyword arguments, got 0'
        " (aux_policy='experimental_format_args')",
    ):
      _ = self.op(y=2)


if __name__ == '__main__':
  absltest.main()
