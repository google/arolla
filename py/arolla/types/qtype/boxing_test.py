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
import numpy
from arolla.abc import abc as rl_abc
from arolla.types.qtype import array_qtype as rl_array_qtype
from arolla.types.qtype import boxing as rl_boxing
from arolla.types.qtype import dense_array_qtype as rl_dense_array_qtype
from arolla.types.qtype import optional_qtype as rl_optional_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import tuple_qtype as rl_tuple_qtype


def iter_range(n):
  """Returns a iterable object that can be scanned only once."""
  return iter(range(n))


class BoxingTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, rl_scalar_qtype.int32(1)),
      (1.0, rl_scalar_qtype.float32(1)),
      ((), rl_tuple_qtype.make_tuple_qvalue()),
      (
          (1, 2.0, True),
          rl_tuple_qtype.make_tuple_qvalue(
              rl_scalar_qtype.int32(1),
              rl_scalar_qtype.float32(2.0),
              rl_scalar_qtype.boolean(True),
          ),
      ),
      ([None, True, False], rl_array_qtype.array_boolean([None, True, False])),
      (None, rl_abc.unspecified()),
      (rl_abc.QTYPE, rl_abc.QTYPE),
      (numpy.bool_(True), rl_scalar_qtype.boolean(True)),
      (numpy.int32(1), rl_scalar_qtype.int32(1)),
      (numpy.int64(1), rl_scalar_qtype.int64(1)),
      (numpy.uint64(1), rl_scalar_qtype.uint64(1)),
      (numpy.float32(1), rl_scalar_qtype.float32(1)),
      (numpy.float64(1), rl_scalar_qtype.float64(1)),
      (numpy.bytes_(b'foo'), rl_scalar_qtype.bytes_(b'foo')),
      (numpy.str_('bar'), rl_scalar_qtype.text('bar')),
      (
          numpy.array([1, 2, 3], numpy.float64),
          rl_array_qtype.array_float64([1, 2, 3]),
      ),
  )
  def testAsQValue(self, py_value, expected_qvalue):
    qvalue = rl_boxing.as_qvalue(py_value)
    self.assertEqual(
        qvalue.fingerprint,
        expected_qvalue.fingerprint,
        f'{qvalue} != {expected_qvalue}',
    )

  def testAsQValue_TypeError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'unsupported type: object'
    ):
      _ = rl_boxing.as_qvalue(object())

  @parameterized.parameters(
      (1, rl_scalar_qtype.int32(1)),
      (1.0, rl_scalar_qtype.float32(1)),
      ((), rl_tuple_qtype.make_tuple_qvalue()),
      (
          ((1, 2.0), rl_abc.leaf('x')),
          rl_abc.bind_op(
              'core.make_tuple',
              rl_tuple_qtype.make_tuple_qvalue(
                  rl_scalar_qtype.int32(1),
                  rl_scalar_qtype.float32(2.0),
              ),
              rl_abc.leaf('x'),
          ),
      ),
      ([None, True, False], rl_array_qtype.array_boolean([None, True, False])),
      (None, rl_abc.unspecified()),
      (rl_abc.QTYPE, rl_abc.QTYPE),
      (
          numpy.array([1, 2, 3], numpy.float64),
          rl_array_qtype.array_float64([1, 2, 3]),
      ),
      (numpy.bool_(True), rl_scalar_qtype.boolean(True)),
      (numpy.int32(1), rl_scalar_qtype.int32(1)),
      (numpy.int64(1), rl_scalar_qtype.int64(1)),
      (numpy.uint64(1), rl_scalar_qtype.uint64(1)),
      (numpy.float32(1), rl_scalar_qtype.float32(1)),
      (numpy.float64(1), rl_scalar_qtype.float64(1)),
      (numpy.bytes_(b'foo'), rl_scalar_qtype.bytes_(b'foo')),
      (numpy.str_('bar'), rl_scalar_qtype.text('bar')),
  )
  def testAsQValueOrExpr(self, py_value, expected_qvalue_or_expr):
    qvalue = rl_boxing.as_qvalue_or_expr(py_value)
    self.assertEqual(
        qvalue.fingerprint,
        expected_qvalue_or_expr.fingerprint,
        f'{qvalue} != {expected_qvalue_or_expr}',
    )

  def testAsQValueOrExpr_TypeError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'unsupported type: object'
    ):
      _ = rl_boxing.as_qvalue_or_expr(object())

  @parameterized.parameters(
      (1, rl_abc.literal(rl_scalar_qtype.int32(1))),
      (1.0, rl_abc.literal(rl_scalar_qtype.float32(1))),
      ((), rl_abc.literal(rl_tuple_qtype.make_tuple_qvalue())),
      (
          ((1, 2.0), rl_abc.leaf('x')),
          rl_abc.bind_op(
              'core.make_tuple',
              rl_tuple_qtype.make_tuple_qvalue(
                  rl_scalar_qtype.int32(1),
                  rl_scalar_qtype.float32(2.0),
              ),
              rl_abc.leaf('x'),
          ),
      ),
      (
          [None, True, False],
          rl_abc.literal(rl_array_qtype.array_boolean([None, True, False])),
      ),
      (None, rl_abc.literal(rl_abc.unspecified())),
      (rl_abc.QTYPE, rl_abc.literal(rl_abc.QTYPE)),
      (
          numpy.array([1, 2, 3], numpy.float64),
          rl_abc.literal(rl_array_qtype.array_float64([1, 2, 3])),
      ),
  )
  def testAsExpr(self, py_value, expected_expr):
    expr = rl_boxing.as_expr(py_value)
    self.assertEqual(
        expr.fingerprint,
        expected_expr.fingerprint,
        f'{expr} != {expected_expr}',
    )

  def testAsExpr_TypeError(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('unable to create a literal from: [L.y]')
    ):
      _ = rl_boxing.as_expr([rl_abc.leaf('y')])

  def testLiteral(self):
    x = rl_boxing.literal(1)
    self.assertTrue(x.is_literal)
    self.assertEqual(str(x), '1')
    self.assertEqual(repr(x), '1')
    assert x.qvalue is not None
    self.assertEqual(x.qvalue.fingerprint, rl_boxing.as_qvalue(1).fingerprint)

  def testBindOp(self):
    self.assertEqual(
        rl_abc.aux_bind_op(
            'core.identity', (rl_abc.leaf('y'), 2.5, ['foo'])
        ).fingerprint,
        rl_abc.bind_op(
            'core.identity',
            rl_abc.bind_op(
                'core.make_tuple',
                rl_abc.leaf('y'),
                rl_scalar_qtype.float32(2.5),
                rl_array_qtype.array_text(['foo']),
            ),
        ).fingerprint,
    )

  def testBindKwargs(self):
    self.assertTrue(
        rl_abc.aux_bind_op('math.add', 1, y=2).equals(
            rl_abc.bind_op(
                'math.add', rl_scalar_qtype.int32(1), rl_scalar_qtype.int32(2)
            )
        )
    )

  def testEval(self):
    self.assertEqual(
        rl_boxing.eval_((rl_abc.leaf('y'), (2.5, ['foo'])), y=1).fingerprint,
        rl_boxing.tuple_(1, (2.5, ['foo'])).fingerprint,
    )

  def testEvalIntermediateResultsNotAllInMemory(self):
    """Will OOM if all intermediate results will be materialized."""

    node = rl_abc.leaf('a')
    num_ops = 1_000
    array_size = 1_000
    for _ in range(num_ops - 1):
      node = rl_abc.bind_op('math.add', node, rl_abc.leaf('a'))
    a = rl_boxing.array([5.0] * array_size, value_qtype=rl_scalar_qtype.FLOAT64)
    _ = rl_boxing.eval_(node, a=a)

  @parameterized.parameters(
      # Builtin types.
      (False, rl_scalar_qtype.BOOLEAN),
      (True, rl_scalar_qtype.BOOLEAN),
      (b'', rl_scalar_qtype.BYTES),
      (b'foo', rl_scalar_qtype.BYTES),
      ('', rl_scalar_qtype.TEXT),
      ('bar', rl_scalar_qtype.TEXT),
      (0, rl_scalar_qtype.INT32),
      (1, rl_scalar_qtype.INT32),
      (0.0, rl_scalar_qtype.FLOAT32),
      (3.14, rl_scalar_qtype.FLOAT32),
      # Numpy types.
      (numpy.bool_(False), rl_scalar_qtype.BOOLEAN),
      (numpy.int32(1), rl_scalar_qtype.INT32),
      (numpy.int64(1), rl_scalar_qtype.INT64),
      (numpy.uint64(1), rl_scalar_qtype.UINT64),
      (numpy.float32(1.0), rl_scalar_qtype.FLOAT32),
      (numpy.float64(1.0), rl_scalar_qtype.FLOAT64),
      (numpy.bytes_(b'foo'), rl_scalar_qtype.BYTES),
      (numpy.str_('foo'), rl_scalar_qtype.TEXT),
      # QValues.
      (rl_scalar_qtype.float64(1.0), rl_scalar_qtype.FLOAT64),
  )
  def testDeduceScalarQType(self, value, expected_value_type):
    actual_value_qtype = rl_boxing._deduce_scalar_qtype_from_value(value)
    self.assertEqual(actual_value_qtype, expected_value_type)

  @parameterized.parameters(
      rl_abc.leaf('x'),
      rl_abc.literal(rl_scalar_qtype.int32(1)),
  )
  def testDeduceScalarQType_TypeError(self, value):
    with self.assertRaises(TypeError):
      _ = rl_boxing._deduce_scalar_qtype_from_value(value)

  @parameterized.parameters(
      # Builtin types.
      ([None, True], rl_scalar_qtype.BOOLEAN),
      ([None, b'foo'], rl_scalar_qtype.BYTES),
      ([None, 'bar'], rl_scalar_qtype.TEXT),
      ([None, 1], rl_scalar_qtype.INT32),
      ([None, 3.14], rl_scalar_qtype.FLOAT32),
      # Numpy scalar types.
      ([None, numpy.bool_(False)], rl_scalar_qtype.BOOLEAN),
      ([None, numpy.int32(1)], rl_scalar_qtype.INT32),
      ([None, numpy.int64(1)], rl_scalar_qtype.INT64),
      ([None, numpy.uint64(1)], rl_scalar_qtype.UINT64),
      ([None, numpy.float32(1.0)], rl_scalar_qtype.FLOAT32),
      ([None, numpy.float64(1.0)], rl_scalar_qtype.FLOAT64),
      ([None, numpy.bytes_(b'foo')], rl_scalar_qtype.BYTES),
      ([None, numpy.str_('foo')], rl_scalar_qtype.TEXT),
      # Numpy arrays.
      (numpy.array([False], numpy.bool_), rl_scalar_qtype.BOOLEAN),
      (numpy.array([1], numpy.int32), rl_scalar_qtype.INT32),
      (numpy.array([1], numpy.int64), rl_scalar_qtype.INT64),
      (numpy.array([1], numpy.uint64), rl_scalar_qtype.UINT64),
      (numpy.array([1], numpy.float32), rl_scalar_qtype.FLOAT32),
      (numpy.array([1], numpy.float64), rl_scalar_qtype.FLOAT64),
      (numpy.array([b'foo'], numpy.bytes_), rl_scalar_qtype.BYTES),
      (numpy.array(['foo'], numpy.str_), rl_scalar_qtype.TEXT),
      # QValues.
      ([None, rl_scalar_qtype.int64(2**34)], rl_scalar_qtype.INT64),
      ([None, rl_scalar_qtype.float64(1.0)], rl_scalar_qtype.FLOAT64),
      # Implicit cast.
      (
          [None, rl_scalar_qtype.float64(1), rl_scalar_qtype.float32(1)],
          rl_scalar_qtype.FLOAT64,
      ),
  )
  def testDeduceQTypeFromValues(self, values, expected_value_qtype):
    actual_value_qtype = rl_boxing._deduce_value_qtype_from_values(values)
    self.assertEqual(actual_value_qtype, expected_value_qtype)

  @parameterized.parameters(
      [[]],
      [[None]],
      [[False, b'']],
  )
  def testDeduceValueQType_ValueError(self, values):
    with self.assertRaises(ValueError):
      _ = rl_boxing._deduce_value_qtype_from_values(values)

  @parameterized.parameters(
      [[[]]],
      [[object()]],
      [[rl_abc.leaf('x')]],
      [[rl_abc.literal(rl_scalar_qtype.int32(1))]],
  )
  def testDeduceValueQType_TypeError(self, values):
    with self.assertRaises(TypeError):
      _ = rl_boxing._deduce_value_qtype_from_values(values)

  @parameterized.parameters(*rl_dense_array_qtype.DENSE_ARRAY_QTYPES)
  def testDenseArrayFromValues_QType(self, dense_array_qtype):
    self.assertEqual(
        rl_boxing.dense_array([], dense_array_qtype.value_qtype).qtype,
        dense_array_qtype,
    )
    self.assertEqual(
        rl_boxing.dense_array(
            [],
            rl_optional_qtype.make_optional_qtype(
                dense_array_qtype.value_qtype
            ),
        ).qtype,
        dense_array_qtype,
    )

  def testDenseArrayFromValues_WithGenerator(self):
    self.assertEqual(
        rl_boxing.dense_array(iter_range(2)).fingerprint,
        rl_dense_array_qtype.dense_array_int32([0, 1]).fingerprint,
    )
    self.assertEqual(
        rl_boxing.dense_array(iter_range(2), rl_scalar_qtype.INT64).fingerprint,
        rl_dense_array_qtype.dense_array_int64([0, 1]).fingerprint,
    )

  @parameterized.parameters(*rl_dense_array_qtype.DENSE_ARRAY_QTYPES)
  def testDenseArrayFromDenseArray_SameQType(self, dense_array_qtype):
    da = rl_boxing.dense_array([], dense_array_qtype.value_qtype)
    self.assertEqual(rl_boxing.dense_array(da).qtype, dense_array_qtype)
    self.assertEqual(
        rl_boxing.dense_array(da, dense_array_qtype.value_qtype).qtype,
        dense_array_qtype,
    )

  @parameterized.parameters(
      *itertools.combinations(
          [
              rl_dense_array_qtype.DENSE_ARRAY_INT32,
              rl_dense_array_qtype.DENSE_ARRAY_INT64,
          ],
          2,
      ),
      *itertools.combinations(
          [
              rl_dense_array_qtype.DENSE_ARRAY_WEAK_FLOAT,
              rl_dense_array_qtype.DENSE_ARRAY_FLOAT32,
              rl_dense_array_qtype.DENSE_ARRAY_FLOAT64,
          ],
          2,
      ),
  )
  def testDenseArrayFromDenseArray_ImplicitCasting(self, from_qtype, to_qtype):
    da = rl_boxing.dense_array([], from_qtype.value_qtype)
    self.assertEqual(
        rl_boxing.dense_array(da, to_qtype.value_qtype).qtype, to_qtype
    )

  def testDenseArrayFromValues_NumpyArrayPerformance(self):
    self.assertEqual(
        rl_boxing.dense_array(numpy.arange(10**6, dtype=numpy.int32)).qtype,
        rl_dense_array_qtype.DENSE_ARRAY_INT32,
    )
    self.assertEqual(
        rl_boxing.dense_array(
            numpy.arange(10**6, dtype=numpy.int32), rl_scalar_qtype.INT64
        ).qtype,
        rl_dense_array_qtype.DENSE_ARRAY_INT64,
    )

  def testDenseArrayFromValues_DenseArrayPerformance(self):
    da_int32 = rl_boxing.dense_array(numpy.arange(10**6, dtype=numpy.int32))
    self.assertEqual(
        rl_boxing.dense_array(da_int32).qtype,
        rl_dense_array_qtype.DENSE_ARRAY_INT32,
    )
    self.assertEqual(
        rl_boxing.dense_array(da_int32, rl_scalar_qtype.INT64).qtype,
        rl_dense_array_qtype.DENSE_ARRAY_INT64,
    )

  @parameterized.parameters(*rl_array_qtype.ARRAY_QTYPES)
  def testArrayFromValues_QType(self, array_qtype):
    self.assertEqual(
        rl_boxing.array([], array_qtype.value_qtype).qtype, array_qtype
    )
    self.assertEqual(
        rl_boxing.array(
            [], rl_optional_qtype.make_optional_qtype(array_qtype.value_qtype)
        ).qtype,
        array_qtype,
    )

  def testArrayFromValues_WithGenerator(self):
    self.assertEqual(
        rl_boxing.array(iter_range(2)).fingerprint,
        rl_array_qtype.array_int32([0, 1]).fingerprint,
    )
    self.assertEqual(
        rl_boxing.array(iter_range(2), rl_scalar_qtype.INT64).fingerprint,
        rl_array_qtype.array_int64([0, 1]).fingerprint,
    )

  @parameterized.parameters(*rl_array_qtype.ARRAY_QTYPES)
  def testArrayFromArray_SameQType(self, array_qtype):
    qb = rl_boxing.array([], array_qtype.value_qtype)
    self.assertEqual(rl_boxing.array(qb).qtype, array_qtype)
    self.assertEqual(
        rl_boxing.array(qb, array_qtype.value_qtype).qtype, array_qtype
    )

  @parameterized.parameters(
      *itertools.combinations(
          [rl_array_qtype.ARRAY_INT32, rl_array_qtype.ARRAY_INT64], 2
      ),
      *itertools.combinations(
          [
              rl_array_qtype.ARRAY_WEAK_FLOAT,
              rl_array_qtype.ARRAY_FLOAT32,
              rl_array_qtype.ARRAY_FLOAT64,
          ],
          2,
      ),
  )
  def testArrayFromArray_ImplicitCasting(self, from_qtype, to_qtype):
    qb = rl_boxing.array([], from_qtype.value_qtype)
    self.assertEqual(rl_boxing.array(qb, to_qtype.value_qtype).qtype, to_qtype)

  def testArrayFromValues_NumpyArrayPerformance(self):
    self.assertEqual(
        rl_boxing.array(numpy.arange(10**6, dtype=numpy.int32)).qtype,
        rl_array_qtype.ARRAY_INT32,
    )
    self.assertEqual(
        rl_boxing.array(
            numpy.arange(10**6, dtype=numpy.int32), rl_scalar_qtype.INT64
        ).qtype,
        rl_array_qtype.ARRAY_INT64,
    )

  def testArrayFromValues_ArrayPerformance(self):
    qb_int32 = rl_boxing.array(numpy.arange(10**6, dtype=numpy.int32))
    self.assertEqual(
        rl_boxing.array(qb_int32).qtype, rl_array_qtype.ARRAY_INT32
    )
    self.assertEqual(
        rl_boxing.array(qb_int32, rl_scalar_qtype.INT64).qtype,
        rl_array_qtype.ARRAY_INT64,
    )

  def testMakeTuple(self):
    self.assertEqual(
        rl_boxing.tuple_(1, 2.0).fingerprint,
        rl_tuple_qtype.make_tuple_qvalue(
            rl_boxing.as_qvalue(1), rl_boxing.as_qvalue(2.0)
        ).fingerprint,
    )

  def testMakeNamedTuple(self):
    self.assertEqual(
        rl_boxing.namedtuple(a=1, b=2.0).fingerprint,
        rl_tuple_qtype.make_namedtuple_qvalue(
            a=rl_boxing.as_qvalue(1), b=rl_boxing.as_qvalue(2.0)
        ).fingerprint,
    )


class KwArgsBindingPolicyTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.op = rl_abc.make_lambda(
        'keys, *values|experimental_kwargs',
        rl_boxing.as_expr(
            (rl_abc.placeholder('keys'), rl_abc.placeholder('values'))
        ),
    )

  def testSignature(self):
    expected_signature = inspect.signature(lambda *args, **keys: None)
    self.assertEqual(inspect.signature(self.op), expected_signature)

  def testClassicMode(self):
    self.assertTrue(
        self.op('').equals(
            rl_abc.make_operator_node(self.op, (rl_boxing.literal(''),)),
        )
    )
    self.assertTrue(
        self.op('x', 1).equals(
            rl_abc.make_operator_node(
                self.op, (rl_boxing.literal('x'), rl_boxing.literal(1))
            ),
        )
    )
    self.assertTrue(
        self.op('x, y', 1, 2).equals(
            rl_abc.make_operator_node(
                self.op,
                (
                    rl_boxing.literal('x, y'),
                    rl_boxing.literal(1),
                    rl_boxing.literal(2),
                ),
            ),
        )
    )

  def testKwargsMode(self):
    self.assertTrue(
        self.op().equals(
            rl_abc.make_operator_node(self.op, (rl_boxing.literal(''),)),
        )
    )
    self.assertTrue(
        self.op(x=1).equals(
            rl_abc.make_operator_node(
                self.op, (rl_boxing.literal('x'), rl_boxing.literal(1))
            ),
        )
    )
    self.assertTrue(
        self.op(x=1, y=2).equals(
            rl_abc.make_operator_node(
                self.op,
                (
                    rl_boxing.literal('x,y'),
                    rl_boxing.literal(1),
                    rl_boxing.literal(2),
                ),
            ),
        )
    )

  def testMixedPositionalAndKeywordsError(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'expected only positional or only keyword arguments, got both'
        " (aux_policy='experimental_kwargs')",
    ):
      _ = self.op('x', 1, y=2)


if __name__ == '__main__':
  absltest.main()
