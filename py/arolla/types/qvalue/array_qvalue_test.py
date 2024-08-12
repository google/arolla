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

"""Tests for arolla.types.qvalue.array_qvalue."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.operators import operators_clib as _
from arolla.types.qtype import array_qtype as rl_array_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qvalue import array_qvalue as rl_array_qvalue
from arolla.types.qvalue import optional_qvalue as _
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins
from arolla.types.qvalue import scalar_qvalue as _
import numpy


class ArrayTest(parameterized.TestCase):

  def testInheritance(self):
    self.assertTrue(
        issubclass(rl_array_qvalue.Array, rl_qvalue_mixins.PresenceQValueMixin)
    )
    self.assertTrue(issubclass(rl_array_qvalue.ArrayInt, rl_array_qvalue.Array))
    self.assertTrue(
        issubclass(
            rl_array_qvalue.ArrayInt,
            rl_qvalue_mixins.IntegralArithmeticQValueMixin,
        )
    )
    self.assertTrue(
        issubclass(rl_array_qvalue.ArrayFloat, rl_array_qvalue.Array)
    )
    self.assertTrue(
        issubclass(
            rl_array_qvalue.ArrayFloat,
            rl_qvalue_mixins.FloatingPointArithmeticQValueMixin,
        )
    )

  @parameterized.parameters(
      (rl_array_qtype.array_unit, rl_array_qvalue.Array),
      (rl_array_qtype.array_boolean, rl_array_qvalue.Array),
      (rl_array_qtype.array_bytes, rl_array_qvalue.Array),
      (rl_array_qtype.array_text, rl_array_qvalue.Array),
      (rl_array_qtype.array_int32, rl_array_qvalue.ArrayInt),
      (rl_array_qtype.array_int64, rl_array_qvalue.ArrayInt),
      (rl_array_qtype.array_uint64, rl_array_qvalue.ArrayInt),
      (rl_array_qtype.array_float32, rl_array_qvalue.ArrayFloat),
      (rl_array_qtype.array_float64, rl_array_qvalue.ArrayFloat),
      (rl_array_qtype.array_weak_float, rl_array_qvalue.ArrayFloat),
  )
  def testArraySpecialisation(self, array_factory, clazz):
    self.assertIsInstance(array_factory([]), clazz)

  def testArrayLen(self):
    self.assertEmpty(rl_array_qtype.array_boolean([]))
    self.assertLen(rl_array_qtype.array_bytes([b'foo', b'bar', None]), 3)

  def testArraySize(self):
    self.assertEqual(rl_array_qtype.array_boolean([]).size, 0)
    self.assertEqual(rl_array_qtype.array_bytes([b'foo', b'bar', None]).size, 3)

  def testArrayGetItem(self):
    x = rl_array_qtype.array_text([None, 'foo', 'bar'])
    self.assertIsNone(x[0].py_value())
    self.assertEqual(x[1].py_value(), 'foo')
    self.assertEqual(x[2].py_value(), 'bar')
    self.assertEqual(x[-1].py_value(), 'bar')
    self.assertEqual(x[-2].py_value(), 'foo')
    self.assertIsNone(x[-3].py_value())
    with self.assertRaises(IndexError):
      _ = x[3]
    with self.assertRaises(IndexError):
      _ = x[-4]
    with self.assertRaises(TypeError):
      _ = x['x']

  @parameterized.parameters(
      (rl_array_qtype.array_unit, [], 'array([], value_qtype=UNIT)'),
      (
          rl_array_qtype.array_unit,
          [rl_scalar_qtype.unit(), None, rl_scalar_qtype.unit()],
          'array([present, NA, present])',
      ),
      (rl_array_qtype.array_int32, range(5), 'array([0, 1, 2, 3, 4])'),
      (
          rl_array_qtype.array_float32,
          [None],
          'array([NA], value_qtype=FLOAT32)',
      ),
      (
          rl_array_qtype.array_weak_float,
          [None],
          'array([NA], value_qtype=WEAK_FLOAT)',
      ),
      (rl_array_qtype.array_weak_float, [1.0], 'array([weak_float{1}])'),
      (
          rl_array_qtype.array_uint64,
          [None, None, 1, 2],
          'array([NA, NA, uint64{1}, uint64{2}])',
      ),
      (
          rl_array_qtype.array_text,
          map(str, range(100)),
          (
              "array(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ...], "
              'size=100)'
          ),
      ),
  )
  def testArrayRepr(self, factory, values, expected_repr):
    self.assertEqual(repr(factory(values)), expected_repr)

  def testArrayValueQType(self):
    self.assertEqual(
        rl_array_qtype.array_int32([]).value_qtype, rl_scalar_qtype.INT32
    )

  def testArrayPyValue(self):
    self.assertListEqual(
        rl_array_qtype.array_int32([1, 2, None, 3]).py_value(), [1, 2, None, 3]
    )

  def testArrayToIdsAndValues(self):
    ids, values = rl_array_qtype.array_int32(
        [1, 2, None, 3]
    ).to_ids_and_values()
    self.assertListEqual(ids, [0, 1, 3])
    self.assertListEqual(values, [1, 2, 3])

  def testArrayToIdsAndValuesAsNumpyArrays(self):
    ids, values = rl_array_qtype.array_float64(
        [1.5, 2.5, None, 3.5]
    ).to_ids_and_values_as_numpy_arrays()
    self.assertEqual(ids.dtype, numpy.int64)
    self.assertTrue(numpy.array_equal(ids, [0, 1, 3]))
    self.assertEqual(values.dtype, numpy.float64)
    self.assertTrue(numpy.array_equal(values, [1.5, 2.5, 3.5]))

  def testArrayPresentCount(self):
    self.assertEqual(rl_array_qtype.array_boolean([]).present_count, 0)
    self.assertEqual(
        rl_array_qtype.array_bytes([b'foo', b'bar', None]).present_count, 2
    )

  @parameterized.parameters(
      (rl_array_qvalue.ArrayEdge.from_sizes, [1, 1, 0, 2, 0]),
      (rl_array_qvalue.ArrayEdge.from_split_points, [0, 1, 2, 2, 4, 4]),
      (rl_array_qvalue.ArrayEdge.from_mapping, [0, 1, 3, 3], 5),
      (
          rl_array_qvalue.ArrayEdge.from_keys,
          [0, 1, 2, 2],
          [0, 1, 3, 2, 4],  # Not required to be sorted.
      ),
      (
          rl_array_qvalue.ArrayEdge.from_keys,
          [b'a', b'b', b'c', b'c'],
          [b'a', b'b', b'd', b'c', b'e'],
      ),
  )
  def testArrayEdge(self, constructor, *args):
    e = constructor(*args)
    self.assertIsInstance(e, rl_array_qvalue.ArrayEdge)
    self.assertEqual(e.qtype, rl_array_qtype.ARRAY_EDGE)
    self.assertEqual(e.parent_size, 5)
    self.assertEqual(e.child_size, 4)
    self.assertListEqual(e.mapping().py_value(), [0, 1, 3, 3])

  def testArrayEdgeNewRaises(self):
    with self.assertRaisesRegex(
        NotImplementedError, r'please use ArrayEdge\.from'
    ):
      rl_array_qvalue.ArrayEdge()

  @parameterized.parameters(0, 1, 5)
  def testArrayShape(self, n):
    shape = rl_array_qvalue.ArrayShape(n)
    self.assertIsInstance(shape, rl_array_qvalue.ArrayShape)
    self.assertEqual(shape.qtype, rl_array_qtype.ARRAY_SHAPE)
    self.assertEqual(shape.size, n)

  @parameterized.parameters(0, 1, 5)
  def testArrayToScalarEdge(self, n):
    edge = rl_array_qvalue.ArrayToScalarEdge(n)
    self.assertIsInstance(edge, rl_array_qvalue.ArrayToScalarEdge)
    self.assertEqual(edge.qtype, rl_array_qtype.ARRAY_TO_SCALAR_EDGE)
    self.assertEqual(edge.child_size, n)


if __name__ == '__main__':
  absltest.main()
