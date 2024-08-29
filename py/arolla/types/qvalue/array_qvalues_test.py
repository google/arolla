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

"""Tests for arolla.types.qvalue.array_qvalues."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.operators import operators_clib as _
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qvalue import array_qvalues
from arolla.types.qvalue import optional_qvalues as _
from arolla.types.qvalue import qvalue_mixins
from arolla.types.qvalue import scalar_qvalues as _


class ArrayTest(parameterized.TestCase):

  def testInheritance(self):
    self.assertTrue(
        issubclass(array_qvalues.Array, qvalue_mixins.PresenceQValueMixin)
    )
    self.assertTrue(issubclass(array_qvalues.ArrayInt, array_qvalues.Array))
    self.assertTrue(
        issubclass(
            array_qvalues.ArrayInt,
            qvalue_mixins.IntegralArithmeticQValueMixin,
        )
    )
    self.assertTrue(issubclass(array_qvalues.ArrayFloat, array_qvalues.Array))
    self.assertTrue(
        issubclass(
            array_qvalues.ArrayFloat,
            qvalue_mixins.FloatingPointArithmeticQValueMixin,
        )
    )

  @parameterized.parameters(
      (array_qtypes.array_unit, array_qvalues.Array),
      (array_qtypes.array_boolean, array_qvalues.Array),
      (array_qtypes.array_bytes, array_qvalues.Array),
      (array_qtypes.array_text, array_qvalues.Array),
      (array_qtypes.array_int32, array_qvalues.ArrayInt),
      (array_qtypes.array_int64, array_qvalues.ArrayInt),
      (array_qtypes.array_uint64, array_qvalues.ArrayInt),
      (array_qtypes.array_float32, array_qvalues.ArrayFloat),
      (array_qtypes.array_float64, array_qvalues.ArrayFloat),
      (array_qtypes.array_weak_float, array_qvalues.ArrayFloat),
  )
  def testArraySpecialisation(self, array_factory, clazz):
    self.assertIsInstance(array_factory([]), clazz)

  def testArrayLen(self):
    self.assertEmpty(array_qtypes.array_boolean([]))
    self.assertLen(array_qtypes.array_bytes([b'foo', b'bar', None]), 3)

  def testArraySize(self):
    self.assertEqual(array_qtypes.array_boolean([]).size, 0)
    self.assertEqual(array_qtypes.array_bytes([b'foo', b'bar', None]).size, 3)

  def testArrayGetItem(self):
    x = array_qtypes.array_text([None, 'foo', 'bar'])
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
      (array_qtypes.array_unit, [], 'array([], value_qtype=UNIT)'),
      (
          array_qtypes.array_unit,
          [scalar_qtypes.unit(), None, scalar_qtypes.unit()],
          'array([present, NA, present])',
      ),
      (array_qtypes.array_int32, range(5), 'array([0, 1, 2, 3, 4])'),
      (
          array_qtypes.array_float32,
          [None],
          'array([NA], value_qtype=FLOAT32)',
      ),
      (
          array_qtypes.array_weak_float,
          [None],
          'array([NA], value_qtype=WEAK_FLOAT)',
      ),
      (array_qtypes.array_weak_float, [1.0], 'array([weak_float{1}])'),
      (
          array_qtypes.array_uint64,
          [None, None, 1, 2],
          'array([NA, NA, uint64{1}, uint64{2}])',
      ),
      (
          array_qtypes.array_text,
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
        array_qtypes.array_int32([]).value_qtype, scalar_qtypes.INT32
    )

  def testArrayPyValue(self):
    self.assertListEqual(
        array_qtypes.array_int32([1, 2, None, 3]).py_value(), [1, 2, None, 3]
    )

  def testArrayToIdsAndValues(self):
    ids, values = array_qtypes.array_int32([1, 2, None, 3]).to_ids_and_values()
    self.assertListEqual(ids, [0, 1, 3])
    self.assertListEqual(values, [1, 2, 3])

  def testArrayPresentCount(self):
    self.assertEqual(array_qtypes.array_boolean([]).present_count, 0)
    self.assertEqual(
        array_qtypes.array_bytes([b'foo', b'bar', None]).present_count, 2
    )

  @parameterized.parameters(
      (array_qvalues.ArrayEdge.from_sizes, [1, 1, 0, 2, 0]),
      (array_qvalues.ArrayEdge.from_split_points, [0, 1, 2, 2, 4, 4]),
      (array_qvalues.ArrayEdge.from_mapping, [0, 1, 3, 3], 5),
      (
          array_qvalues.ArrayEdge.from_keys,
          [0, 1, 2, 2],
          [0, 1, 3, 2, 4],  # Not required to be sorted.
      ),
      (
          array_qvalues.ArrayEdge.from_keys,
          [b'a', b'b', b'c', b'c'],
          [b'a', b'b', b'd', b'c', b'e'],
      ),
  )
  def testArrayEdge(self, constructor, *args):
    e = constructor(*args)
    self.assertIsInstance(e, array_qvalues.ArrayEdge)
    self.assertEqual(e.qtype, array_qtypes.ARRAY_EDGE)
    self.assertEqual(e.parent_size, 5)
    self.assertEqual(e.child_size, 4)
    self.assertListEqual(e.mapping().py_value(), [0, 1, 3, 3])

  def testArrayEdgeNewRaises(self):
    with self.assertRaisesRegex(
        NotImplementedError, r'please use ArrayEdge\.from'
    ):
      array_qvalues.ArrayEdge()

  @parameterized.parameters(0, 1, 5)
  def testArrayShape(self, n):
    shape = array_qvalues.ArrayShape(n)
    self.assertIsInstance(shape, array_qvalues.ArrayShape)
    self.assertEqual(shape.qtype, array_qtypes.ARRAY_SHAPE)
    self.assertEqual(shape.size, n)

  @parameterized.parameters(0, 1, 5)
  def testArrayToScalarEdge(self, n):
    edge = array_qvalues.ArrayToScalarEdge(n)
    self.assertIsInstance(edge, array_qvalues.ArrayToScalarEdge)
    self.assertEqual(edge.qtype, array_qtypes.ARRAY_TO_SCALAR_EDGE)
    self.assertEqual(edge.child_size, n)


if __name__ == '__main__':
  absltest.main()
