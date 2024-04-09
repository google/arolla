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

"""Tests for arolla.types.qvalue.dense_array_qvalue."""

from absl.testing import absltest
from absl.testing import parameterized

from arolla.types.qtype import dense_array_qtype as rl_dense_array_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qvalue import dense_array_qvalue as rl_dense_array_qvalue
from arolla.types.qvalue import optional_qvalue as rl_optional_qvalue  # pylint: disable=unused-import
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins
from arolla.types.qvalue import scalar_qvalue as rl_scalar_qvalue  # pylint: disable=unused-import


class DenseArrayTest(parameterized.TestCase):

  def testInheritance(self):
    self.assertTrue(
        issubclass(
            rl_dense_array_qvalue.DenseArray,
            rl_qvalue_mixins.PresenceQValueMixin,
        )
    )
    self.assertTrue(
        issubclass(
            rl_dense_array_qvalue.DenseArrayInt,
            rl_dense_array_qvalue.DenseArray,
        )
    )
    self.assertTrue(
        issubclass(
            rl_dense_array_qvalue.DenseArrayInt,
            rl_qvalue_mixins.IntegralArithmeticQValueMixin,
        )
    )
    self.assertTrue(
        issubclass(
            rl_dense_array_qvalue.DenseArrayFloat,
            rl_dense_array_qvalue.DenseArray,
        )
    )
    self.assertTrue(
        issubclass(
            rl_dense_array_qvalue.DenseArrayFloat,
            rl_qvalue_mixins.FloatingPointArithmeticQValueMixin,
        )
    )

  @parameterized.parameters(
      (rl_dense_array_qtype.dense_array_unit, rl_dense_array_qvalue.DenseArray),
      (
          rl_dense_array_qtype.dense_array_boolean,
          rl_dense_array_qvalue.DenseArray,
      ),
      (
          rl_dense_array_qtype.dense_array_bytes,
          rl_dense_array_qvalue.DenseArray,
      ),
      (rl_dense_array_qtype.dense_array_text, rl_dense_array_qvalue.DenseArray),
      (
          rl_dense_array_qtype.dense_array_int32,
          rl_dense_array_qvalue.DenseArrayInt,
      ),
      (
          rl_dense_array_qtype.dense_array_int64,
          rl_dense_array_qvalue.DenseArrayInt,
      ),
      (
          rl_dense_array_qtype.dense_array_uint64,
          rl_dense_array_qvalue.DenseArrayInt,
      ),
      (
          rl_dense_array_qtype.dense_array_float32,
          rl_dense_array_qvalue.DenseArrayFloat,
      ),
      (
          rl_dense_array_qtype.dense_array_float64,
          rl_dense_array_qvalue.DenseArrayFloat,
      ),
      (
          rl_dense_array_qtype.dense_array_weak_float,
          rl_dense_array_qvalue.DenseArrayFloat,
      ),
  )
  def testDenseArraySpecialisation(self, dense_array_factory, clazz):
    self.assertIsInstance(dense_array_factory([]), clazz)

  def testDenseArrayLen(self):
    self.assertEmpty(rl_dense_array_qtype.dense_array_boolean([]))
    self.assertLen(
        rl_dense_array_qtype.dense_array_bytes([b'foo', b'bar', None]), 3
    )

  def testDenseArraySize(self):
    self.assertEqual(rl_dense_array_qtype.dense_array_boolean([]).size, 0)
    self.assertEqual(
        rl_dense_array_qtype.dense_array_bytes([b'foo', b'bar', None]).size, 3
    )

  def testDenseArrayGetItem(self):
    x = rl_dense_array_qtype.dense_array_text([None, 'foo', 'bar'])
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
      (
          rl_dense_array_qtype.dense_array_unit,
          [],
          'dense_array([], value_qtype=UNIT)',
      ),
      (
          rl_dense_array_qtype.dense_array_unit,
          [rl_scalar_qtype.unit(), None, rl_scalar_qtype.unit()],
          'dense_array([present, NA, present])',
      ),
      (
          rl_dense_array_qtype.dense_array_int32,
          range(5),
          'dense_array([0, 1, 2, 3, 4])',
      ),
      (
          rl_dense_array_qtype.dense_array_float32,
          [None],
          'dense_array([NA], value_qtype=FLOAT32)',
      ),
      (
          rl_dense_array_qtype.dense_array_weak_float,
          [None],
          'dense_array([NA], value_qtype=WEAK_FLOAT)',
      ),
      (
          rl_dense_array_qtype.dense_array_weak_float,
          [1.0],
          'dense_array([weak_float{1}])',
      ),
      (
          rl_dense_array_qtype.dense_array_uint64,
          [None, None, 1, 2],
          'dense_array([NA, NA, uint64{1}, uint64{2}])',
      ),
      (
          rl_dense_array_qtype.dense_array_text,
          map(str, range(100)),
          (
              "dense_array(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9',"
              ' ...], size=100)'
          ),
      ),
  )
  def testDenseArrayRepr(self, factory, values, expected_repr):
    self.assertEqual(repr(factory(values)), expected_repr)

  def testDenseArrayValueQType(self):
    self.assertEqual(
        rl_dense_array_qtype.dense_array_int64([]).value_qtype,
        rl_scalar_qtype.INT64,
    )

  def testDenseArrayPyValue(self):
    self.assertListEqual(
        rl_dense_array_qtype.dense_array_int32([1, 2, None, 3]).py_value(),
        [1, 2, None, 3],
    )

  def testDenseArrayPresentCount(self):
    self.assertEqual(
        rl_dense_array_qtype.dense_array_boolean([]).present_count, 0
    )
    self.assertEqual(
        rl_dense_array_qtype.dense_array_bytes(
            [b'foo', b'bar', None]
        ).present_count,
        2,
    )

  @parameterized.parameters(
      (rl_dense_array_qvalue.DenseArrayEdge.from_sizes, [1, 1, 0, 2, 0]),
      (
          rl_dense_array_qvalue.DenseArrayEdge.from_split_points,
          [0, 1, 2, 2, 4, 4],
      ),
      (rl_dense_array_qvalue.DenseArrayEdge.from_mapping, [0, 1, 3, 3], 5),
      (
          rl_dense_array_qvalue.DenseArrayEdge.from_keys,
          [0, 1, 2, 2],
          [0, 1, 3, 2, 4],  # Not required to be sorted.
      ),
      (
          rl_dense_array_qvalue.DenseArrayEdge.from_keys,
          [b'a', b'b', b'c', b'c'],
          [b'a', b'b', b'd', b'c', b'e'],
      ),
  )
  def testDenseArrayEdge(self, constructor, *args):
    e = constructor(*args)
    self.assertIsInstance(e, rl_dense_array_qvalue.DenseArrayEdge)
    self.assertEqual(e.qtype, rl_dense_array_qtype.DENSE_ARRAY_EDGE)
    self.assertEqual(e.parent_size, 5)
    self.assertEqual(e.child_size, 4)
    self.assertListEqual(e.mapping().py_value(), [0, 1, 3, 3])

  def testDenseArrayEdgeNewRaises(self):
    with self.assertRaisesRegex(
        NotImplementedError, r'please use DenseArrayEdge\.from'
    ):
      rl_dense_array_qvalue.DenseArrayEdge()

  @parameterized.parameters(0, 1, 5)
  def testDenseArrayShape(self, n):
    shape = rl_dense_array_qvalue.DenseArrayShape(n)
    self.assertIsInstance(shape, rl_dense_array_qvalue.DenseArrayShape)
    self.assertEqual(shape.qtype, rl_dense_array_qtype.DENSE_ARRAY_SHAPE)
    self.assertEqual(shape.size, n)

  @parameterized.parameters(0, 1, 5)
  def testDenseArrayToScalarEdge(self, n):
    edge = rl_dense_array_qvalue.DenseArrayToScalarEdge(n)
    self.assertIsInstance(edge, rl_dense_array_qvalue.DenseArrayToScalarEdge)
    self.assertEqual(
        edge.qtype, rl_dense_array_qtype.DENSE_ARRAY_TO_SCALAR_EDGE
    )
    self.assertEqual(edge.child_size, n)


if __name__ == '__main__':
  absltest.main()
