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
from arolla import arolla
from arolla.jagged_shape import jagged_shape


M = arolla.OperatorsContainer(jagged_shape)


def jagged_array_shape_from_sizes(*sizes_seq):
  return jagged_shape.JaggedArrayShape.from_edges(
      *(arolla.types.ArrayEdge.from_sizes(sizes) for sizes in sizes_seq)
  )


def jagged_dense_array_shape_from_sizes(*sizes_seq):
  return jagged_shape.JaggedDenseArrayShape.from_edges(
      *(arolla.types.DenseArrayEdge.from_sizes(sizes) for sizes in sizes_seq)
  )


class JaggedShapeQTypeTest(parameterized.TestCase):

  def test_jagged_array_shape_qtype(self):
    self.assertEqual(
        jagged_shape.JAGGED_ARRAY_SHAPE,
        arolla.eval(M.jagged.make_jagged_shape_qtype(arolla.ARRAY_EDGE)),
    )

  def test_jagged_dense_array_shape_qtype(self):
    self.assertEqual(
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
        arolla.eval(M.jagged.make_jagged_shape_qtype(arolla.DENSE_ARRAY_EDGE)),
    )

  @parameterized.parameters(
      (jagged_shape.JAGGED_ARRAY_SHAPE, True),
      (jagged_shape.JAGGED_DENSE_ARRAY_SHAPE, True),
      (arolla.make_tuple_qtype(arolla.INT32), False),
  )
  def test_is_jagged_shape_qtype(self, qtype, is_jagged_shape):
    self.assertEqual(jagged_shape.is_jagged_shape_qtype(qtype), is_jagged_shape)


class JaggedArrayShapeTest(parameterized.TestCase):

  def test_empty(self):
    qvalue = jagged_shape.JaggedArrayShape.from_edges()
    self.assertEqual(qvalue.qtype, jagged_shape.JAGGED_ARRAY_SHAPE)
    self.assertEqual(qvalue.rank(), 0)
    self.assertIsInstance(qvalue.rank(), int)
    self.assertIsInstance(qvalue, arolla.QValue)
    self.assertEqual(repr(qvalue), 'JaggedArrayShape()')

  def test_non_empty(self):
    edge1 = arolla.types.ArrayEdge.from_sizes([2])
    edge2 = arolla.types.ArrayEdge.from_sizes([2, 1])
    qvalue = jagged_shape.JaggedArrayShape.from_edges(edge1, edge2)
    self.assertEqual(qvalue.qtype, jagged_shape.JAGGED_ARRAY_SHAPE)
    self.assertEqual(qvalue.rank(), 2)
    self.assertIsInstance(qvalue.rank(), int)
    self.assertIsInstance(qvalue, arolla.QValue)
    self.assertEqual(repr(qvalue), 'JaggedArrayShape(2, [2, 1])')

  def test_non_array_edge(self):
    with self.assertRaisesRegex(ValueError, 'expected_type: ARRAY_EDGE'):
      jagged_shape.JaggedArrayShape.from_edges(  # pytype: disable=wrong-arg-types
          arolla.types.DenseArrayEdge.from_sizes([2])
      )

  def test_invalid_edge(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('incompatible dimensions - edges[0].parent_size != 1'),
    ):
      jagged_shape.JaggedArrayShape.from_edges(
          arolla.types.ArrayEdge.from_sizes([1, 2])
      )

  def test_init_exception(self):
    with self.assertRaisesWithLiteralMatch(
        NotImplementedError, 'please use the from_* classmethods instead'
    ):
      jagged_shape.JaggedArrayShape()

  def test_getitem_index(self):
    edge1 = arolla.types.ArrayEdge.from_sizes([2])
    edge2 = arolla.types.ArrayEdge.from_sizes([2, 1])
    qvalue = jagged_shape.JaggedArrayShape.from_edges(edge1, edge2)
    # Positive indexing.
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[0], edge1)
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[1], edge2)
    # Negative indexing.
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[-2], edge1)
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[-1], edge2)
    # OOB indexing.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected rank > 3, but got rank = 2, when trying to get the edge'
            ' at dim = -4'
        ),
    ):
      _ = qvalue[-4]

  @parameterized.parameters(
      (slice(3), jagged_array_shape_from_sizes([2], [2, 1])),
      (slice(2), jagged_array_shape_from_sizes([2], [2, 1])),
      (slice(1), jagged_array_shape_from_sizes([2])),
      (slice(0), jagged_array_shape_from_sizes()),
      (slice(None), jagged_array_shape_from_sizes([2], [2, 1])),
      (slice(-1), jagged_array_shape_from_sizes([2])),
      (slice(-2), jagged_array_shape_from_sizes()),
      (slice(-3), jagged_array_shape_from_sizes()),
      (slice(0, 1), jagged_array_shape_from_sizes([2])),
      (slice(0, 1, 1), jagged_array_shape_from_sizes([2])),
  )
  def test_getitem_slice(self, slice_, expected_qvalue):
    qvalue = jagged_array_shape_from_sizes([2], [2, 1])
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        qvalue[slice_], expected_qvalue
    )

  @parameterized.parameters(slice(1, 2), slice(0, 1, 2))
  def test_getitem_slice_unsupported_values(self, slice_):
    qvalue = jagged_array_shape_from_sizes([2], [2, 1])
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'jagged shape slicing must start at 0 (or None) and have a step'
        f' size of 1 (or None), got: (start, stop, step)={slice_}',
    ):
      _ = qvalue[slice_]

  def test_getitem_unsupported_type(self):
    qvalue = jagged_array_shape_from_sizes([2], [2, 1])
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'unsupported type: tuple'
    ):
      _ = qvalue[1, 2, 3]

  def test_edges_empty(self):
    qvalue = jagged_shape.JaggedArrayShape.from_edges()
    self.assertEmpty(qvalue.edges())

  def test_edges_non_empty(self):
    edge0 = arolla.types.ArrayEdge.from_sizes([2])
    edge1 = arolla.types.ArrayEdge.from_sizes([2, 1])
    qvalue = jagged_shape.JaggedArrayShape.from_edges(edge0, edge1)
    edges = qvalue.edges()
    self.assertLen(edges, 2)
    arolla.testing.assert_qvalue_equal_by_fingerprint(edges[0], edge0)
    arolla.testing.assert_qvalue_equal_by_fingerprint(edges[1], edge1)

  def test_equal(self):
    edge1 = arolla.types.ArrayEdge.from_sizes([2])
    edge2 = arolla.types.ArrayEdge.from_sizes([2, 1])
    qvalue1 = jagged_shape.JaggedArrayShape.from_edges(edge1, edge2)
    qvalue2 = jagged_shape.JaggedArrayShape.from_edges(edge1, edge2)
    self.assertEqual(qvalue1, qvalue2)
    self.assertEqual(qvalue2, qvalue1)

  def test_not_equal(self):
    edge1 = arolla.types.ArrayEdge.from_sizes([2])
    edge2 = arolla.types.ArrayEdge.from_sizes([2, 1])
    qvalue1 = jagged_shape.JaggedArrayShape.from_edges(edge1)
    qvalue2 = jagged_shape.JaggedArrayShape.from_edges(edge1, edge2)
    self.assertNotEqual(qvalue1, qvalue2)
    self.assertNotEqual(qvalue1, 1)
    self.assertNotEqual(1, qvalue1)


class JaggedDenseArrayShapeTest(parameterized.TestCase):

  def test_empty(self):
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges()
    self.assertEqual(qvalue.qtype, jagged_shape.JAGGED_DENSE_ARRAY_SHAPE)
    self.assertEqual(qvalue.rank(), 0)
    self.assertIsInstance(qvalue.rank(), int)
    self.assertIsInstance(qvalue, arolla.QValue)
    self.assertEqual(repr(qvalue), 'JaggedShape()')

  def test_non_empty(self):
    edge1 = arolla.types.DenseArrayEdge.from_sizes([2])
    edge2 = arolla.types.DenseArrayEdge.from_sizes([2, 1])
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges(edge1, edge2)
    self.assertEqual(qvalue.qtype, jagged_shape.JAGGED_DENSE_ARRAY_SHAPE)
    self.assertEqual(qvalue.rank(), 2)
    self.assertIsInstance(qvalue.rank(), int)
    self.assertIsInstance(qvalue, arolla.QValue)
    self.assertEqual(repr(qvalue), 'JaggedShape(2, [2, 1])')

  def test_non_array_edge(self):
    with self.assertRaisesRegex(ValueError, 'expected_type: DENSE_ARRAY_EDGE'):
      jagged_shape.JaggedDenseArrayShape.from_edges(  # pytype: disable=wrong-arg-types
          arolla.types.ArrayEdge.from_sizes([2])
      )

  def test_invalid_edge(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('incompatible dimensions - edges[0].parent_size != 1'),
    ):
      jagged_shape.JaggedDenseArrayShape.from_edges(
          arolla.types.DenseArrayEdge.from_sizes([1, 2])
      )

  def test_init_exception(self):
    with self.assertRaisesWithLiteralMatch(
        NotImplementedError, 'please use the from_* classmethods instead'
    ):
      jagged_shape.JaggedDenseArrayShape()

  def test_getitem_index(self):
    edge1 = arolla.types.DenseArrayEdge.from_sizes([2])
    edge2 = arolla.types.DenseArrayEdge.from_sizes([2, 1])
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges(edge1, edge2)
    # Positive indexing.
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[0], edge1)
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[1], edge2)
    # Negative indexing.
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[-2], edge1)
    arolla.testing.assert_qvalue_equal_by_fingerprint(qvalue[-1], edge2)
    # OOB indexing.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected rank > 3, but got rank = 2, when trying to get the edge'
            ' at dim = -4'
        ),
    ):
      _ = qvalue[-4]

  @parameterized.parameters(
      (slice(3), jagged_dense_array_shape_from_sizes([2], [2, 1])),
      (slice(2), jagged_dense_array_shape_from_sizes([2], [2, 1])),
      (slice(1), jagged_dense_array_shape_from_sizes([2])),
      (slice(0), jagged_dense_array_shape_from_sizes()),
      (slice(None), jagged_dense_array_shape_from_sizes([2], [2, 1])),
      (slice(-1), jagged_dense_array_shape_from_sizes([2])),
      (slice(-2), jagged_dense_array_shape_from_sizes()),
      (slice(-3), jagged_dense_array_shape_from_sizes()),
      (slice(0, 1), jagged_dense_array_shape_from_sizes([2])),
      (slice(0, 1, 1), jagged_dense_array_shape_from_sizes([2])),
  )
  def test_getitem_slice(self, slice_, expected_qvalue):
    qvalue = jagged_dense_array_shape_from_sizes([2], [2, 1])
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        qvalue[slice_], expected_qvalue
    )

  @parameterized.parameters(slice(1, 2), slice(0, 1, 2))
  def test_getitem_slice_unsupported_values(self, slice_):
    qvalue = jagged_dense_array_shape_from_sizes([2], [2, 1])
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'jagged shape slicing must start at 0 (or None) and have a step'
        f' size of 1 (or None), got: (start, stop, step)={slice_}',
    ):
      _ = qvalue[slice_]

  def test_getitem_unsupported_type(self):
    qvalue = jagged_dense_array_shape_from_sizes([2], [2, 1])
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'unsupported type: tuple'
    ):
      _ = qvalue[1, 2, 3]

  def test_edges_empty(self):
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges()
    self.assertEmpty(qvalue.edges())

  def test_edges_non_empty(self):
    edge0 = arolla.types.DenseArrayEdge.from_sizes([2])
    edge1 = arolla.types.DenseArrayEdge.from_sizes([2, 1])
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges(edge0, edge1)
    edges = qvalue.edges()
    self.assertLen(edges, 2)
    arolla.testing.assert_qvalue_equal_by_fingerprint(edges[0], edge0)
    arolla.testing.assert_qvalue_equal_by_fingerprint(edges[1], edge1)

  def test_equal(self):
    edge1 = arolla.types.DenseArrayEdge.from_sizes([2])
    edge2 = arolla.types.DenseArrayEdge.from_sizes([2, 1])
    qvalue1 = jagged_shape.JaggedDenseArrayShape.from_edges(edge1, edge2)
    qvalue2 = jagged_shape.JaggedDenseArrayShape.from_edges(edge1, edge2)
    self.assertEqual(qvalue1, qvalue2)
    self.assertEqual(qvalue2, qvalue1)

  def test_not_equal(self):
    edge1 = arolla.types.DenseArrayEdge.from_sizes([2])
    edge2 = arolla.types.DenseArrayEdge.from_sizes([2, 1])
    qvalue1 = jagged_shape.JaggedDenseArrayShape.from_edges(edge1)
    qvalue2 = jagged_shape.JaggedDenseArrayShape.from_edges(edge1, edge2)
    self.assertNotEqual(qvalue1, qvalue2)
    self.assertNotEqual(qvalue1, 1)
    self.assertNotEqual(1, qvalue1)


if __name__ == '__main__':
  absltest.main()
