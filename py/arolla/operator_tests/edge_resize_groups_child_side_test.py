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

"""Tests for edge.resize_groups_child_side_test."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils

M = arolla.M

ARRAY_INT_TYPES = (arolla.types.ARRAY_INT32, arolla.types.ARRAY_INT64)

DENSE_ARRAY_INT_TYPES = (
    arolla.types.DENSE_ARRAY_INT32,
    arolla.types.DENSE_ARRAY_INT64,
)


def gen_qtype_signatures():
  yield from (
      (arolla.ARRAY_EDGE, int_type, arolla.ARRAY_EDGE)
      for int_type in arolla.types.INTEGRAL_QTYPES + ARRAY_INT_TYPES
  )
  yield from (
      (arolla.ARRAY_TO_SCALAR_EDGE, int_type, arolla.ARRAY_EDGE)
      for int_type in arolla.types.INTEGRAL_QTYPES
  )
  yield from (
      (arolla.types.DENSE_ARRAY_EDGE, int_type, arolla.types.DENSE_ARRAY_EDGE)
      for int_type in arolla.types.INTEGRAL_QTYPES + DENSE_ARRAY_INT_TYPES
  )
  yield from (
      (
          arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
          int_type,
          arolla.types.DENSE_ARRAY_EDGE,
      )
      for int_type in arolla.types.INTEGRAL_QTYPES
  )
  for offset_type in ARRAY_INT_TYPES + (arolla.UNSPECIFIED,):
    yield from (
        (arolla.ARRAY_EDGE, size_type, offset_type, arolla.ARRAY_EDGE)
        for size_type in arolla.types.INTEGRAL_QTYPES + ARRAY_INT_TYPES
    )
    yield from (
        (arolla.ARRAY_TO_SCALAR_EDGE, size_type, offset_type, arolla.ARRAY_EDGE)
        for size_type in arolla.types.INTEGRAL_QTYPES
    )

  for offset_type in DENSE_ARRAY_INT_TYPES + (arolla.UNSPECIFIED,):
    yield from (
        (
            arolla.types.DENSE_ARRAY_EDGE,
            size_type,
            offset_type,
            arolla.types.DENSE_ARRAY_EDGE,
        )
        for size_type in arolla.types.INTEGRAL_QTYPES + DENSE_ARRAY_INT_TYPES
    )
    yield from (
        (
            arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
            size_type,
            offset_type,
            arolla.types.DENSE_ARRAY_EDGE,
        )
        for size_type in arolla.types.INTEGRAL_QTYPES
    )


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures())


class EdgeResizeGroupsChildSideTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.edge.resize_groups_child_side, QTYPE_SIGNATURES
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_scalar_edge(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    edge = M.edge.resize_groups_child_side(edge, 4)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 3)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [0, 1, 2, None])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_scalar_edge_with_offsets(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    new_offsets = array_factory([2, 0, 1])
    edge = M.edge.resize_groups_child_side(edge, 4, new_offsets)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 3)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [1, 2, 0, None])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_scalar_edge_with_missing_offsets(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    new_offsets = array_factory([2, None, 1])
    edge = M.edge.resize_groups_child_side(edge, 4, new_offsets)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 3)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [None, 2, 0, None])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_groups(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 1, 4]))
    edge = M.edge.resize_groups_child_side(edge, 3)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 7)
    self.assertEqual(qvalue.child_size, 9)
    self.assertListEqual(
        qvalue.mapping().py_value(), [0, 1, None, 2, None, None, 3, 4, 5]
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_groups_with_offsets(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 1, 4]))
    new_offsets = array_factory([1, 0, 1, 3, 2, 1, 0])
    edge = M.edge.resize_groups_child_side(edge, 3, new_offsets)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 7)
    self.assertEqual(qvalue.child_size, 9)
    self.assertListEqual(
        qvalue.mapping().py_value(), [1, 0, None, None, 2, None, 6, 5, 4]
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_empty_groups(self, array_factory):
    edge = M.edge.from_sizes(array_factory([0, 0]))
    edge = M.edge.resize_groups_child_side(edge, 2)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 0)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [None, None, None, None])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_empty_edge(self, array_factory):
    edge = M.edge.from_sizes(array_factory([], arolla.OPTIONAL_INT64))
    edge = M.edge.resize_groups_child_side(edge, 2)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 0)
    self.assertEqual(qvalue.child_size, 0)
    self.assertListEqual(qvalue.mapping().py_value(), [])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_groups_to_empty(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    edge = M.edge.resize_groups_child_side(edge, 0)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 4)
    self.assertEqual(qvalue.child_size, 0)
    self.assertListEqual(qvalue.mapping().py_value(), [])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_resize(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    new_sizes = array_factory([1, 3])
    edge = M.edge.resize_groups_child_side(edge, new_sizes)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 4)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [0, 2, 3, None])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_resize_with_offsets(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    new_sizes = array_factory([1, 3])
    new_offsets = array_factory([1, 0, 1, 0])

    edge = M.edge.resize_groups_child_side(edge, new_sizes, new_offsets)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 4)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [1, 3, 2, None])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_negative_resize(self, array_factory):
    edge = M.edge.from_sizes(array_factory([1, 1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'got a negative size value in operator '
            'edge.resize_groups_child_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, -1))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_non_full_array_resize(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    new_sizes = array_factory([None, 3])
    edge = M.edge.resize_groups_child_side(edge, new_sizes)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            '`new_sizes` should be a full array for operator '
            'edge.resize_groups_child_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, new_sizes))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_mismatched_array_resize(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    new_sizes = array_factory([1, 3, 2])
    edge = M.edge.resize_groups_child_side(edge, new_sizes)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'number of new sizes should match number of edge parent-side '
            'groups in operator edge.resize_groups_child_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, new_sizes))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_mapping_edge(self, array_factory):
    edge = M.edge.from_mapping(array_factory([0, 1, 1]), 2)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'operator edge.resize_groups_child_side is only supported for '
            'SPLIT_POINTS edges'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, 3))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_resize_mapping_edge(self, array_factory):
    edge = M.edge.from_mapping(array_factory([0, 1, 1]), 2)
    new_sizes = array_factory([1, 3, 2])
    edge = M.edge.resize_groups_child_side(edge, new_sizes)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'operator edge.resize_groups_child_side is only supported for '
            'SPLIT_POINTS edges'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, new_sizes))

  def test_scalar_to_scalar_edge(self):
    edge = arolla.types.ScalarToScalarEdge()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('edge parameter cannot have scalar as its child space.'),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, 3))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_with_negative_offsets(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    new_offsets = array_factory([-1, 0, 1])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'got a negative offset in operator edge.resize_groups_child_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, 3, new_offsets))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_with_duplicate_offsets(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    new_offsets = array_factory([0, 0, 1])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'duplicate offsets in the same group in operator '
            'edge.resize_groups_child_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, 3, new_offsets))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_with_mismatched_offsets(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    new_offsets = array_factory([0, 1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            '`new_offsets` argument should be the same size as the child side'
            ' of the edge in edge.resize_groups_child_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_child_side(edge, 3, new_offsets))


if __name__ == '__main__':
  absltest.main()
