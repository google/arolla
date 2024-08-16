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
      (arolla.types.ARRAY_EDGE, int_type, arolla.types.ARRAY_EDGE)
      for int_type in arolla.types.INTEGRAL_QTYPES + ARRAY_INT_TYPES
  )
  yield from (
      (
          arolla.types.ARRAY_TO_SCALAR_EDGE,
          int_type,
          arolla.types.ARRAY_TO_SCALAR_EDGE,
      )
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
          arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
      )
      for int_type in arolla.types.INTEGRAL_QTYPES
  )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class EdgeResizeGroupsParentSideTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.edge.resize_groups_parent_side, QTYPE_SIGNATURES
    )

  @parameterized.named_parameters(
      (
          '_DenseArray',
          arolla.dense_array,
          arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
      ),
      ('_Array', arolla.array, arolla.ARRAY_TO_SCALAR_EDGE),
  )
  def test_scalar_edge(self, array_factory, expected_edge_qtype):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    edge = M.edge.resize_groups_parent_side(edge, 4)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.qtype, expected_edge_qtype)
    self.assertEqual(
        qvalue.fingerprint,
        arolla.eval(M.edge.to_scalar(array_factory([0, 1, 2, 3]))).fingerprint,
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_groups(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 1, 4]))
    edge = M.edge.resize_groups_parent_side(edge, 3)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 3)
    self.assertEqual(qvalue.child_size, 9)
    self.assertListEqual(
        qvalue.mapping().py_value(), [0, 0, 0, 1, 1, 1, 2, 2, 2]
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_empty_groups(self, array_factory):
    edge = M.edge.from_sizes(array_factory([0, 0]))
    edge = M.edge.resize_groups_parent_side(edge, 2)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 2)
    self.assertEqual(qvalue.child_size, 4)
    self.assertListEqual(qvalue.mapping().py_value(), [0, 0, 1, 1])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_empty_edge(self, array_factory):
    edge = M.edge.from_sizes(array_factory([], arolla.OPTIONAL_INT64))
    edge = M.edge.resize_groups_parent_side(edge, 2)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 0)
    self.assertEqual(qvalue.child_size, 0)
    self.assertListEqual(qvalue.mapping().py_value(), [])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_resize_groups_to_empty(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    edge = M.edge.resize_groups_parent_side(edge, 0)
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 2)
    self.assertEqual(qvalue.child_size, 0)
    self.assertListEqual(qvalue.mapping().py_value(), [])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_resize_groups(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 1, 4]))
    edge = M.edge.resize_groups_parent_side(edge, array_factory([3, 2, 3]))
    qvalue = arolla.eval(edge)

    self.assertEqual(qvalue.parent_size, 3)
    self.assertEqual(qvalue.child_size, 8)
    self.assertListEqual(qvalue.mapping().py_value(), [0, 0, 0, 1, 1, 2, 2, 2])

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_negative_resize(self, array_factory):
    edge = M.edge.from_sizes(array_factory([1, 1]))
    with self.assertRaisesRegex(
        ValueError,
        (
            '`size` argument should be a non-negative integer for operator '
            'edge.resize_groups_parent_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_parent_side(edge, -1))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_negative_resize_to_scalar_edge(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 1, 2]))
    with self.assertRaisesRegex(
        ValueError,
        (
            '`size` argument should be a non-negative integer for operator '
            'edge.resize_groups_parent_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_parent_side(edge, -1))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_mapping_edge(self, array_factory):
    edge = M.edge.from_mapping(array_factory([0, 1, 1]), 2)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'operator edge.resize_groups_parent_side is only supported for '
            'SPLIT_POINTS edges'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_parent_side(edge, 3))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_resize_mapping_edge(self, array_factory):
    edge = M.edge.from_mapping(array_factory([0, 1, 1]), 2)
    new_sizes = array_factory([1, 3, 2])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'operator edge.resize_groups_parent_side is only supported for '
            'SPLIT_POINTS edges'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_parent_side(edge, new_sizes))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_mismatched_array_resize(self, array_factory):
    edge = M.edge.from_sizes(array_factory([2, 2]))
    new_sizes = array_factory([1, 3, 2])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'number of new sizes should match number of edge parent-side '
            'groups in operator edge.resize_groups_parent_side'
        ),
    ):
      _ = arolla.eval(M.edge.resize_groups_parent_side(edge, new_sizes))

  def test_scalar_to_scalar_edge(self):
    edge = arolla.types.ScalarToScalarEdge()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('edge parameter cannot have scalar as its child space'),
    ):
      _ = arolla.eval(M.edge.resize_groups_parent_side(edge, 3))


if __name__ == '__main__':
  absltest.main()
