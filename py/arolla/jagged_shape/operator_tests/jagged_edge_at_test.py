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

"""Tests for jagged_edge_at."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape
from arolla.operator_tests import pointwise_test_utils

M = arolla.OperatorsContainer(jagged_shape)
L = arolla.L


# Test data: tuple(
#     (shape, dim, result),
#     ...
# )
def gen_test_data():
  edges = (
      arolla.types.ArrayEdge.from_sizes([2]),
      arolla.types.ArrayEdge.from_sizes([2, 1]),
      arolla.types.ArrayEdge.from_sizes([2, 1, 2]),
  )
  shape = jagged_shape.JaggedArrayShape.from_edges(*edges)
  for dim in range(-len(edges), len(edges)):
    yield shape, arolla.int32(dim), edges[dim]
    yield shape, arolla.int64(dim), edges[dim]

  edges = (
      arolla.types.DenseArrayEdge.from_sizes([2]),
      arolla.types.DenseArrayEdge.from_sizes([2, 1]),
      arolla.types.DenseArrayEdge.from_sizes([2, 1, 2]),
  )
  shape = jagged_shape.JaggedDenseArrayShape.from_edges(*edges)
  for dim in range(-len(edges), len(edges)):
    yield shape, arolla.int32(dim), edges[dim]
    yield shape, arolla.int64(dim), edges[dim]


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    (shape.qtype, dim.qtype, res.qtype) for shape, dim, res in TEST_DATA
)


class JaggedEdgeAtTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    possible_qtypes = pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES + (
        jagged_shape.JAGGED_ARRAY_SHAPE,
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
    )
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(
            M.jagged.edge_at, possible_qtypes=possible_qtypes
        ),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, shape, dim, expected_value):
    actual_value = arolla.eval(
        M.jagged.edge_at(L.shape, L.dim), shape=shape, dim=dim
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_value, expected_value
    )

  def test_non_shape_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected a jagged shape, got: shape: QTYPE'
    ):
      M.jagged.edge_at(arolla.INT32, 1)

  @parameterized.parameters(
      jagged_shape.JaggedArrayShape.from_edges(),
      jagged_shape.JaggedDenseArrayShape.from_edges(),
  )
  def test_invalid_index_error(self, shape):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected rank > 1, but got rank = 0, when trying to get the edge'
            ' at dim = -2'
        ),
    ):
      arolla.eval(M.jagged.edge_at(shape, -2))


if __name__ == '__main__':
  absltest.main()
