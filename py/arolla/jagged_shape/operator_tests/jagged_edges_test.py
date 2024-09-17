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

"""Tests for M.jagged.edges operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape
from arolla.operator_tests import pointwise_test_utils

M = arolla.OperatorsContainer(jagged_shape)
L = arolla.L

ARRAY_EDGE_SEQUENCE = arolla.types.make_sequence_qtype(arolla.ARRAY_EDGE)
DENSE_ARRAY_EDGE_SEQUENCE = arolla.types.make_sequence_qtype(
    arolla.DENSE_ARRAY_EDGE
)


# Test data: tuple(
#     (shape, result),
#     ...
# )
def gen_test_data():
  sizes = ([2], [2, 1], [2, 1, 2])
  array_edges = [arolla.types.ArrayEdge.from_sizes(s) for s in sizes]
  dense_array_edges = [arolla.types.DenseArrayEdge.from_sizes(s) for s in sizes]
  for dim in range(len(sizes) + 1):
    # ArrayEdge.
    shape = jagged_shape.JaggedArrayShape.from_edges(*array_edges[:dim])
    result = arolla.types.Sequence(
        *array_edges[:dim], value_qtype=arolla.ARRAY_EDGE
    )
    yield shape, result
    # DenseArrayEdge.
    shape = jagged_shape.JaggedDenseArrayShape.from_edges(
        *dense_array_edges[:dim]
    )
    result = arolla.types.Sequence(
        *dense_array_edges[:dim], value_qtype=arolla.DENSE_ARRAY_EDGE
    )
    yield shape, result


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    (shape.qtype, res.qtype) for shape, res in TEST_DATA
)


class JaggedEdgesTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    possible_qtypes = pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES + (
        jagged_shape.JAGGED_ARRAY_SHAPE,
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
        ARRAY_EDGE_SEQUENCE,
        DENSE_ARRAY_EDGE_SEQUENCE,
    )
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(
            M.jagged.edges, possible_qtypes=possible_qtypes
        ),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, shape, expected_value):
    actual_value = arolla.eval(M.jagged.edges(L.shape), shape=shape)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_value, expected_value
    )

  def test_non_shape_error(self):
    with self.assertRaisesRegex(ValueError, 'no matching overload.*QTYPE'):
      M.jagged.edges(arolla.INT32)


if __name__ == '__main__':
  absltest.main()
