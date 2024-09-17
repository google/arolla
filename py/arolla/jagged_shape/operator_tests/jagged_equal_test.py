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

"""Tests for M.jagged.equal operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape
from arolla.operator_tests import pointwise_test_utils

M = arolla.OperatorsContainer(jagged_shape)
L = arolla.L

# Test data: tuple(
#     (shape, other_shape, result),
#     ...
# )
TEST_DATA = (
    (
        jagged_shape.JaggedArrayShape.from_edges(),
        jagged_shape.JaggedArrayShape.from_edges(),
        arolla.present(),
    ),
    (
        jagged_shape.JaggedArrayShape.from_edges(),
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2])
        ),
        arolla.missing(),
    ),
    (
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2])
        ),
        jagged_shape.JaggedArrayShape.from_edges(),
        arolla.missing(),
    ),
    (
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2])
        ),
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2])
        ),
        arolla.present(),
    ),
    (
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([3])
        ),
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2])
        ),
        arolla.missing(),
    ),
    (
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2]),
            arolla.types.ArrayEdge.from_sizes([2, 1]),
        ),
        jagged_shape.JaggedArrayShape.from_edges(
            arolla.types.ArrayEdge.from_sizes([2]),
            arolla.types.ArrayEdge.from_sizes([2, 1]),
        ),
        arolla.present(),
    ),
    (
        jagged_shape.JaggedDenseArrayShape.from_edges(),
        jagged_shape.JaggedDenseArrayShape.from_edges(),
        arolla.present(),
    ),
    (
        jagged_shape.JaggedDenseArrayShape.from_edges(),
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2])
        ),
        arolla.missing(),
    ),
    (
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2])
        ),
        jagged_shape.JaggedDenseArrayShape.from_edges(),
        arolla.missing(),
    ),
    (
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2])
        ),
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2])
        ),
        arolla.present(),
    ),
    (
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([3])
        ),
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2])
        ),
        arolla.missing(),
    ),
    (
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2]),
            arolla.types.DenseArrayEdge.from_sizes([2, 1]),
        ),
        jagged_shape.JaggedDenseArrayShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2]),
            arolla.types.DenseArrayEdge.from_sizes([2, 1]),
        ),
        arolla.present(),
    ),
)

QTYPE_SIGNATURES = frozenset(
    (shape.qtype, other_shape.qtype, res.qtype)
    for shape, other_shape, res in TEST_DATA
)


class JaggedEqualTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    possible_qtypes = pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES + (
        jagged_shape.JAGGED_ARRAY_SHAPE,
        jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
    )
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(
            M.jagged.equal, possible_qtypes=possible_qtypes
        ),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, arg1, arg2, expected_value):
    actual_value = arolla.eval(
        M.jagged.equal(L.arg1, L.arg2), arg1=arg1, arg2=arg2
    )
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_non_shape_error(self):
    with self.assertRaisesRegex(
        ValueError, "expected a jagged shape, got: shape: QTYPE"
    ):
      M.jagged.equal(arolla.INT32, arolla.INT32)


if __name__ == "__main__":
  absltest.main()
